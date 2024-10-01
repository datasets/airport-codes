import re
import copy
import os
import warnings
import collections

from kvfile import KVFile

from dataflows import PackageWrapper


# DB Helper
class KeyCalc(object):

    def __init__(self, key_spec):
        if isinstance(key_spec, list):
            key_list = key_spec
            key_spec = ':'.join('{%s}' % key for key in key_spec)
        else:
            key_list = re.findall(r'\{(.*?)\}', key_spec)
        self.key_spec = key_spec
        self.key_list = key_list

    def __call__(self, row, row_number):
        return self.key_spec.format(**{**row, '#': row_number})


# Aggregator helpers
def identity(x):
    return x


def median(values):
    if values is None:
        return None
    ll = len(values)
    mid = int(ll/2)
    values = sorted(values)
    if ll % 2 == 0:
        return (values[mid - 1] + values[mid])/2
    else:
        return values[mid]


def update_counter(curr, new):
    if new is None:
        return curr
    if curr is None:
        curr = collections.Counter()
    if isinstance(new, str):
        new = [new]
    if not isinstance(curr, collections.Counter):
        curr = collections.Counter(curr)
    curr.update(new)
    return curr


# Aggregators
Aggregator = collections.namedtuple('Aggregator',
                                    ['func', 'finaliser', 'dataType', 'copyProperties'])
AGGREGATORS = {
    'sum': Aggregator(lambda curr, new:
                      new + curr if curr is not None else new,
                      identity,
                      None,
                      False),
    'avg': Aggregator(lambda curr, new:
                      (curr[0] + 1, new + curr[1])
                      if curr is not None
                      else (1, new),
                      lambda value: value[1] / value[0],
                      None,
                      False),
    'median': Aggregator(lambda curr, new:
                         curr + [new] if curr is not None else [new],
                         median,
                         None,
                         True),
    'max': Aggregator(lambda curr, new:
                      max(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'min': Aggregator(lambda curr, new:
                      min(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'first': Aggregator(lambda curr, new:
                        curr if curr is not None else new,
                        identity,
                        None,
                        True),
    'last': Aggregator(lambda curr, new: new,
                       identity,
                       None,
                       True),
    'count': Aggregator(lambda curr, new:
                        curr+1 if curr is not None else 1,
                        identity,
                        'integer',
                        False),
    'any': Aggregator(lambda curr, new: new,
                      identity,
                      None,
                      True),
    'set': Aggregator(lambda curr, new:
                      curr.union({new}) if curr is not None else {new},
                      lambda value: list(value) if value is not None else [],
                      'array',
                      False),
    'array': Aggregator(lambda curr, new:
                        curr + [new] if curr is not None else [new],
                        lambda value: value if value is not None else [],
                        'array',
                        False),
    'counters': Aggregator(lambda curr, new:
                           update_counter(curr, new),
                           lambda value:
                           list(collections.Counter(value).most_common()) if value is not None else [],
                           'array',
                           False),
}


# Input helpers

def fix_fields(fields):
    for field in sorted(fields.keys()):
        spec = fields[field]
        if spec is None:
            fields[field] = spec = {}
        if 'name' not in spec:
            spec['name'] = field
        if 'aggregate' not in spec:
            spec['aggregate'] = 'any'
    return fields


def expand_fields(fields, schema_fields):
    if '*' in fields:
        existing_names = set(f['name'] for f in fields.values())
        spec = fields.pop('*')
        for sf in schema_fields:
            sf_name = sf['name']
            if sf_name not in existing_names:
                fields[sf_name] = copy.deepcopy(spec)
                fields[sf_name]['name'] = sf_name


def order_fields(fields, schema_fields):
    ordered_fields = collections.OrderedDict()
    for descriptor in schema_fields:
        name = descriptor['name']
        if name in fields:
            ordered_fields[name] = fields.pop(name)
    for name in sorted(fields.keys()):
        ordered_fields[name] = fields[name]
    return ordered_fields


def concatenator(resources, all_target_fields, field_mapping):
    for resource_ in resources:
        for row in resource_:
            processed = dict((k, '') for k in all_target_fields)
            values = [(field_mapping[k], v) for (k, v)
                    in row.items()
                    if k in field_mapping]
            assert len(values) > 0
            processed.update(dict(values))
            yield processed


def join_aux(source_name, source_key, source_delete,  # noqa: C901
             target_name, target_key, fields, full, mode):

    deduplication = target_key is None
    fields = fix_fields(fields)
    source_key = KeyCalc(source_key)
    target_key = KeyCalc(target_key) if target_key is not None else target_key
    # We will store db keys as boolean flags:
    # - False -> inserted/not used
    # - True -> inserted/used
    db_keys_usage = KVFile()
    db = KVFile()

    # Mode of join operation
    if full is not None:
        warnings.warn(
            'For the `join` processor the `full=True` flag is deprecated. '
            'Please use the "mode" parameter instead.',
            UserWarning)
        mode = 'half-outer' if full else 'inner'
    assert mode in ['inner', 'half-outer', 'full-outer']

    # Indexes the source data
    def indexer(resource):
        for row_number, row in enumerate(resource, start=1):
            key = source_key(row, row_number)
            try:
                current = db.get(key)
            except KeyError:
                current = {}
            for field, spec in fields.items():
                name = spec['name']
                curr = current.get(field)
                agg = spec['aggregate']
                if agg != 'count':
                    new = row.get(name)
                else:
                    new = ''
                if new is not None:
                    current[field] = AGGREGATORS[agg].func(curr, new)
                elif field not in current:
                    current[field] = None
            if mode == 'full-outer':
                current['__key__'] = [row.get(field) for field in source_key.key_list]
            db.set(key, current)
            db_keys_usage.set(key, False)
            yield row

    # Generates the joined data
    def process_target(resource):
        if deduplication:
            # just empty the iterable
            collections.deque(indexer(resource), maxlen=0)
            for key, value in db.items():
                row = dict(
                    (f, None) for f in fields.keys()
                )
                row.update(dict(
                    (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
                    for k, v in value.items()
                ))
                yield row
        else:
            for row_number, row in enumerate(resource, start=1):
                key = target_key(row, row_number)
                try:
                    extra = create_extra_by_key(key)
                    db_keys_usage.set(key, True)
                except KeyError:
                    if mode == 'inner':
                        continue
                    extra = dict(
                        (k, row.get(k))
                        for k in fields.keys()
                    )
                row.update(extra)
                yield row
            if mode == 'full-outer':
                for key, value in db_keys_usage.items():
                    if value is False:
                        extra = create_extra_by_key(key)
                        yield extra

    # Creates extra by key
    def create_extra_by_key(key):
        extra = db.get(key)
        key = extra.pop('__key__', None)
        extra = dict(
            (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
            for k, v in extra.items()
            if k in fields
        )
        if key:
            for k, v in zip(target_key.key_list, key):
                extra[k] = v
        return extra

    # Yields the new resources
    def new_resource_iterator(resource_iterator):
        has_index = False
        for resource in resource_iterator:
            name = resource.res.name
            if name == source_name:
                has_index = True
                if source_delete:
                    # just empty the iterable
                    collections.deque(indexer(resource), maxlen=0)
                else:
                    yield indexer(resource)
                if deduplication:
                    yield process_target(resource)
            elif name == target_name:
                assert has_index
                yield process_target(resource)
            else:
                yield resource

    # Updates / creates the target resource descriptor
    def process_target_resource(source_spec, resource):
        target_fields = \
            resource.setdefault('schema', {}).setdefault('fields', [])
        for name, spec in fields.items():
            agg = spec['aggregate']
            data_type = AGGREGATORS[agg].dataType
            copy_properties = AGGREGATORS[agg].copyProperties
            to_copy = {}
            if data_type is None:
                try:
                    source_field = \
                        next(filter(lambda f: f['name'] == spec['name'],
                                    source_spec['schema']['fields']))
                except StopIteration:
                    raise KeyError('Failed to find field with name %s in resource %s' %
                                   (spec['name'], source_spec['name']))
                if copy_properties:
                    to_copy = copy.deepcopy(source_field)
                data_type = source_field['type']
            try:
                existing_field = next(iter(filter(
                    lambda f: f['name'] == name,
                    target_fields)))
                assert existing_field['type'] == data_type, \
                    'Reusing %s but with different data types: %s != %s' % (name, existing_field['type'], data_type)
            except StopIteration:
                to_copy.update({
                    'name': name,
                    'type': data_type
                })
                target_fields.append(to_copy)
        return resource

    # Updates the datapackage descriptor based on parameters
    def process_datapackage(datapackage):

        new_resources = []
        source_spec = None

        resource_names = [resource['name'] for resource in datapackage['resources']]
        assert source_name in resource_names, \
            'Source resource ({}) not found package (target={}, found: {})'\
            .format(source_name, target_name, resource_names)
        assert target_name in resource_names, \
            'Target resource ({}) not found package (source={}, found: {})'\
            .format(target_name, source_name, resource_names)

        for resource in datapackage['resources']:

            if resource['name'] == source_name:
                nonlocal fields
                source_spec = resource
                schema_fields = source_spec.get('schema', {}).get('fields', [])
                expand_fields(fields, schema_fields)
                fields = order_fields(fields, schema_fields)
                if not source_delete:
                    new_resources.append(resource)
                if deduplication:
                    resource = process_target_resource(
                        source_spec,
                        {
                            'name': target_name,
                            'path': os.path.join('data', target_name + '.csv')
                        })
                    new_resources.append(resource)

            elif resource['name'] == target_name:
                assert isinstance(source_spec, dict),\
                       'Source resource ({}) must appear before target resource ({}), found: {}'\
                       .format(source_name, target_name, resource_names)
                resource = process_target_resource(source_spec, resource)
                new_resources.append(resource)

            else:
                new_resources.append(resource)

        datapackage['resources'] = new_resources

    def func(package: PackageWrapper):
        process_datapackage(package.pkg.descriptor)
        yield package.pkg
        yield from new_resource_iterator(package)
        db.close()
        db_keys_usage.close()

    return func


def join(source_name, source_key, target_name, target_key, fields={}, full=None, mode='half-outer', source_delete=True):
    return join_aux(source_name, source_key, source_delete, target_name, target_key, fields, full, mode)


def join_with_self(resource_name, join_key, fields):
    return join_aux(resource_name, join_key, True, resource_name, None, fields, True, None)


def join_self(source_name, source_key, target_name, fields):
    import warnings
    warnings.warn('join_self is being deprecated, use join_with_self instead',
                  DeprecationWarning)
    return join_aux(source_name, source_key, True, target_name, None, fields, True, None)
