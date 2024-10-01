import functools
import collections

from ..helpers.resource_matcher import ResourceMatcher

Aggregator = collections.namedtuple('Aggregator', ['func'])

AGGREGATORS = {
    'sum': Aggregator(lambda values, fstr, row: sum(values)),
    'avg': Aggregator(lambda values, fstr, row: sum(values) / len(values)),
    'max': Aggregator(lambda values, fstr, row: max(values)),
    'min': Aggregator(lambda values, fstr, row: min(values)),
    'multiply': Aggregator(
        lambda values, fstr, row: functools.reduce(lambda x, y: x*y, values)),
    'constant': Aggregator(lambda values, fstr, row: fstr),
    'join': Aggregator(
        lambda values, fstr, row: fstr.join([str(x) for x in values])),
    'format': Aggregator(lambda values, fstr, row: fstr.format(**row)),
}


def get_type(res_fields, operation_fields, operation):
    types = [f.get('type') for f in res_fields if f['name'] in operation_fields]
    if 'any' in types:
        return 'any'
    if operation in ('format', 'join'):
        return 'string'
    if ('number' in types) or (operation == 'avg'):
        return 'number'
    # integers
    if len(types):
        return types[0]
    # constant
    return 'any'


def process_resource(fields, rows):
    for row in rows:
        for field in fields:
            op = field['operation']
            target = field['target']['name']
            if isinstance(op, str):
                values = [
                    row.get(c)
                    for c in field.get('source', [])
                    if row.get(c) is not None
                ]
                with_ = field.get('with', field.get('with_', ''))
                new_col = AGGREGATORS[op].func(values, with_, row)
                row[target] = new_col
            elif callable(op):
                row[target] = op(row)
        yield row


def get_new_fields(resource, fields):
    new_fields = []
    for f in fields:
        target = f['target']
        if isinstance(target, str):
            target = dict(
                name=target,
                type=get_type(resource['schema']['fields'],
                              f.get('source', []),
                              f['operation'])
            )
            new_fields.append(target)
        elif isinstance:
            new_fields.append(target)
    return new_fields


def add_computed_field(*args, resources=None, **kw):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        assert len(args) < 2, 'add_computed_fields expects at most one positional argument'
        if len(args) == 0:
            fields = [kw]
        elif len(args) == 1:
            fields = args[0]

        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                new_fields = get_new_fields(resource, fields)
                resource['schema']['fields'].extend(new_fields)
        yield package.pkg

        for f in fields:
            target = f['target']
            if isinstance(target, str):
                f['target'] = dict(name=target)

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(fields, resource)

    return func
