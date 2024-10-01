import itertools

from ..helpers.resource_matcher import ResourceMatcher


def concatenator(resources, all_target_fields, field_mapping):
    for resource_ in resources:
        for row in resource_:
            processed = dict((k, None) for k in all_target_fields)
            values = [(field_mapping[k], v) for (k, v)
                      in row.items()
                      if k in field_mapping and v is not None]
            if len(values) == 0:
                message = 'Got an empty row after concatenation' +\
                    '(resource=%s, source=%r)' % (resource_.res.name, row)
                assert len(values) > 0, message

            processed.update(dict(values))
            yield processed


def concatenate(fields, target={}, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        # Prepare target resource
        if 'name' not in target:
            target['name'] = 'concat'
        if 'path' not in target:
            target['path'] = 'data/' + target['name'] + '.csv'
        target.update(dict(
            mediatype='text/csv',
            schema=dict(fields=[], primaryKey=[]),
            profile='tabular-data-resource'
        ))

        # Create mapping between source field names to target field names
        field_mapping = {}
        for target_field, source_fields in fields.items():
            if source_fields is not None:
                for source_field in source_fields:
                    if source_field in field_mapping:
                        raise RuntimeError('Duplicate appearance of %s (%r)' % (source_field, field_mapping))
                    field_mapping[source_field] = target_field

            if target_field in field_mapping:
                raise RuntimeError('Duplicate appearance of %s' % target_field)

            field_mapping[target_field] = target_field

        # Create the schema for the target resource
        needed_fields = list(fields.keys())
        for resource in package.pkg.descriptor['resources']:
            if not matcher.match(resource['name']):
                continue

            schema = resource.get('schema', {})
            pk = schema.get('primaryKey', [])
            for field in schema.get('fields', []):
                orig_name = field['name']
                if orig_name in field_mapping:
                    name = field_mapping[orig_name]
                    if name not in needed_fields:
                        continue
                    if orig_name in pk:
                        target['schema']['primaryKey'].append(name)
                    target['schema']['fields'].append(field)
                    field['name'] = name
                    needed_fields.remove(name)

        if len(target['schema']['primaryKey']) == 0:
            del target['schema']['primaryKey']

        for name in needed_fields:
            target['schema']['fields'].append(dict(
                name=name, type='string'
            ))

        # Update resources in datapackage (make sure they are consecutive)
        prefix = True
        suffix = False
        num_concatenated = 0
        new_resources = []
        for resource in package.pkg.descriptor['resources']:
            name = resource['name']
            match = matcher.match(name)
            if prefix:
                if match:
                    prefix = False
                    num_concatenated += 1
                else:
                    new_resources.append(resource)
            elif suffix:
                assert not match
                new_resources.append(resource)
            else:
                if not match:
                    suffix = True
                    new_resources.append(target)
                    new_resources.append(resource)
                else:
                    num_concatenated += 1
        if not suffix:
            new_resources.append(target)

        package.pkg.descriptor['resources'] = new_resources
        yield package.pkg

        needed_fields = list(fields.keys())
        it = iter(package)
        for resource in it:
            if matcher.match(resource.res.name):
                resource_chain = \
                    itertools.chain([resource],
                                    itertools.islice(it,
                                                     num_concatenated-1))
                yield concatenator(resource_chain, needed_fields, field_mapping)
            else:
                yield resource

    return func
