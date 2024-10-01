import re

from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows, fields):
    for row in rows:
        yield dict(
            (k, v)
            for k, v in row.items()
            if k in fields
        )


def delete_fields(fields, resources=None, regex=True):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        dp_resources = package.pkg.descriptor.get('resources', [])
        field_res = [
            re.compile('^{}$'.format(f if regex else re.escape(f))) for f in fields
        ]
        matched = set()
        new_field_names = {}
        for resource in dp_resources:
            if matcher.match(resource['name']):
                schema_fields = resource['schema'].get('fields', [])
                new_fields = []
                for sf in schema_fields:
                    skip = False
                    for f in field_res:
                        if f.match(sf['name']):
                            skip = True
                            matched.add(f.pattern)
                            break
                    if not skip:
                        new_fields.append(sf)
                not_matched = [f for f in field_res if f.pattern not in matched]
                if len(not_matched) > 0:
                    print('WARNING: Failed to match these fields to delete {!r}'.format(not_matched))
                resource['schema']['fields'] = new_fields
                new_field_names[resource['name']] = [f['name'] for f in new_fields]
        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(resource, new_field_names[resource.res.name])

    return func
