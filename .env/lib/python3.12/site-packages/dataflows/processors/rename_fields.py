import re

from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows, fields):
    for row in rows:
        yield dict(
            (fields.get(k, k), v)
            for k, v in row.items()
        )


def rename_fields(fields, resources=None, regex=True):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        dp_resources = package.pkg.descriptor.get('resources', [])
        field_res = [
            (re.compile(
                '^{}$'.format(
                    src if regex else re.escape(src)
                )
            ), tgt) for src, tgt in fields.items()
        ]
        matched = set()
        renames = dict()
        renamed_fields = dict()
        for resource in dp_resources:
            res_name = resource['name']
            renamed_fields.setdefault(res_name, set())
            renames.setdefault(res_name, dict())
            if matcher.match(res_name):
                schema_fields = resource['schema'].get('fields', [])
                for sf in schema_fields:
                    sf_name = sf['name']
                    for src, tgt in field_res:
                        if src.match(sf_name):
                            matched.add(src.pattern)
                            target_name = src.sub(tgt, sf_name)
                            assert target_name not in renamed_fields[res_name],\
                                f'Renaming two fields to the same name "{target_name}"'
                            renamed_fields[res_name].add(target_name)
                            renames[res_name][sf_name] = target_name
                            sf['name'] = target_name
                            break
        not_matched = [
            src.pattern for src, _ in field_res
            if src.pattern not in matched
        ]
        if len(not_matched) > 0:
            print('WARNING: Failed to match these fields to rename {!r}'.format(not_matched))
        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(resource, renames[resource.res.name])

    return func
