import copy
import re

from ..helpers.resource_matcher import ResourceMatcher


def match_fields(field_name_re, expected):
    def _filter(field):
        return (field_name_re.fullmatch(field['name']) is not None) is expected
    return _filter


def unpivot_rows(rows, fields_to_unpivot, fields_to_keep, extra_value):
    for row in rows:
        for unpivot_field in fields_to_unpivot:
            new_row = copy.deepcopy(unpivot_field['keys'])
            for field in fields_to_keep:
                new_row[field] = row[field]
            new_row[extra_value['name']] = row.get(unpivot_field['name'])
            yield new_row


def unpivot(unpivot_fields, extra_keys, extra_value, regex=True, resources=None):

    def func(package):

        matcher = ResourceMatcher(resources, package.pkg)
        all_res_config = {}
        for resource in package.pkg.descriptor['resources']:
            config = all_res_config.setdefault(resource['name'], {})
            name = resource['name']
            if not matcher.match(name):
                continue
            schema = resource.get('schema')
            if schema is None:
                continue

            fields = schema.get('fields', [])

            for u_field in unpivot_fields:
                if regex:
                    field_name_re = re.compile(u_field['name'])
                    fields_to_pivot = list(
                        filter(match_fields(field_name_re, True), fields)
                    )
                    fields = list(
                        filter(match_fields(field_name_re, False), fields)
                    )
                else:
                    field_name = u_field['name']
                    fields_to_pivot = list(
                        filter(lambda f: f['name'] == field_name, fields)
                    )
                    fields = list(
                        filter(lambda f: f['name'] != field_name, fields)
                    )

                # handle with regex
                config.setdefault('unpivot_fields_without_regex', [])
                for field_to_pivot in fields_to_pivot:
                    original_key_values = u_field['keys']  # With regex
                    new_key_values = {}
                    for key in original_key_values:
                        new_val = original_key_values[key]
                        if regex and isinstance(new_val, str):
                            new_val = re.sub(
                                u_field['name'], new_val, field_to_pivot['name'])
                        new_key_values[key] = new_val
                    field_to_pivot['keys'] = new_key_values
                    config['unpivot_fields_without_regex'].append(field_to_pivot)

            config['fields_to_keep'] = [f['name'] for f in fields]
            fields.extend(extra_keys)
            fields.append(extra_value)
            schema['fields'] = fields

        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield unpivot_rows(resource,
                                   all_res_config[resource.res.name]['unpivot_fields_without_regex'],
                                   all_res_config[resource.res.name]['fields_to_keep'],
                                   extra_value)

    return func
