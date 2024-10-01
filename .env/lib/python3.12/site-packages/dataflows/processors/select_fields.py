import re

from .. import ResourceWrapper
from ..helpers.resource_matcher import ResourceMatcher


def process_resource(rows: ResourceWrapper, configuration):
    fields = configuration[rows.res.descriptor['name']]
    for row in rows:
        row = dict(
            (k, v)
            for k, v in row.items()
            if k in fields
        )
        yield row


def select_fields(fields, resources=None, regex=True):

    def func(package):
        configuration = dict()
        matcher = ResourceMatcher(resources, package.pkg)
        dp_resources = package.pkg.descriptor.get('resources', [])
        for resource in dp_resources:
            if matcher.match(resource['name']):
                configuration.setdefault(resource['name'], set())
                dp_fields = resource['schema'].get('fields', [])
                dp_fields = dict(
                    (f['name'], f)
                    for f in dp_fields
                )
                new_fields = []
                for selected_field in fields:
                    selected_field = re.compile('^{}$'.format(
                        selected_field
                        if regex
                        else re.escape(selected_field)))
                    for name in list(dp_fields.keys()):
                        if selected_field.match(name):
                            new_fields.append(dp_fields.pop(name))
                            configuration[resource['name']].add(name)

                assert len(new_fields) > 0, \
                    "Can't find any fields to select in resource %s" % resource['name']

                resource['schema']['fields'] = new_fields
        yield package.pkg

        for resource in package:
            if not matcher.match(resource.res.name):
                yield resource
            else:
                yield process_resource(resource, configuration)

    return func
