from dataflows import PackageWrapper, ResourceWrapper

from ..helpers.resource_matcher import ResourceMatcher


def deduper(rows: ResourceWrapper):
    pk = rows.res.descriptor['schema'].get('primaryKey', [])
    if len(pk) == 0:
        yield from rows
    else:
        keys = set()
        for row in rows:
            key = tuple(row[k] for k in pk)
            if key in keys:
                continue
            keys.add(key)
            yield row


def deduplicate(resources=None):

    def func(package: PackageWrapper):
        resource_matcher = ResourceMatcher(resources, package)
        yield package.pkg
        resource: ResourceWrapper
        for resource in package:
            if resource_matcher.match(resource.res.name):
                yield deduper(resource)
            else:
                yield resource
    return func
