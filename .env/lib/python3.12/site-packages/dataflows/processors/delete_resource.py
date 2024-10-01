import collections
from dataflows import PackageWrapper
from dataflows.helpers.resource_matcher import ResourceMatcher


def delete_resource(resources):

    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        descriptor = package.pkg.descriptor
        descriptor['resources'] = [
            resource for resource in descriptor['resources']
            if not matcher.match(resource['name'])
        ]
        package.pkg.commit()
        yield package.pkg

        for r in package:
            if not matcher.match(r.res.name):
                yield r
            else:
                collections.deque(r, maxlen=0)

    return func
