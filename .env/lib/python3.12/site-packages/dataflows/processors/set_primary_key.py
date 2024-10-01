from dataflows import PackageWrapper
from dataflows.helpers.resource_matcher import ResourceMatcher


def set_primary_key(primary_key, resources=None):

    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                resource.setdefault('schema', {})['primaryKey'] = primary_key
        yield package.pkg

        res_iter = iter(package)
        for r in res_iter:
            if matcher.match(r.res.name):
                yield r.it
            else:
                yield r

    return func
