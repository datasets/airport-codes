from ..helpers.resource_matcher import ResourceMatcher


def old_style_conditions(equals, not_equals):
    def func(row):
        return any(
                    (row[k] == v)
                    for o in equals
                    for k, v in o.items()
                ) or any(
                    (row[k] != v)
                    for o in not_equals
                    for k, v in o.items()
                )
    return func


def process_resource(rows, condition):
    for row in rows:
        if condition(row):
            yield row


def filter_rows(condition=None, equals=tuple(), not_equals=tuple(), resources=None):

    if not condition:
        condition = old_style_conditions(equals, not_equals)

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for r in package:
            if matcher.match(r.res.name):
                yield process_resource(r, condition)
            else:
                yield r

    return func
