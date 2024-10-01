import re

from ..helpers.resource_matcher import ResourceMatcher


def _find_replace(rows, fields):
    for row in rows:
        for field in fields:
            for pattern in field.get('patterns', []):
                row[field['name']] = re.sub(
                    str(pattern['find']),
                    str(pattern['replace']),
                    str(row[field['name']]))
        yield row


def find_replace(fields, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield _find_replace(rows, fields)
            else:
                yield rows

    return func
