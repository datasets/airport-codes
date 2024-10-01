import sys

from datapackage import Package

from ..helpers.extended_json import ejson


def unstream(file=sys.stdin):

    if isinstance(file, str):
        file = open(file)

    def read():
        line = file.readline().strip()
        if len(line) > 0:
            return ejson.loads(line)
        return None

    def res_reader():
        while True:
            r = read()
            if r is not None:
                yield r
            else:
                break

    def func(package):
        descriptor = read()
        yield Package(descriptor)
        for _ in descriptor['resources']:
            yield res_reader()

    return func
