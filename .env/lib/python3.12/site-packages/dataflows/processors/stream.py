import sys
import os

from ..helpers.extended_json import ejson

ACTIVE_SUFFIX = '.active'


def stream(file=sys.stdout):

    filename = None

    if isinstance(file, str):
        filename = file + ACTIVE_SUFFIX
        basedir = os.path.dirname(filename)
        os.makedirs(basedir, exist_ok=True)
        file = open(filename, 'w')

    def write(obj):
        file.write(ejson.dumps(obj, sort_keys=True, ensure_ascii=True)+'\n')
        file.flush()

    def res_writer(res):
        for r in res:
            write(r)
            yield r

    def func(package):
        write(package.pkg.descriptor)
        yield package.pkg
        for res in package:
            yield res_writer(res)
            file.write('\n')
        file.close()
        if filename:
            os.rename(filename, filename[:-len(ACTIVE_SUFFIX)])

    return func
