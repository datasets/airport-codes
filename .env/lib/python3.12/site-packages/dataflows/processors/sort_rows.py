import re
import decimal
from kvfile import KVFile
from bitstring import BitArray
from ..helpers.resource_matcher import ResourceMatcher


FIELDS_RE = re.compile(r'(\{[^\}]+\})')
KEY_RE = re.compile(r'[^!:\}]+')


class KeyCalc(object):
    def __init__(self, key_spec):
        self.calculator = self.__calculator(key_spec)

    def __calculator(self, key_spec):
        if callable(key_spec):
            return key_spec
        formatters = None
        if isinstance(key_spec, str):
            formatters = FIELDS_RE.findall(key_spec)
            key_spec = [KEY_RE.findall(fmt[1:])[0] for fmt in formatters]
        if isinstance(key_spec, (list, tuple)):
            def func(row):
                ret = ''
                for i, key in enumerate(key_spec):
                    value = row[key]
                    # numbers
                    # https://www.h-schmidt.net/FloatConverter/IEEE754.html
                    raw = not formatters or formatters[i] == '{' + key + '}'
                    if raw and isinstance(value, (int, float, decimal.Decimal)):
                        bits = BitArray(float=value, length=64)
                        # invert the sign bit
                        bits.invert(0)
                        # invert negative numbers
                        if value < 0:
                            bits.invert(range(1, 64))
                        value = bits.hex
                    if formatters:
                        ret += formatters[i].format(**{key: value})
                    else:
                        ret += str(value)
                return ret
            return func
        assert False, 'key should be either a format string or a row->string callable'

    def __call__(self, row):
        return self.calculator(row)


def _sorter(rows, key_calc, reverse, batch_size):
    db = KVFile()

    def process(rows):
        for row_num, row in enumerate(rows):
            key = key_calc(row) + '{:08x}'.format(row_num)
            yield (key, row)

    db.insert(process(rows), batch_size=batch_size)
    for _, value in db.items(reverse=reverse):
        yield value
    db.close()


def sort_rows(key, resources=None, reverse=False, batch_size=1000):
    key_calc = KeyCalc(key)

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield _sorter(rows, key_calc, reverse, batch_size)
            else:
                yield rows

    return func
