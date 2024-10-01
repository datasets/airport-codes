# coding: utf-8
from __future__ import unicode_literals

import csv
import warnings

import six


def un(source, wrapper=list, error_bad_lines=True):
    """Parse a text stream to TSV

    If the source is a string, it is converted to a line-iterable stream. If
    it is a file handle or other object, we assume that we can iterate over
    the lines in it.

    The result is a generator, and what it contains depends on whether the
    second argument is set and what it is set to.

    If the second argument is set to list, the default, then each element of
    the result is a list of strings. If it is set to a class generated with
    namedtuple(), then each element is an instance of this class, or None if
    there were too many or too few fields.

    Although newline separated input is preferred, carriage-return-newline is
    accepted on every platform.

    Since there is no definite order to the fields of a dict, there is no
    consistent way to format dicts for output. To avoid the asymmetry of a
    type that can be read but not written, plain dictionary parsing is
    omitted.
    """
    if isinstance(source, six.string_types):
        source = six.StringIO(source)

    # Prepare source lines for reading
    rows = parse_lines(source)

    # Get columns
    if is_namedtuple(wrapper):
        columns = wrapper._fields
        wrapper = wrapper._make
    else:
        columns = next(rows, None)
        if columns is not None:
            i, columns = columns
            yield wrapper(columns)

    # Get values
    for i, values in rows:
        if check_line_consistency(columns, values, i, error_bad_lines):
            yield wrapper(values)


def is_namedtuple(obj):
    return (
        issubclass(obj, tuple) and
        hasattr(obj, '_fields') and
        hasattr(obj, '_make')
    )


def parse_lines(lines):
    for i, line in enumerate(lines, 1):
        if line != '':
            values = parse_line(line)
            if values is not None:
                yield i, values


def parse_line(line):
    line = line.split('\n')[0].split('\r')[0]
    return [parse_field(s) for s in line.split('\t')]


def check_line_consistency(columns, values, line_no, error_bad_lines):
    if columns is None or len(columns) == len(values):
        return True
    else:
        message = (
            "Expected %d fields in line %d, saw %d"
        ) % (len(columns), line_no, len(values))
        if error_bad_lines:
            raise ValueError(message)
        else:
            warnings.warn(message)
        return False


def parse_field(s):
    o = ''
    if s == '\\N':
        return None
    before, sep, after = s.partition('\\')
    while sep != '':
        o += before
        if after == '':
            raise FinalBackslashInFieldIsForbidden
        if after[0] in escapes:
            o += escapes[after[0]]
            before, sep, after = after[1:].partition('\\')
        else:
            before, sep, after = after.partition('\\')
    else:
        o += before
        return o


escapes = {'t': '\t', 'n': '\n', 'r': '\r', '\\': '\\'}


def to(items, output=None):
    """Present a collection of items as TSV

    The items in the collection can themselves be any iterable collection.
    (Single field structures should be represented as one tuples.)

    With no output parameter, a generator of strings is returned. If an output
    parameter is passed, it should be a file-like object. Output is always
    newline separated.
    """
    strings = (format_collection(item) for item in items)
    if output is None:
        return strings
    else:
        for s in strings:
            output.write(s + '\n')


def format_collection(col):
    return format_fields(col)


def format_fields(fields):
    return '\t'.join(format_field(field) for field in fields)


def format_field(value):
    if value is None:
        return '\\N'
    if not isinstance(value, six.string_types):
        value = six.text_type(value)
    return escape_special_chars(value)


def escape_special_chars(s):
    for a, b in [('\\', '\\\\'), ('\t', '\\t'), ('\n', '\\n'), ('\r', '\\r')]:
        s = s.replace(a, b)
    return s


class FinalBackslashInFieldIsForbidden(ValueError):
    pass


class reader(object):
    dialect = 'linear-tsv'

    def __init__(self, tsvfile):
        self.tsvfile = tsvfile
        self.line_num = 0

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.tsvfile)
        self.line_num += 1
        return parse_line(line)


class writer(object):
    dialect = 'linear-tsv'

    def __init__(self, tsvfile):
        self.tsvfile = tsvfile

    def writerow(self, row):
        to([row], self.tsvfile)

    def writerows(self, rows):
        to(rows, self.tsvfile)


class DictReader(csv.DictReader):
    def __init__(self, f, fieldnames=None, restkey=None, restval=None):
        csv.DictReader.__init__(self, f, fieldnames=fieldnames,
                                restkey=restkey, restval=restval)
        self.reader = reader(f)


class DictWriter(csv.DictWriter):
    def __init__(self, f, fieldnames, restval='', extrasaction='raise'):
        csv.DictWriter.__init__(self, f, fieldnames=fieldnames,
                                restval=restval, extrasaction=extrasaction)
        self.writer = writer(f)
