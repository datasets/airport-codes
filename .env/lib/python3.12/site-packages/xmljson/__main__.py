import sys
import json
import argparse
from contextlib import closing
import xmljson

try:
    from lxml.etree import parse
except ImportError:
    from xml.etree.cElementTree import parse

dialects = {
    key.lower(): val for key, val in sorted(vars(xmljson).items())
    if isinstance(val, type) and issubclass(val, xmljson.XMLData)
}


def parse_args(args=None, in_file=sys.stdin, out_file=sys.stdout):
    parser = argparse.ArgumentParser(prog='xmljson')
    parser.add_argument('in_file', type=argparse.FileType(), nargs='?', default=in_file,
                        help='defaults to stdin')
    parser.add_argument('-o', '--out_file', type=argparse.FileType('w'), default=out_file,
                        help='defaults to stdout')
    parser.add_argument('-d', '--dialect', choices=list(dialects.keys()), default='parker',
                        type=str.lower, help='defaults to parker')
    args = parser.parse_args() if args is None else parser.parse_args(args)

    if args.dialect not in dialects:
        raise TypeError('Unknown dialect: %s' % args.dialect)
    else:
        dialect = dialects[args.dialect]()

    return args.in_file, args.out_file, dialect


def main(*test_args):
    in_file, out_file, dialect = parse_args() if not test_args else test_args
    with closing(in_file) as in_file, closing(out_file) as out_file:
        json.dump(dialect.data(parse(in_file).getroot()), out_file, indent=2)


if __name__ == '__main__':
    main()
