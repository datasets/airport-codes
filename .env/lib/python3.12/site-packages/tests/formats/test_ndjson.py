# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import pytest
from mock import Mock
from six import StringIO
from tabulator import exceptions, Stream
from tabulator.parsers.ndjson import NDJSONParser


# Read

def test_stream_ndjson():
    with Stream('data/table.ndjson', headers=1) as stream:
        assert stream.headers == ['id', 'name']
        assert stream.read(keyed=True) == [
            {'id': 1, 'name': 'english'},
            {'id': 2, 'name': '中国人'}]


# Internal

def test_parser_ndjson():

    source = 'data/table.ndjson'
    encoding = None
    loader = Mock()
    loader.load = Mock(return_value=io.open(source, encoding='utf-8'))
    parser = NDJSONParser(loader)

    assert parser.closed is True
    parser.open(source, encoding=encoding)
    assert parser.closed is False

    assert list(parser.extended_rows) == [
        (1, ['id', 'name'], [1, 'english']),
        (2, ['id', 'name'], [2, '中国人']),
    ]

    assert len(list(parser.extended_rows)) == 0
    parser.reset()
    assert len(list(parser.extended_rows)) == 2

    parser.close()
    assert parser.closed


def test_parser_ndjson_list():
    stream = StringIO(
        '[1, 2, 3]\n'
        '[4, 5, 6]\n'
    )

    loader = Mock(load=Mock(return_value=stream))
    parser = NDJSONParser(loader)
    parser.open(None)

    assert list(parser.extended_rows) == [
        (1, None, [1, 2, 3]),
        (2, None, [4, 5, 6]),
    ]


def test_parser_ndjson_scalar():
    stream = StringIO(
        '1\n'
        '2\n'
    )

    loader = Mock(load=Mock(return_value=stream))
    parser = NDJSONParser(loader)
    parser.open(None)

    with pytest.raises(exceptions.SourceError):
        list(parser.extended_rows)
