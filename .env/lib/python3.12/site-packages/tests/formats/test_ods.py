# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from datetime import datetime

import io
import pytest
from mock import Mock
from tabulator import Stream, exceptions
from tabulator.parsers.ods import ODSParser
BASE_URL = 'https://raw.githubusercontent.com/okfn/tabulator-py/master/%s'


# Read

def test_stream_ods():
    with Stream('data/table.ods', headers=1) as stream:
        assert stream.headers == ['id', 'name']
        assert stream.read(keyed=True) == [
            {'id': 1, 'name': 'english'},
            {'id': 2, 'name': '中国人'},
        ]


@pytest.mark.remote
def test_stream_ods_remote():
    source = BASE_URL % 'data/table.ods'
    with Stream(source) as stream:
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_ods_sheet_by_index():
    with Stream('data/table.ods', sheet=1) as stream:
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_ods_sheet_by_index_not_existent():
    with pytest.raises(exceptions.SourceError) as excinfo:
        Stream('data/table.ods', sheet=3).open()
    assert 'sheet "3"' in str(excinfo.value)


def test_stream_ods_sheet_by_name():
    with Stream('data/table.ods', sheet='Лист1') as stream:
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_ods_sheet_by_index_not_existent_2():
    with pytest.raises(exceptions.SourceError) as excinfo:
        Stream('data/table.ods', sheet='not-existent').open()
    assert 'sheet "not-existent"' in str(excinfo.value)


def test_stream_ods_with_boolean():
    with Stream('data/special/table-with-booleans.ods') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'boolean'], [1, True], [2, False]]


def test_stream_ods_with_ints_floats_dates():
    source = 'data/special/table-with-ints-floats-dates.ods'
    with Stream(source) as stream:
        assert stream.read() == [['Int', 'Float', 'Date', 'Datetime'],
                                 [2013, 3.3, datetime(2009, 8, 16).date(), datetime(2009, 8, 16, 5, 43, 21)],
                                 [1997, 5.6, datetime(2009, 9, 20).date(), datetime(2009, 9, 20, 15, 30, 0)],
                                 [1969, 11.7, datetime(2012, 8, 23).date(), datetime(2012, 8, 23, 20, 40, 59)]]


# Internal

def test_parser_ods():

    source = 'data/table.ods'
    encoding = None
    loader = Mock()
    loader.load = Mock(return_value=io.open(source, 'rb'))
    parser = ODSParser(loader)

    assert parser.closed
    parser.open(source, encoding=encoding)
    assert not parser.closed

    assert list(parser.extended_rows) == [
        (1, None, ['id', 'name']),
        (2, None, [1.0, 'english']),
        (3, None, [2.0, '中国人']),
    ]

    assert len(list(parser.extended_rows)) == 0
    parser.reset()
    assert len(list(parser.extended_rows)) == 3

    parser.close()
    assert parser.closed
