# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import pytest
from datetime import datetime
from mock import Mock
from tabulator import parsers
from tabulator import Stream, exceptions
from tabulator.parsers.xls import XLSParser
BASE_URL = 'https://raw.githubusercontent.com/okfn/tabulator-py/master/%s'


# Read

def test_stream_local_xls():
    with Stream('data/table.xls') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


@pytest.mark.remote
def test_stream_remote_xls():
    with Stream(BASE_URL % 'data/table.xls') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_xls_sheet_by_index():
    source = 'data/special/sheet2.xls'
    with Stream(source, sheet=2) as stream:
        assert stream.fragment == 'Sheet2'
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_xls_sheet_by_index_not_existent():
    source = 'data/special/sheet2.xls'
    with pytest.raises(exceptions.SourceError) as excinfo:
        Stream(source, sheet=3).open()
    assert 'sheet "3"' in str(excinfo.value)


def test_stream_xls_sheet_by_name():
    source = 'data/special/sheet2.xls'
    with Stream(source, sheet='Sheet2') as stream:
        assert stream.fragment == 'Sheet2'
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_xls_sheet_by_name_not_existent():
    source = 'data/special/sheet2.xls'
    with pytest.raises(exceptions.SourceError) as excinfo:
        Stream(source, sheet='not-existent').open()
    assert 'sheet "not-existent"' in str(excinfo.value)


def test_stream_xlsx_merged_cells():
    source = 'data/special/merged-cells.xls'
    with Stream(source) as stream:
        assert stream.read() == [['data', ''], ['', ''], ['', '']]


def test_stream_xlsx_merged_cells_fill():
    source = 'data/special/merged-cells.xls'
    with Stream(source, fill_merged_cells=True) as stream:
        assert stream.read() == [['data', 'data'], ['data', 'data'], ['data', 'data']]


def test_stream_xls_with_boolean():
    with Stream('data/special/table-with-booleans.xls') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'boolean'], [1, True], [2, False]]


def test_stream_xlsx_merged_cells_boolean():
    source = 'data/special/merged-cells-boolean.xls'
    with Stream(source) as stream:
        assert stream.read() == [[True, ''], ['', ''], ['', '']]


def test_stream_xlsx_merged_cells_fill_boolean():
    source = 'data/special/merged-cells-boolean.xls'
    with Stream(source, fill_merged_cells=True) as stream:
        assert stream.read() == [[True, True], [True, True], [True, True]]


def test_stream_xls_with_ints_floats_dates():
    source = 'data/special/table-with-ints-floats-dates.xls'
    with Stream(source) as stream:
        assert stream.read() == [['Int', 'Float', 'Date'],
                                 [2013, 3.3, datetime(2009, 8, 16)],
                                 [1997, 5.6, datetime(2009, 9, 20)],
                                 [1969, 11.7, datetime(2012, 8, 23)]]

@pytest.mark.skip
@pytest.mark.remote
def test_fix_for_2007_xls():
    source = 'https://ams3.digitaloceanspaces.com/budgetkey-files/spending-reports/2018-3-משרד התרבות והספורט-לשכת הפרסום הממשלתית-2018-10-22-c457.xls'
    with Stream(source) as stream:
        assert len(stream.read()) > 10

# Internal

def test_parser_xls():

    source = 'data/table.xls'
    encoding = None
    loader = Mock()
    loader.load = Mock(return_value=io.open(source, 'rb'))
    parser = XLSParser(loader)

    assert parser.closed
    parser.open(source, encoding=encoding)
    assert not parser.closed

    assert list(parser.extended_rows) == [
        (1, None, ['id', 'name']),
        (2, None, [1, 'english']),
        (3, None, [2, '中国人'])]

    assert len(list(parser.extended_rows)) == 0
    parser.reset()
    assert len(list(parser.extended_rows)) == 3

    parser.close()
    assert parser.closed
