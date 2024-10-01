# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import pytest
from mock import Mock
from tabulator import Stream
from tabulator.parsers.csv import CSVParser
BASE_URL = 'https://raw.githubusercontent.com/okfn/tabulator-py/master/%s'


# Read

def test_stream_local_csv():
    with Stream('data/table.csv') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_local_csv_with_bom():
    with Stream('data/special/bom.csv') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_local_csv_with_bom_with_encoding():
    with Stream('data/special/bom.csv', encoding='utf-8') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_csv_excel():
    source = 'value1,value2\nvalue3,value4'
    with Stream(source, scheme='text', format='csv') as stream:
        assert stream.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_stream_csv_excel_tab():
    source = 'value1\tvalue2\nvalue3\tvalue4'
    with Stream(source, scheme='text', format='csv', delimiter='\t') as stream:
        assert stream.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_stream_csv_unix():
    source = '"value1","value2"\n"value3","value4"'
    with Stream(source, scheme='text', format='csv') as stream:
        assert stream.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_stream_csv_escaping():
    with Stream('data/special/escaping.csv', escapechar='\\') as stream:
        assert stream.read() == [
            ['ID', 'Test'],
            ['1', 'Test line 1'],
            ['2', 'Test " line 2'],
            ['3', 'Test " line 3'],
        ]


def test_stream_csv_doublequote():
    with Stream('data/special/doublequote.csv') as stream:
        for row in  stream:
            assert len(row) == 17


def test_stream_stream_csv():
    source = io.open('data/table.csv', mode='rb')
    with Stream(source, format='csv') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_text_csv():
    source = 'text://id,name\n1,english\n2,中国人\n'
    with Stream(source, format='csv') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.remote
def test_stream_remote_csv():
    with Stream(BASE_URL % 'data/table.csv') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.remote
def test_stream_remote_csv_non_ascii_url():
    with Stream('http://data.defra.gov.uk/ops/government_procurement_card/over_£500_GPC_apr_2013.csv') as stream:
        assert stream.sample[0] == [
            'Entity',
            'Transaction Posting Date',
            'Merchant Name',
            'Amount',
            'Description']


def test_stream_csv_delimiter():
    source = '"value1";"value2"\n"value3";"value4"'
    with Stream(source, scheme='text', format='csv', delimiter=';') as stream:
        assert stream.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_stream_csv_escapechar():
    source = 'value1%,value2\nvalue3%,value4'
    with Stream(source, scheme='text', format='csv', escapechar='%') as stream:
        assert stream.read() == [['value1,value2'], ['value3,value4']]


def test_stream_csv_quotechar():
    source = '%value1,value2%\n%value3,value4%'
    with Stream(source, scheme='text', format='csv', quotechar='%') as stream:
        assert stream.read() == [['value1,value2'], ['value3,value4']]


def test_stream_csv_skipinitialspace():
    source = 'value1, value2\nvalue3, value4'
    with Stream(source, scheme='text', format='csv', skipinitialspace=True) as stream:
        assert stream.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_stream_csv_detect_delimiter_tab():
    source = 'a1\tb1\tc1A,c1B\na2\tb2\tc2\n'
    with Stream(source, scheme='text', format='csv') as stream:
        assert stream.read() == [['a1', 'b1', 'c1A,c1B'], ['a2', 'b2', 'c2']]


def test_stream_csv_detect_delimiter_semicolon():
    source = 'a1;b1\na2;b2\n'
    with Stream(source, scheme='text', format='csv') as stream:
        assert stream.read() == [['a1', 'b1'], ['a2', 'b2']]


def test_stream_csv_detect_delimiter_pipe():
    source = 'a1|b1\na2|b2\n'
    with Stream(source, scheme='text', format='csv') as stream:
        assert stream.read() == [['a1', 'b1'], ['a2', 'b2']]


def test_stream_csv_dialect_should_not_persist_if_sniffing_fails_issue_goodtables_228():
    source1 = 'a;b;c\n#comment'
    source2 = 'a,b,c\n#comment'
    with Stream(source1, scheme='text', format='csv', headers=1, delimiter=';') as stream:
        assert stream.headers == ['a', 'b', 'c']
    with Stream(source2, scheme='text', format='csv', headers=1) as stream:
        assert stream.headers == ['a', 'b', 'c']


def test_stream_csv_quotechar_is_empty_string():
    import csv
    source = 'value1,value2",value3'
    with Stream(source, scheme='text', format='csv', quoting=csv.QUOTE_NONE) as stream:
        stream.read() == ['value1', 'value2"', 'value3']
# Write

def test_stream_save_csv(tmpdir):
    source = 'data/table.csv'
    target = str(tmpdir.join('table.csv'))
    with Stream(source, headers=1) as stream:
        assert stream.save(target) == 2
    with Stream(target, headers=1) as stream:
        assert stream.headers == ['id', 'name']
        assert stream.read(extended=True) == [
            (2, ['id', 'name'], ['1', 'english']),
            (3, ['id', 'name'], ['2', '中国人']),
        ]


# Internal

def test_parser_csv():

    source = 'data/table.csv'
    encoding = None
    loader = Mock()
    loader.load = Mock(return_value=io.open(source, encoding='utf-8'))
    parser = CSVParser(loader)

    assert parser.closed
    parser.open(source, encoding=encoding)
    assert not parser.closed

    assert list(parser.extended_rows) == [
        (1, None, ['id', 'name']),
        (2, None, ['1', 'english']),
        (3, None, ['2', '中国人'])]

    assert len(list(parser.extended_rows)) == 0
    parser.reset()
    assert len(list(parser.extended_rows)) == 3

    parser.close()
    assert parser.closed
