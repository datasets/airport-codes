# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tabulator import Stream, exceptions


# Read

def test_stream_format_sql(database_url):
    with Stream(database_url, table='data') as stream:
        assert stream.read() == [[1, 'english'], [2, '中国人']]


def test_stream_format_sql_order_by(database_url):
    with Stream(database_url, table='data', order_by='id') as stream:
        assert stream.read() == [[1, 'english'], [2, '中国人']]


def test_stream_format_sql_order_by_desc(database_url):
    with Stream(database_url, table='data', order_by='id desc') as stream:
        assert stream.read() == [[2, '中国人'], [1, 'english']]


def test_stream_format_sql_table_is_required_error(database_url):
    with pytest.raises(exceptions.TabulatorException) as excinfo:
        Stream(database_url).open()
    assert 'table' in str(excinfo.value)


def test_stream_format_sql_headers(database_url):
    with Stream(database_url, table='data', headers=1) as stream:
        assert stream.headers == ['id', 'name']
        assert stream.read() == [[1, 'english'], [2, '中国人']]


# Write

def test_stream_save_sqlite(database_url):
    source = 'data/table.csv'
    with Stream(source, headers=1) as stream:
        assert stream.save(database_url, table='test_stream_save_sqlite') == 2
    with Stream(database_url, table='test_stream_save_sqlite', order_by='id', headers=1) as stream:
        assert stream.read() == [['1', 'english'], ['2', '中国人']]
        assert stream.headers == ['id', 'name']
