# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import json
import pytest
from mock import Mock
from tabulator import Stream, exceptions
from tabulator.parsers.json import JSONParser
BASE_URL = 'https://raw.githubusercontent.com/okfn/tabulator-py/master/%s'


# Read

def test_stream_local_json_dicts():
    with Stream('data/table-dicts.json') as stream:
        assert stream.headers is None
        assert stream.read() == [[1, 'english'], [2, '中国人']]


def test_stream_local_json_lists():
    with Stream('data/table-lists.json') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_stream_text_json_dicts():
    source = '[{"id": 1, "name": "english" }, {"id": 2, "name": "中国人" }]'
    with Stream(source, scheme='text', format='json') as stream:
        assert stream.headers is None
        assert stream.read() == [[1, 'english'], [2, '中国人']]


def test_stream_text_json_dicts_with_headers_argument():
    source = '[{"id": 1, "name": "english" }, {"id": 2, "name": "中国人" }]'
    with Stream(source, scheme='text', format='json', headers=['name', 'id']) as stream:
        assert stream.headers == ['name', 'id']
        assert stream.read() == [['english', 1], ['中国人', 2]]


def test_stream_text_json_lists():
    source = '[["id", "name"], [1, "english"], [2, "中国人"]]'
    with Stream(source, scheme='text', format='json') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


@pytest.mark.remote
def test_stream_remote_json_dicts():
    with Stream(BASE_URL % 'data/table-dicts.json') as stream:
        assert stream.headers is None
        assert stream.read() == [[1, 'english'], [2, '中国人']]


@pytest.mark.remote
def test_stream_remote_json_lists():
    with Stream(BASE_URL % 'data/table-lists.json') as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


# Write

def test_stream_save_json(tmpdir):
    source = 'data/table.csv'
    target = str(tmpdir.join('table.json'))
    with Stream(source, headers=1) as stream:
        assert stream.save(target) == 2
    with open(target) as file:
        assert json.load(file) == [
            ['id', 'name'],
            ['1', 'english'],
            ['2', '中国人'],
        ]


def test_stream_save_json_keyed(tmpdir):
    source = 'data/table.csv'
    target = str(tmpdir.join('table.json'))
    with Stream(source, headers=1) as stream:
        assert stream.save(target, keyed=True) == 2
    with open(target) as file:
        assert json.load(file) == [
            {'id': '1', 'name': 'english'},
            {'id': '2', 'name': '中国人'},
        ]


# Internal

def test_parser_json():

    source = 'data/table-dicts.json'
    encoding = None
    loader = Mock()
    loader.load = Mock(return_value=io.open(source, 'rb'))
    parser = JSONParser(loader)

    assert parser.closed
    parser.open(source, encoding=encoding)
    assert not parser.closed

    assert list(parser.extended_rows) == [
        (1, ['id', 'name'], [1, 'english']),
        (2, ['id', 'name'], [2, '中国人'])]

    assert len(list(parser.extended_rows)) == 0
    parser.reset()
    assert len(list(parser.extended_rows)) == 2

    parser.close()
    assert parser.closed
