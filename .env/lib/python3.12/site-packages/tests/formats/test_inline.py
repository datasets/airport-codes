# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from collections import OrderedDict
from tabulator import Stream, exceptions


# Read

def test_stream_inline():
    source = [['id', 'name'], ['1', 'english'], ['2', '中国人']]
    with Stream(source) as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_inline_iterator():
    source = iter([['id', 'name'], ['1', 'english'], ['2', '中国人']])
    with Stream(source) as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_inline_iterator():
    def generator():
        yield ['id', 'name']
        yield ['1', 'english']
        yield ['2', '中国人']
    with pytest.raises(exceptions.SourceError) as excinfo:
        iterator = generator()
        Stream(iterator).open()
    assert 'callable' in str(excinfo.value)


def test_stream_inline_generator():
    def generator():
        yield ['id', 'name']
        yield ['1', 'english']
        yield ['2', '中国人']
    with Stream(generator) as stream:
        assert stream.headers is None
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_stream_inline_keyed():
    source = [{'id': '1', 'name': 'english'}, {'id': '2', 'name': '中国人'}]
    with Stream(source, format='inline') as stream:
        assert stream.headers is None
        assert stream.read() == [['1', 'english'], ['2', '中国人']]


def test_stream_inline_keyed_with_headers_argument():
    source = [{'id': '1', 'name': 'english'}, {'id': '2', 'name': '中国人'}]
    with Stream(source, format='inline', headers=['name', 'id']) as stream:
        assert stream.headers == ['name', 'id']
        assert stream.read() == [['english', '1'], ['中国人', '2']]


def test_stream_inline_ordered_dict():
    source = [
        OrderedDict([('name', 'english'), ('id', '1')]),
        OrderedDict([('name', '中国人'), ('id', '2')]),
    ]
    with Stream(source, headers=1) as stream:
        assert stream.headers == ['name', 'id']
        assert stream.read() == [['english', '1'], ['中国人', '2']]


# Write

def test_stream_save_inline_keyed_with_headers_argument(tmpdir):
    source = [{'key1': 'value1', 'key2': 'value2'}]
    target = str(tmpdir.join('table.csv'))
    with Stream(source, headers=['key2', 'key1']) as stream:
        stream.save(target)
    with Stream(target, headers=1) as stream:
        assert stream.headers == ['key2', 'key1']
        assert stream.read() == [['value2', 'value1']]
