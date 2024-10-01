# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import pytest
from tabulator import Stream
from importlib import import_module
from tabulator.loaders.local import LocalLoader


# Read

def test_stream_file():
    with Stream('data/table.csv') as stream:
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.skipif(sys.version_info < (3, 4), reason='not supported')
def test_stream_file_pathlib_path():
    pathlib = import_module('pathlib')
    with Stream(pathlib.Path('data/table.csv')) as stream:
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


# Internal

def test_loader_local_t():
    loader = LocalLoader()
    chars = loader.load('data/table.csv', encoding='utf-8')
    assert chars.read() == 'id,name\n1,english\n2,中国人\n'


def test_loader_local_b():
    spec = '中国人'.encode('utf-8')
    loader = LocalLoader()
    chars = loader.load('data/table.csv', mode='b', encoding='utf-8')
    assert chars.read() == b'id,name\n1,english\n2,' + spec + b'\n'
