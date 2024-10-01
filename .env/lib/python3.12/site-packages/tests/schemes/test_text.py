# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from tabulator import Stream
from tabulator.loaders.text import TextLoader


# Read

def test_stream_text():
    source = 'text://value1,value2\nvalue3,value4'
    with Stream(source, format='csv') as stream:
        assert stream.read() == [['value1', 'value2'], ['value3', 'value4']]


# Internal

def test_load_t():
    loader = TextLoader()
    chars = loader.load('id,name\n1,english\n2,中国人\n', encoding='utf-8')
    assert chars.read() == 'id,name\n1,english\n2,中国人\n'

def test_load_b():
    spec = '中国人'.encode('utf-8')
    loader = TextLoader()
    chars = loader.load('id,name\n1,english\n2,中国人\n', mode='b', encoding='utf-8')
    assert chars.read() == b'id,name\n1,english\n2,' + spec + b'\n'
