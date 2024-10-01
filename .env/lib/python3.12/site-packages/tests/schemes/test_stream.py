# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
from tabulator import Stream


# Read

def test_stream_stream():
    source = io.open('data/table.csv', mode='rb')
    with Stream(source, format='csv') as stream:
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]
