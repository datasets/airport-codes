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


# Read

@pytest.mark.parametrize('source, selector', [
    ('data/table1.html', 'table'),
    ('data/table2.html', 'table'),
    ('data/table3.html', '.mememe'),
    ('data/table4.html', ''),
])
def test_stream_html(source, selector):
    with Stream(source, selector=selector, headers=1, encoding='utf8') as stream:
        assert stream.headers == ['id', 'name']
        assert stream.read(keyed=True) == [
            {'id': '1', 'name': 'english'},
            {'id': '2', 'name': '中国人'}]

def test_stream_html_raw_html():
    with Stream('data/table3.html', selector='.mememe', headers=1, encoding='utf8', raw_html=True) as stream:
        assert stream.headers == ['id', 'name']
        assert stream.read(keyed=True) == [
            {'id': '1', 'name': '<b>english</b>'},
            {'id': '2', 'name': '中国人'}]


