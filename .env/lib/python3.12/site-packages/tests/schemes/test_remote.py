# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tabulator import Stream
from tabulator.loaders.remote import RemoteLoader
from tabulator.exceptions import HTTPError
from time import time

BASE_URL = 'https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/%s'


# Read

@pytest.mark.remote
def test_stream_https():
    with Stream(BASE_URL % 'data/table.csv') as stream:
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.remote
def test_stream_https_latin1():
    # Github returns wrong encoding `utf-8`
    with Stream(BASE_URL % 'data/special/latin1.csv') as stream:
        assert stream.read()


# Internal

@pytest.mark.remote
def test_loader_remote_t():
    loader = RemoteLoader()
    chars = loader.load(BASE_URL % 'data/table.csv', encoding='utf-8')
    assert chars.read() == 'id,name\n1,english\n2,中国人\n'


@pytest.mark.remote
def test_loader_remote_b():
    spec = '中国人'.encode('utf-8')
    loader = RemoteLoader()
    chars = loader.load(BASE_URL % 'data/table.csv', mode='b', encoding='utf-8')
    assert chars.read() == b'id,name\n1,english\n2,' + spec + b'\n'


@pytest.mark.skip
@pytest.mark.remote
def test_loader_no_timeout():
    loader = RemoteLoader()
    t = time()
    chars = loader.load('https://httpstat.us/200?sleep=5000', mode='b', encoding='utf-8')
    assert time() - t > 5
    assert chars.read() == b'200 OK'
    t = time()


@pytest.mark.remote
def test_loader_has_timeout():
    loader = RemoteLoader(http_timeout=1)
    t = time()
    with pytest.raises(HTTPError):
        chars = loader.load('https://httpstat.us/200?sleep=5000', mode='b', encoding='utf-8')
    assert time() - t < 5
    assert time() - t > 1
