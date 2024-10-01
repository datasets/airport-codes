# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tabulator import Stream, exceptions


# Read

@pytest.mark.remote
def test_stream_gsheet():
    source = 'https://docs.google.com/spreadsheets/d/1mHIWnDvW9cALRMq9OdNfRwjAthCUFUOACPp0Lkyl7b4/edit?usp=sharing'
    with Stream(source) as stream:
        assert stream.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.remote
def test_stream_gsheet_with_gid():
    source = 'https://docs.google.com/spreadsheets/d/1mHIWnDvW9cALRMq9OdNfRwjAthCUFUOACPp0Lkyl7b4/edit#gid=960698813'
    with Stream(source) as stream:
        assert stream.read() == [['id', 'name'], ['2', '中国人'], ['3', 'german']]


@pytest.mark.remote
def test_stream_gsheet_bad_url():
    stream = Stream('https://docs.google.com/spreadsheets/d/bad')
    with pytest.raises(exceptions.HTTPError) as excinfo:
        stream.open()
