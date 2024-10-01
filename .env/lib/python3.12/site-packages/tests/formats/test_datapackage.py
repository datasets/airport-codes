# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import json
import pytest
from mock import Mock
from tabulator import Stream
from tabulator.parsers.datapackage import DataPackageParser


# Read


def test_stream_datapackage():
    with Stream('data/datapackage.json', resource=0, headers=1) as stream:
        assert stream.fragment == 'first-resource'
        assert stream.headers == ['id', 'name']
        assert stream.read(keyed=True) == [
            {'id': 1, 'name': 'english'},
            {'id': 2, 'name': '中国人'}]


def test_second_resource():
    with Stream('data/datapackage.json', resource=1, headers=1) as stream:
        assert stream.fragment == 'number-two'
        assert stream.headers == ['id', 'name']
        assert stream.read(keyed=True) == [
            {'id': 1, 'name': '中国人'},
            {'id': 2, 'name': 'english'}
        ]


def test_named_resource():
    curdir = os.getcwd()
    try:
        os.chdir('data/')
        with Stream('datapackage.json', resource='number-two', headers=1) as stream:
            assert stream.fragment == 'number-two'
            assert stream.headers == ['id', 'name']
            assert stream.read(keyed=True) == [
                {'id': 1, 'name': '中国人'},
                {'id': 2, 'name': 'english'},
            ]
    finally:
        os.chdir(curdir)


# Internal

def test_datapackage_parser():

    source = 'data/datapackage.json'
    parser = DataPackageParser(None)

    assert parser.closed is True
    parser.open(source)
    assert parser.closed is False

    assert list(parser.extended_rows) == [
        (1, ['id', 'name'], [1, 'english']),
        (2, ['id', 'name'], [2, '中国人']),
    ]

    assert len(list(parser.extended_rows)) == 0
    parser.reset()
    assert len(list(parser.extended_rows)) == 2

    parser.close()
    assert parser.closed


def test_datapackage_list():
    curdir= os.getcwd()
    try:
        os.chdir('data/')
        stream = json.load(open('datapackage.json'))

        parser = DataPackageParser(None)
        parser.open(stream)

        assert list(parser.extended_rows) == [
            (1, ['id', 'name'], [1, 'english']),
            (2, ['id', 'name'], [2, '中国人'])
        ]
    finally:
        os.chdir(curdir)
