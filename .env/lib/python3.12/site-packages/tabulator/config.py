# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import os


# General

VERSION = io.open(os.path.join(os.path.dirname(__file__), 'VERSION')).read().strip()
DEFAULT_SCHEME = 'file'
DEFAULT_ENCODING = 'utf-8'
DEFAULT_SAMPLE_SIZE = 100
DEFAULT_BYTES_SAMPLE_SIZE = 10000
SUPPORTED_COMPRESSION = ['zip', 'gz']
SUPPORTED_HASHING_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512']
ENCODING_CONFIDENCE = 0.5
HTTP_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) ' +
                'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                'Chrome/54.0.2840.87 Safari/537.36'
}
CSV_SAMPLE_LINES = 100
# http://docs.sqlalchemy.org/en/latest/dialects/index.html
SQL_SCHEMES = ['firebird', 'mssql', 'mysql', 'oracle', 'postgresql', 'sqlite', 'sybase']
S3_DEFAULT_ENDPOINT_URL = 'https://s3.amazonaws.com'

# Loaders

LOADERS = {
    's3': 'tabulator.loaders.aws.AWSLoader',
    'file': 'tabulator.loaders.local.LocalLoader',
    'http': 'tabulator.loaders.remote.RemoteLoader',
    'https': 'tabulator.loaders.remote.RemoteLoader',
    'ftp': 'tabulator.loaders.remote.RemoteLoader',
    'ftps': 'tabulator.loaders.remote.RemoteLoader',
    'stream': 'tabulator.loaders.stream.StreamLoader',
    'text': 'tabulator.loaders.text.TextLoader',
}

# Parsers

PARSERS = {
    'csv': 'tabulator.parsers.csv.CSVParser',
    'datapackage': 'tabulator.parsers.datapackage.DataPackageParser',
    'gsheet': 'tabulator.parsers.gsheet.GsheetParser',
    'html': 'tabulator.parsers.html.HTMLTableParser',
    'inline': 'tabulator.parsers.inline.InlineParser',
    'json': 'tabulator.parsers.json.JSONParser',
    'jsonl': 'tabulator.parsers.ndjson.NDJSONParser',
    'ndjson': 'tabulator.parsers.ndjson.NDJSONParser',
    'ods': 'tabulator.parsers.ods.ODSParser',
    'sql': 'tabulator.parsers.sql.SQLParser',
    'tsv': 'tabulator.parsers.tsv.TSVParser',
    'xls': 'tabulator.parsers.xls.XLSParser',
    'xlsx': 'tabulator.parsers.xlsx.XLSXParser',
}

# Writers

WRITERS = {
    'csv': 'tabulator.writers.csv.CSVWriter',
    'json': 'tabulator.writers.json.JSONWriter',
    'xlsx': 'tabulator.writers.xlsx.XLSXWriter',
    'sql': 'tabulator.writers.sql.SQLWriter',
}
