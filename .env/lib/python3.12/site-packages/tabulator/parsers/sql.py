# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import create_engine, sql
from ..parser import Parser
from .. import exceptions


# Module API

class SQLParser(Parser):
    """Parser to get data from SQL database.
    """

    # Public

    options = [
        'table',
        'order_by',
    ]

    def __init__(self, loader, force_parse=False, table=None, order_by=None):

        # Ensure table
        if table is None:
            raise exceptions.TabulatorException('Format `sql` requires `table` option.')

        # Set attributes
        self.__loader = loader
        self.__table = table
        self.__order_by = order_by
        self.__force_parse = force_parse
        self.__engine = None
        self.__extended_rows = None
        self.__encoding = None

    @property
    def closed(self):
        return self.__engine is None

    def open(self, source, encoding=None):
        self.close()
        self.__engine = create_engine(source)
        self.__engine.update_execution_options(stream_results=True)
        self.__encoding = encoding
        self.reset()

    def close(self):
        if not self.closed:
            self.__engine.dispose()
            self.__engine = None

    def reset(self):
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        table = sql.table(self.__table)
        order = sql.text(self.__order_by) if self.__order_by else None
        query = sql.select(['*']).select_from(table).order_by(order)
        result = self.__engine.execute(query)
        for row_number, row in enumerate(iter(result), start=1):
            yield (row_number, list(row.keys()), list(row))
