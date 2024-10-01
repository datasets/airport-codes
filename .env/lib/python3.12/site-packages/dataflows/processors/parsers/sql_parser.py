# -*- coding: utf-8 -*-

from sqlalchemy import sql, create_engine
from tabulator import exceptions
from tabulator.parser import Parser


class ExtendedSQLParser(Parser):
    """Parser to get data from SQL database.
    """

    # Public

    options = [
        'table',
        'order_by',
        'query'
    ]

    def __init__(self, loader, force_parse=False, table=None, order_by=None, query=None):
        if query is None and table is None:
            raise exceptions.TabulatorException('Format `sql` requires `table` or `query` options.')

        # Set attributes
        self.__loader = loader
        self.__table = table
        self.__order_by = order_by
        self.__query = query
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
        if self.__query is not None:
            query = sql.text(self.__query)
        else:
            table = sql.table(self.__table)
            order = sql.text(self.__order_by) if self.__order_by else None
            query = sql.select(sql.text('*')).select_from(table).order_by(order)
        with self.__engine.connect() as connection:
            result = connection.execute(query)
            for row_number, row in enumerate(iter(result), start=1):
                row = row._asdict()
                yield (row_number, list(row.keys()), list(row.values()))
