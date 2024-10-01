# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import create_engine, MetaData, Table, Column, String
from ..writer import Writer
from .. import exceptions


# Module API

class SQLWriter(Writer):
    """SQL writer.
    """

    # Public

    options = [
        'table',
    ]

    def __init__(self, table=None, **options):

        # Ensure table
        if table is None:
            raise exceptions.TabulatorException('Format `sql` requires `table` option.')

        self.__table = table

    def write(self, source, target, headers, encoding=None):
        engine = create_engine(target)
        count = 0
        buffer = []
        buffer_size = 1000
        with engine.begin() as conn:
            meta = MetaData()
            columns = [Column(header, String()) for header in headers]
            table = Table(self.__table, meta, *columns)
            meta.create_all(conn)
            for row in source:
                count += 1
                buffer.append(row)
                if len(buffer) > buffer_size:
                    conn.execute(table.insert().values(buffer))
                    buffer = []
            if len(buffer):
                conn.execute(table.insert().values(buffer))
        return count
