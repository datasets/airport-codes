# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pybloom_live
from collections import namedtuple
WrittenRow = namedtuple('WrittenRow', ['row', 'updated', 'updated_id'])


# Module API

class Writer(object):

    # Public

    def __init__(self, engine, table, schema, update_keys,
                 autoincrement, convert_row, buffer_size,
                 use_bloom_filter):
        """Writer to insert/update rows into table
        """
        self.__engine = engine
        self.__table = table
        self.__schema = schema
        self.__update_keys = update_keys
        self.__autoincrement = autoincrement
        self.__convert_row = convert_row
        self.__buffer = []
        self.__buffer_size = buffer_size
        self.__use_bloom_filter = use_bloom_filter
        if update_keys is not None and use_bloom_filter:
            with self.__engine.connect() as connection:
                self.__prepare_bloom(connection)

    def write(self, rows, keyed=False):
        """Write rows/keyed_rows to table
        """
        with self.__engine.connect() as connection:
            with connection.begin():
                for row in rows:
                    keyed_row = row
                    if not keyed:
                        keyed_row = dict(zip(self.__schema.field_names, row))
                    keyed_row = self.__convert_row(keyed_row)
                    if self.__check_existing(keyed_row):
                        for wr in self.__insert(connection):
                            yield wr
                        ret = self.__update(connection, keyed_row)
                        if ret is not None:
                            yield WrittenRow(keyed_row, True, ret if self.__autoincrement else None)
                            continue
                    self.__buffer.append(keyed_row)
                    if len(self.__buffer) > self.__buffer_size:
                        for wr in self.__insert(connection):
                            yield wr
                for wr in self.__insert(connection):
                    yield wr

    # Private

    def __prepare_bloom(self, connection):
        """Prepare bloom for existing checks
        """
        self.__bloom = pybloom_live.ScalableBloomFilter()
        columns = [getattr(self.__table.c, key) for key in self.__update_keys]
        keys = connection.execute(self.__table.select().with_only_columns(*columns).execution_options(stream_results=True))
        for key in keys:
            self.__bloom.add(tuple(key))

    def __insert(self, connection):
        """Insert rows to table
        """
        if len(self.__buffer) > 0:
            # Insert data
            statement = self.__table.insert()
            if self.__autoincrement:
                statement = statement.returning(
                    getattr(self.__table.c, self.__autoincrement))
                statement = statement.values(self.__buffer)
                res = connection.execute(statement)
                for id, in res:
                    row = self.__buffer.pop(0)
                    yield WrittenRow(row, False, id)
            else:
                connection.execute(statement, self.__buffer)
                for row in self.__buffer:
                    yield WrittenRow(row, False, None)
            # Clean memory
            self.__buffer = []

    def __update(self, connection, row):
        """Update rows in table
        """
        expr = self.__table.update().values(row)
        for key in self.__update_keys:
            expr = expr.where(getattr(self.__table.c, key) == row[key])
        if self.__autoincrement:
            expr = expr.returning(getattr(self.__table.c, self.__autoincrement))
        res = connection.execute(expr)
        if res.rowcount > 0:
            if self.__autoincrement:
                first = next(iter(res))
                last_row_id = first[0]
                return last_row_id
            return 0
        return None

    def __check_existing(self, row):
        """Check if row exists in table
        """
        if self.__update_keys is not None:
            if self.__use_bloom_filter:
                key = tuple(row[key] for key in self.__update_keys)
                if key in self.__bloom:
                    return True
                self.__bloom.add(key)
                return False
            else:
                return True
        return False
