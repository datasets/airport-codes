# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from datetime import datetime

import six
import ezodf
from six import BytesIO
from ..parser import Parser
from .. import exceptions
from .. import helpers


# Module API

class ODSParser(Parser):
    """Parser to parse ODF Spreadsheets.
    """

    # Public

    options = [
        'sheet',
    ]

    def __init__(self, loader, force_parse=False, sheet=1):
        self.__loader = loader
        self.__sheet_pointer = sheet
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
        self.__bytes = None
        self.__book = None
        self.__sheet = None

    @property
    def closed(self):
        return self.__bytes is None or self.__bytes.closed

    def open(self, source, encoding=None):
        self.close()
        self.__encoding = encoding
        self.__bytes = self.__loader.load(source, mode='b', encoding=encoding)

        # Get book
        self.__book = ezodf.opendoc(BytesIO(self.__bytes.read()))

        # Get sheet
        try:
            if isinstance(self.__sheet_pointer, six.string_types):
                self.__sheet = self.__book.sheets[self.__sheet_pointer]
            else:
                self.__sheet = self.__book.sheets[self.__sheet_pointer - 1]
        except (KeyError, IndexError):
            message = 'OpenOffice document "%s" doesn\'t have a sheet "%s"'
            raise exceptions.SourceError(message % (source, self.__sheet_pointer))

        # Rest parser
        self.reset()

    def close(self):
        if not self.closed:
            self.__bytes.close()

    def reset(self):
        helpers.reset_stream(self.__bytes)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):

        def type_value(cell):
            """Detects int value, date and datetime"""

            ctype = cell.value_type
            value = cell.value

            # ods numbers are float only
            # float with no decimals can be cast into int
            if isinstance(value, float) and value == value // 1:
                return int(value)

            # Date or datetime
            if ctype == 'date':
                if len(value) == 10:
                    return datetime.strptime(value, '%Y-%m-%d').date()
                else:
                    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')

            return value

        for row_number, row in enumerate(self.__sheet.rows(), start=1):
            yield row_number, None, [type_value(cell) for cell in row]
