from tabulator.parser import Parser
from tabulator.helpers import reset_stream


class ExcelXMLParser(Parser):
    options = []

    def __init__(self, loader, force_parse, **options):
        self.__loader = loader
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
        self.__chars = None

    def open(self, source, encoding=None):
        self.close()
        self.__chars = self.__loader.load(source, encoding=encoding)
        self.__encoding = getattr(self.__chars, 'encoding', encoding)
        if self.__encoding:
            self.__encoding.lower()
        self.reset()

    def close(self):
        if not self.closed:
            self.__chars.close()

    def reset(self):
        reset_stream(self.__chars)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def closed(self):
        return self.__chars is None

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        from xml.sax import ContentHandler, parse

        class ExcelHandler(ContentHandler):
            def __init__(self):
                self.chars = []
                self.cells = []
                self.rows = []
                self.tables = []

            def characters(self, content):
                self.chars.append(content)

            def startElement(self, name, atts):
                if name == 'Cell':
                    self.chars = []
                elif name == 'Row':
                    self.cells = []
                elif name == 'Table':
                    self.rows = []

            def endElement(self, name):
                if name == 'Cell':
                    self.cells.append(''.join(self.chars))
                elif name == 'Row':
                    self.rows.append(self.cells)
                elif name == 'Table':
                    self.tables.append(self.rows)

        excelHandler = ExcelHandler()
        parse(self.__chars, excelHandler)
        headers = excelHandler.tables[0][0]
        rows = excelHandler.tables[0][1:]
        for row_number, row in enumerate(rows, start=1):
            yield (row_number, headers, row)
