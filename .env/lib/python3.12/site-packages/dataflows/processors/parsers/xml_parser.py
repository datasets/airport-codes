from tabulator.parser import Parser
from tabulator.helpers import reset_stream


class XMLParser(Parser):
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
        return self.__chars is None or self.__chars.closed

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        from xml.etree.ElementTree import parse
        from xmljson import parker

        parsed = parker.data(parse(self.__chars).getroot())
        elements = list(parsed.values())
        if len(elements) > 0:
            elements = elements[0]
        else:
            elements = []
        for row_number, row in enumerate(elements, start=1):
            keys, values = zip(*(row.items()))
            yield (row_number, list(keys), list(values))
