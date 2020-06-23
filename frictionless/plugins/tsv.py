import tsv
from ..parser import Parser
from ..plugin import Plugin
from .. import helpers


# Plugin


class TsvPlugin(Plugin):
    def create_parser(self, source, *, dialect=None):
        pass


# Parsers


class TsvParser(Parser):
    options = []  # type: ignore

    def __init__(self, loader):
        self.__loader = loader
        self.__extended_rows = None
        self.__encoding = None
        self.__chars = None

    @property
    def closed(self):
        return self.__chars is None or self.__chars.closed

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
        helpers.reset_stream(self.__chars)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        items = tsv.un(self.__chars)
        for row_number, item in enumerate(items, start=1):
            yield (row_number, None, list(item))
