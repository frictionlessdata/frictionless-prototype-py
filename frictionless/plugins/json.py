import json
import ijson
import jsonlines
from ..plugin import Plugin
from ..parser import Parser
from .. import exceptions
from .. import helpers


# Plugin


class JsonPlugin(Plugin):
    def create_parser(self, source, *, dialect=None):
        pass


# Parsers


class JsonParser(Parser):
    options = [
        'property',
    ]

    def __init__(self, loader, property=None):
        self.__loader = loader
        self.__property = property
        self.__extended_rows = None
        self.__encoding = None
        self.__bytes = None

    @property
    def closed(self):
        return self.__bytes is None or self.__bytes.closed

    def open(self, source, encoding=None):
        self.close()
        self.__encoding = encoding
        self.__bytes = self.__loader.load(source, mode='b', encoding=encoding)
        if self.__encoding:
            self.__encoding.lower()
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

    def __iter_extended_rows(self, dialect=None):
        path = 'item'
        if self.__property is not None:
            path = '%s.item' % self.__property
        items = ijson.items(self.__bytes, path)
        for row_number, item in enumerate(items, start=1):
            if isinstance(item, (tuple, list)):
                yield (row_number, None, list(item))
            elif isinstance(item, dict):
                keys = []
                values = []
                for key in sorted(item.keys()):
                    keys.append(key)
                    values.append(item[key])
                yield (row_number, list(keys), list(values))
            else:
                # TODO: remove not dialect
                if not dialect or not dialect.get('forced'):
                    message = 'JSON item has to be list or dict'
                    raise exceptions.SourceError(message)
                yield (row_number, None, [])


class NdjsonParser(Parser):
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

    def __iter_extended_rows(self, dialect=None):
        rows = jsonlines.Reader(self.__chars)
        for row_number, row in enumerate(rows, start=1):
            if isinstance(row, (tuple, list)):
                yield row_number, None, list(row)
            elif isinstance(row, dict):
                keys, values = zip(*sorted(row.items()))
                yield (row_number, list(keys), list(values))
            else:
                # TODO: remove not dialect
                if not dialect and not dialect.get('forced'):
                    raise exceptions.SourceError('JSON item has to be list or dict')
                yield (row_number, None, [])


class JsonWriter:
    options = [
        'keyed',
    ]

    def __init__(self, keyed=False):
        self.__keyed = keyed

    def write(self, source, target, headers, encoding=None):
        helpers.ensure_dir(target)
        data = []
        count = 0
        if not self.__keyed:
            data.append(headers)
        for row in source:
            if self.__keyed:
                row = dict(zip(headers, row))
            data.append(row)
            count += 1
        with open(target, 'w') as file:
            json.dump(data, file, indent=2)
        return count
