import json
import ijson
import jsonlines
from ..parser import Parser
from .. import exceptions
from .. import dialects
from .. import helpers


class JsonParser(Parser):
    Dialect = dialects.JsonDialect

    # Read

    def read_loader_open(self, loader):
        loader.open(mode='b')

    def read_cell_stream_create(self, dialect=None):
        path = 'item'
        if self.file.dialect.property is not None:
            path = '%s.item' % self.file.dialect.property
        items = ijson.items(self.loader.byte_stream, path)
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
                message = 'JSON item has to be list or dict'
                raise exceptions.SourceError(message)

    # Write

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


class JsonlParser(Parser):
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
