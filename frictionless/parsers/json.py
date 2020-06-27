import json
import ijson
import jsonlines
from ..parser import Parser
from .. import exceptions
from .. import dialects
from .. import helpers
from .. import errors


class JsonParser(Parser):
    Dialect = dialects.JsonDialect

    # Read

    def read_data_stream_create(self, dialect=None):
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
                error = errors.SourceError(note='JSON item has to be list or dict')
                raise exceptions.FrictionlessException(error)

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
    Dialect = dialects.JsonDialect

    # Read

    def read_data_stream_create(self, dialect=None):
        rows = jsonlines.Reader(self.loader.text_stream)
        for row_number, row in enumerate(rows, start=1):
            if isinstance(row, (tuple, list)):
                yield row_number, None, list(row)
            elif isinstance(row, dict):
                keys, values = zip(*sorted(row.items()))
                yield (row_number, list(keys), list(values))
            else:
                error = errors.SourceError(note='JSON item has to be list or dict')
                raise exceptions.FrictionlessException(error)
