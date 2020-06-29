import json
import ijson
import jsonlines
from ..file import File
from ..parser import Parser
from ..system import system
from .. import dialects
from .. import helpers


class JsonParser(Parser):
    Dialect = dialects.JsonDialect

    # Read

    def read_data_stream_create(self, dialect=None):
        path = "item"
        if self.file.dialect.property is not None:
            path = "%s.item" % self.file.dialect.property
        data = ijson.items(self.loader.byte_stream, path)
        with system.create_parser(File(source=data)) as parser:
            yield from parser.data_stream

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
        with open(target, "w") as file:
            json.dump(data, file, indent=2)
        return count


class JsonlParser(Parser):
    Dialect = dialects.JsonDialect

    # Read

    def read_data_stream_create(self, dialect=None):
        data = iter(jsonlines.Reader(self.loader.text_stream))
        with system.create_parser(File(source=data)) as parser:
            yield from parser.data_stream
