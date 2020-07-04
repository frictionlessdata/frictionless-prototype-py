import io
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
        dialect = self.file.dialect
        if dialect.property is not None:
            path = "%s.item" % self.file.dialect.property
        source = ijson.items(self.loader.byte_stream, path)
        file = File(source=source, dialect=dialects.InlineDialect(keys=dialect.keys))
        with system.create_parser(file) as parser:
            yield next(parser.data_stream)
            if parser.file.dialect.keyed:
                dialect["keyed"] = True
            yield from parser.data_stream

    # Write

    # TODO: use tempfile to prevent loosing data
    def write(self, data_stream):
        data = []
        dialect = self.file.dialect
        helpers.ensure_dir(self.file.source)
        headers = next(data_stream)
        if not dialect.keyed:
            data.append(headers)
        for item in data_stream:
            if dialect.keyed:
                item = dict(zip(headers, item))
            data.append(item)
        with open(self.file.source, "w") as file:
            json.dump(data, file, indent=2)


class JsonlParser(Parser):
    Dialect = dialects.JsonDialect

    # Read

    def read_data_stream_create(self, dialect=None):
        dialect = self.file.dialect
        source = iter(jsonlines.Reader(self.loader.text_stream))
        file = File(source=source, dialect=dialects.InlineDialect(keys=dialect.keys))
        with system.create_parser(file) as parser:
            yield next(parser.data_stream)
            if parser.file.dialect.keyed:
                dialect["keyed"] = True
            yield from parser.data_stream

    # Write

    # TODO: use tempfile to prevent loosing data
    def write(self, data_stream):
        dialect = self.file.dialect
        helpers.ensure_dir(self.file.source)
        headers = next(data_stream)
        with io.open(self.file.source, "wb") as file:
            writer = jsonlines.Writer(file)
            if not dialect.keyed:
                writer.write(headers)
            for item in data_stream:
                if dialect.keyed:
                    item = dict(zip(headers, item))
                writer.write(item)
