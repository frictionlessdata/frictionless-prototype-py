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
        if self.file.dialect.property is not None:
            path = "%s.item" % self.file.dialect.property
        source = ijson.items(self.loader.byte_stream, path)
        with system.create_parser(File(source=source)) as parser:
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
        source = iter(jsonlines.Reader(self.loader.text_stream))
        with system.create_parser(File(source=source)) as parser:
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
