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

    # Read

    def read_data_stream_create(self, dialect=None):
        path = "item"
        dialect = self.file.dialect
        if dialect.property is not None:
            path = "%s.item" % self.file.dialect.property
        source = ijson.items(self.loader.byte_stream, path)
        file = File(source, dialect=dialects.InlineDialect(keys=dialect.keys))
        with system.create_parser(file) as parser:
            yield next(parser.data_stream)
            if parser.file.dialect.keyed:
                dialect["keyed"] = True
            yield from parser.data_stream

    # Write

    # TODO: use tempfile to prevent loosing data
    def write(self, row_stream, *, schema):
        data = []
        dialect = self.file.dialect
        helpers.ensure_dir(self.file.source)
        for row in row_stream:
            item = row.to_dict() if dialect.keyed else list(row.values())
            if not dialect.keyed and row.row_number == 1:
                data.append(schema.field_names)
            data.append(item)
        with open(self.file.source, "w") as file:
            json.dump(data, file, indent=2)


class JsonlParser(Parser):

    # Read

    def read_data_stream_create(self, dialect=None):
        dialect = self.file.dialect
        source = iter(jsonlines.Reader(self.loader.text_stream))
        file = File(source, dialect=dialects.InlineDialect(keys=dialect.keys))
        with system.create_parser(file) as parser:
            yield next(parser.data_stream)
            if parser.file.dialect.keyed:
                dialect["keyed"] = True
            yield from parser.data_stream

    # Write

    # TODO: use tempfile to prevent loosing data
    def write(self, row_stream, *, schema):
        dialect = self.file.dialect
        helpers.ensure_dir(self.file.source)
        with io.open(self.file.source, "wb") as file:
            writer = jsonlines.Writer(file)
            for row in row_stream:
                item = row.to_dict() if dialect.keyed else list(row.values())
                if not dialect.keyed and row.row_number == 1:
                    writer.write(schema.field_names)
                writer.write(item)


# Internal


NATIVE_TYPES = [
    "array",
    "boolean",
    "geojson",
    "integer",
    "number",
    "object",
    "string",
    "year",
]
