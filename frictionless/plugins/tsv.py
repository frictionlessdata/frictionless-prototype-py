import tempfile
from ..parser import Parser
from ..plugin import Plugin
from ..dialects import Dialect
from .. import helpers


# Plugin


class TsvPlugin(Plugin):
    def create_dialect(self, file, *, descriptor):
        if file.format == "tsv":
            return TsvDialect(descriptor)

    def create_parser(self, file):
        if file.format == "tsv":
            return TsvParser(file)


# Parser


class TsvParser(Parser):
    native_types = [
        "string",
    ]

    # Read

    def read_data_stream_create(self):
        tsv = helpers.import_from_plugin("tsv", plugin="tsv")
        data = tsv.reader(self.loader.text_stream)
        yield from data

    # Write

    def write(self, row_stream):
        tsv = helpers.import_from_plugin("tsv", plugin="tsv")
        with tempfile.NamedTemporaryFile("wt", delete=False) as file:
            writer = tsv.writer(file)
            for row in row_stream:
                schema = row.schema
                if row.row_number == 1:
                    writer.writerow(schema.field_names)
                cells = list(row.values())
                cells, notes = schema.write_data(cells, native_types=self.native_types)
                writer.writerow(cells)
        helpers.move_file(file.name, self.file.source)


# Dialect


class TsvDialect(Dialect):
    """Tsv dialect representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    pass
