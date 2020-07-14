import io
from importlib import import_module
from datetime import datetime
from ..metadata import Metadata
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser
from .. import exceptions
from .. import errors


# Plugin


class OdsPlugin(Plugin):
    def create_dialect(self, file, *, descriptor):
        if file.format == "ods":
            return OdsDialect(descriptor)

    def create_parser(self, file):
        if file.format == "ods":
            return OdsParser(file)


# Parser


class OdsParser(Parser):

    # Read

    def read_data_stream_create(self):
        ezodf = import_module("ezodf")
        dialect = self.file.dialect

        # Get book
        book = ezodf.opendoc(io.BytesIO(self.loader.byte_stream.read()))

        # Get sheet
        try:
            if isinstance(dialect.sheet, str):
                sheet = book.sheets[dialect.sheet]
            else:
                sheet = book.sheets[dialect.sheet - 1]
        except (KeyError, IndexError):
            note = 'OpenOffice document "%s" does not have a sheet "%s"'
            note = note % (self.file.source, dialect.sheet)
            raise exceptions.FrictionlessException(errors.FormatError(note=note))

        # Type cells
        def type_value(cell):
            """Detects int value, date and datetime"""

            ctype = cell.value_type
            value = cell.value

            # ods numbers are float only
            # float with no decimals can be cast into int
            if isinstance(value, float) and value == value // 1:
                return int(value)

            # Date or datetime
            if ctype == "date":
                if len(value) == 10:
                    return datetime.strptime(value, "%Y-%m-%d").date()
                else:
                    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")

            return value

        # Stream data
        for cells in sheet.rows():
            yield [type_value(cell) for cell in cells]


# Dialect


class OdsDialect(Dialect):
    """Ods dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        sheet? (str): sheet

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "sheet": {"type": ["number", "string"]},
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        sheet=None,
        header=None,
        header_rows=None,
        header_join=None,
    ):
        self.setinitial("sheet", sheet)
        super().__init__(
            descriptor=descriptor,
            header=header,
            header_rows=header_rows,
            header_join=header_join,
        )

    @Metadata.property
    def sheet(self):
        return self.get("sheet", 1)

    # Expand

    def expand(self):
        super().expand()
        self.setdefault("sheet", self.sheet)
