from itertools import chain
from collections import OrderedDict
from ..parser import Parser
from .. import exceptions
from .. import errors


class InlineParser(Parser):
    """Inline parser implementation.

    API      | Usage
    -------- | --------
    Public   | `from frictionless import parsers`

    """

    loading = False
    native_types = [
        "array",
        "boolean",
        "date",
        "datetime",
        "duration",
        "geojson",
        "geopoint",
        "integer",
        "number",
        "object",
        "string",
        "time",
        "year",
        "yearmonth",
    ]

    # Read

    def read_data_stream_create(self):
        dialect = self.file.dialect

        # Iter
        data = self.file.source
        if not hasattr(data, "__iter__"):
            data = data()
        data = iter(data)

        # Empty
        try:
            cells = next(data)
        except StopIteration:
            yield from []
            return

        # Keyed
        if isinstance(cells, dict):
            dialect["keyed"] = True
            headers = dialect.keys or list(cells.keys())
            if not dialect.keys and not isinstance(cells, OrderedDict):
                headers = sorted(headers)
            yield headers
            for cells in chain([cells], data):
                if not isinstance(cells, dict):
                    error = errors.SourceError(note="all keyed data items must be dicts")
                    raise exceptions.FrictionlessException(error)
                yield [cells.get(header) for header in headers]
            return

        # General
        for cells in chain([cells], data):
            if not isinstance(cells, (list, tuple)):
                error = errors.SourceError(note="all data items must be lists")
                raise exceptions.FrictionlessException(error)
            yield cells

    # Write

    def write(self, row_stream):
        dialect = self.file.dialect
        for row in row_stream:
            item = row.to_dict() if dialect.keyed else list(row.values())
            if not dialect.keyed and row.row_number == 1:
                self.file.source.append(row.schema.field_names)
            self.file.source.append(item)
