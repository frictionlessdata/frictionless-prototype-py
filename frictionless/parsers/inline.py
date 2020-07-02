from itertools import chain
from collections import OrderedDict
from ..parser import Parser
from .. import exceptions
from .. import dialects
from .. import errors


class InlineParser(Parser):
    Dialect = dialects.InlineDialect
    loading = False

    # Read

    def read_data_stream_create(self):
        data = self.file.source
        if not hasattr(data, "__iter__"):
            data = data()
        data = iter(data)

        # Empty
        try:
            cells = next(data)
        except StopIteration:
            yield from []

        # Lined
        if isinstance(cells, (tuple, list)):
            yield from chain([cells], data)

        # Keyed
        elif isinstance(cells, dict):
            headers = list(cells.keys())
            if not isinstance(cells, OrderedDict):
                headers = sorted(headers)
            yield headers
            for cells in chain([cells], data):
                yield [cells.get(header) for header in headers]

        # Error
        else:
            note = "data item has to be tuple, list or dict"
            raise exceptions.FrictionlessException(errors.SourceError(note=note))

    # Write

    def write(self, data_stream):
        dialect = self.file.dialect
        headers = next(data_stream)
        if not dialect.keyed:
            self.file.source.append(headers)
        for item in data_stream:
            if dialect.keyed:
                item = dict(zip(headers, item))
            self.file.source.append(item)
