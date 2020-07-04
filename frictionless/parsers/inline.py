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
        dialect = self.file.dialect
        error = errors.SourceError(note="data item has to be tuple, list or dict")

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
            if not isinstance(cells, list):
                error = errors.SourceError(note="all data items must be lists")
                raise exceptions.FrictionlessException(error)
            yield cells

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
