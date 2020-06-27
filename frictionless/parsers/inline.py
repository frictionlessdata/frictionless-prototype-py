from collections import OrderedDict
from ..parser import Parser
from .. import exceptions
from .. import dialects


class InlineParser(Parser):
    Dialect = dialects.InlineDialect
    loading = False

    # Read

    def read_data_stream_create(self):
        items = self.file.source
        if not hasattr(items, '__iter__'):
            items = items()
        for row_number, item in enumerate(items, start=1):
            if isinstance(item, (tuple, list)):
                yield (row_number, None, list(item))
            elif isinstance(item, dict):
                keys = []
                values = []
                iterator = item.keys()
                if not isinstance(item, OrderedDict):
                    iterator = sorted(iterator)
                for key in iterator:
                    keys.append(key)
                    values.append(item[key])
                yield (row_number, list(keys), list(values))
            else:
                message = 'Inline data item has to be tuple, list or dict'
                raise exceptions.SourceError(message)
