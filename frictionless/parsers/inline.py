from collections import OrderedDict
from ..parser import Parser
from .. import exceptions


class InlineParser(Parser):

    # Read

    def read_line_stream_create(self):
        for row_number, item in enumerate(self.source, start=1):
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
