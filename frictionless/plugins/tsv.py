import tsv
from ..parser import Parser
from ..plugin import Plugin
from ..dialects import Dialect


# Plugin


class TsvPlugin(Plugin):
    def create_parser(self, file):
        if file.format == 'tsv':
            return TsvParser(file)


# Parser


class TsvParser(Parser):
    Dialect = property(lambda self: TsvDialect)

    # Read

    def read_cell_stream_create(self):
        items = tsv.un(self.loader.text_stream)
        for row_number, item in enumerate(items, start=1):
            yield (row_number, None, list(item))


# Dialect


class TsvDialect(Dialect):
    """Tsv dialect representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'additionalProperties': False,
        'properties': {},
    }
