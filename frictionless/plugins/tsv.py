from importlib import import_module
from ..parser import Parser
from ..plugin import Plugin
from ..dialects import Dialect


# Plugin


class TsvPlugin(Plugin):
    def create_parser(self, file):
        if file.format == "tsv":
            return TsvParser(file)


# Parser


# TODO: implement write
class TsvParser(Parser):
    Dialect = property(lambda self: TsvDialect)

    # Read

    def read_data_stream_create(self):
        tsv = import_module("tsv")
        data = tsv.un(self.loader.text_stream)
        yield from data


# Dialect


class TsvDialect(Dialect):
    """Tsv dialect representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    pass
