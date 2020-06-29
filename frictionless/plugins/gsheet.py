import re
from ..file import File
from ..plugin import Plugin
from ..parser import Parser
from ..system import system
from ..dialects import Dialect


# Plugin


class GsheetPlugin(Plugin):
    def create_parser(self, file):
        if file.format == "gsheet":
            return GsheetParser(file)


# Parser


class GsheetParser(Parser):
    Dialect = property(lambda self: GsheetDialect)
    network = True
    loading = False

    # Read

    def read_data_stream_create(self):
        source = self.file.source
        match = re.search(r".*/d/(?P<key>[^/]+)/.*?(?:gid=(?P<gid>\d+))?$", source)
        source = "https://docs.google.com/spreadsheets/d/%s/export?format=csv&id=%s"
        key, gid = "", ""
        if match:
            key = match.group("key")
            gid = match.group("gid")
        source = source % (key, key)
        if gid:
            source = "%s&gid=%s" % (source, gid)
        with system.create_parser(File(source=source, stats=self.file.stats)) as parser:
            yield from parser.data_stream


# Dialect


class GsheetDialect(Dialect):
    """Gsheet dialect representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    pass
