import re
from ..file import File
from ..plugin import Plugin
from ..parser import Parser
from ..system import system
from ..dialects import Dialect


# Plugin


class GsheetPlugin(Plugin):
    def create_parser(self, file):
        if file.format == 'gsheet':
            return GsheetParser(file)


# Parser


class GsheetParser(Parser):
    Dialect = property(lambda self: GsheetDialect)
    network = True
    loading = False

    # Manage

    def open(self):
        self.__parser = None
        super().open()

    def close(self):
        if self.__parser:
            self.__parser.close()

    # Read

    def read_cell_stream_create(self):
        source = self.file.source
        url = 'https://docs.google.com/spreadsheets/d/%s/export?format=csv&id=%s'
        match = re.search(r'.*/d/(?P<key>[^/]+)/.*?(?:gid=(?P<gid>\d+))?$', source)
        key, gid = '', ''
        if match:
            key = match.group('key')
            gid = match.group('gid')
        url = url % (key, key)
        if gid:
            url = '%s&gid=%s' % (url, gid)
        file = File(source=url, stats=self.file.stats)
        self.__parser = system.create_parser(file)
        self.__parser.open()
        return self.__parser.cell_stream


# Dialect


class GsheetDialect(Dialect):
    """Gsheet dialect representation

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
