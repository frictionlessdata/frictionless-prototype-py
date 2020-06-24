from .system import system
from .helpers import cached_property


class Parser:
    """Abstract class implemented by the data parsers.

    The parsers inherit and implement this class' methods to add support for a
    new file type.

    # Arguments
        loader (tabulator.Loader): Loader instance to read the file.
        **options (dict): Loader options

    """

    def __init__(self, location, *, control=None, dialect=None):
        self.__location = location
        self.__control = control
        self.__dialect = dialect
        self.__loader = None

    @property
    def location(self):
        return self.__location

    @property
    def control(self):
        return self.__control

    @property
    def dialect(self):
        return self.__dialect

    @property
    def loader(self):
        return self.__loader

    @property
    def scheme(self):
        return getattr(self.loader, 'scheme', self.location.scheme)

    @property
    def format(self):
        return getattr(self.loader, 'format', self.location.format)

    @property
    def hashing(self):
        return getattr(self.loader, 'hashing', self.location.hashing)

    @property
    def encoding(self):
        return getattr(self.loader, 'encoding', self.location.encoding)

    @property
    def compression(self):
        return getattr(self.loader, 'compression', self.location.compression)

    @property
    def compression_file(self):
        return getattr(self.loader, 'compression_file', self.location.compression_file)

    @property
    def stats(self):
        return getattr(self.loader, 'stats', {})

    # Close

    def close(self):
        if self.loader:
            self.loader.close()

    # Read

    def read_line_stream(self):
        self.__loader = self.read_line_stream_create_loader()
        return self.read_line_stream_create()

    def read_line_stream_create(self, loader):
        raise NotImplementedError

    def read_line_stream_create_loader(self):
        return system.create_loader(self.location, control=self.control)
