from .system import system


class Parser:
    """Abstract class implemented by the data parsers.

    The parsers inherit and implement this class' methods to add support for a
    new file type.

    # Arguments
        loader (tabulator.Loader): Loader instance to read the file.
        **options (dict): Loader options

    """

    def __init__(self, file, *, control=None, dialect=None):
        self.__file = file
        self.__control = control
        self.__dialect = dialect
        self.__loader = None

    @property
    def file(self):
        return self.__file

    @property
    def control(self):
        return getattr(self.loader, 'control', self.__control)

    @property
    def dialect(self):
        return self.__dialect

    @property
    def loader(self):
        return self.__loader

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
        return system.create_loader(self.file, control=self.control)
