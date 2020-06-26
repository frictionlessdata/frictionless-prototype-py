from .system import system


class Parser:
    """Parser representation

    # Arguments
        file (File): file

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    Dialect = None
    newline = None
    loading = True

    def __init__(self, file):
        self.__file = file
        self.__loader = None
        self.__cell_stream = None
        if self.Dialect is not None:
            self.__file.dialect = self.Dialect(file.dialect, metadata_root=file)
        if self.newline is not None:
            self.__file.newline = self.newline

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def file(self):
        return self.__file

    @property
    def loader(self):
        return self.__loader

    @property
    def cell_stream(self):
        return self.__cell_stream

    # Manage

    def open(self):
        self.close()
        self.__loader = self.read_loader()
        self.__cell_stream = self.read_cell_stream()
        return self

    def close(self):
        if self.__loader:
            self.__loader.close()

    # Read

    def read_loader(self):
        if self.loading:
            loader = system.create_loader(self.file)
            return loader.open()

    def read_cell_stream(self):
        return self.read_cell_stream_create()

    def read_cell_stream_create(self, loader):
        raise NotImplementedError
