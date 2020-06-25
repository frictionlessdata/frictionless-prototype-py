from .system import system


class Parser:
    """Parser representation

    # Arguments
        file (File): file

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    Dialect = None

    def __init__(self, file):
        self.__file = file
        self.__file.dialect = self.Dialect(self.__file.dialect, metadata_root=file)
        self.__loader = None
        self.__cell_stream = None

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

    def close(self):
        if self.__loader:
            self.__loader.close()

    # Read

    def read_loader(self):
        loader = self.read_loader_create()
        if loader:
            self.read_loader_open(loader)
        return loader

    def read_loader_create(self):
        return system.create_loader(self.file)

    def read_loader_open(self, loader):
        loader.open()

    def read_cell_stream(self):
        return self.read_cell_stream_create()

    def read_cell_stream_create(self, loader):
        raise NotImplementedError
