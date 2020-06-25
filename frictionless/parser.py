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
        self.__file.dialect = self.Dialect(self.__file.dialect)
        self.__loader = None

    @property
    def file(self):
        return self.__file

    @property
    def loader(self):
        return self.__loader

    # Admin

    def open(self):
        self.close()
        self.__loader = self.read_cell_stream_open()

    def close(self):
        if self.__loader:
            self.__loader.close()

    # Read

    def read_cell_stream(self):
        return self.read_cell_stream_create()

    def read_cell_stream_open(self):
        loader = system.create_loader(self.file)
        loader.open()
        return loader

    def read_cell_stream_create(self, loader):
        raise NotImplementedError
