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
