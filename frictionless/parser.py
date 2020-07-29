from .system import system
from . import exceptions
from . import errors
from . import config


class Parser:
    """Parser representation

    # Arguments
        file (File): file

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    newline = None
    loading = True
    native_types = []

    def __init__(self, file):
        self.__file = file
        self.__loader = None
        self.__data_stream = None
        if self.newline is not None:
            self.__file["newline"] = self.newline

    def __enter__(self):
        if self.closed:
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
    def data_stream(self):
        return self.__data_stream

    # Open/Close

    def open(self):
        self.close()
        if self.__file.dialect.metadata_errors:
            error = self.__file.dialect.metadata_errors[0]
            raise exceptions.FrictionlessException(error)
        try:
            self.__loader = self.read_loader()
            self.__data_stream = self.read_data_stream()
            return self
        except Exception:
            self.close()
            raise

    def close(self):
        if self.__loader:
            self.__loader.close()

    @property
    def closed(self):
        return self.__loader is None

    # Read

    def read_loader(self):
        if self.loading:
            loader = system.create_loader(self.file)
            return loader.open()

    def read_data_stream(self):
        data_stream = self.read_data_stream_create()
        data_stream = self.read_data_stream_handle_errors(data_stream)
        return data_stream

    def read_data_stream_create(self, loader):
        raise NotImplementedError

    def read_data_stream_handle_errors(self, data_stream):
        return DataStreamWithErrorHandling(data_stream)

    # Write

    def write(self, row_stream):
        raise NotImplementedError


# Internal


# NOTE: Try moving loader related errors to loader
class DataStreamWithErrorHandling:
    def __init__(self, data_stream):
        self.data_stream = data_stream

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.data_stream.__next__()
        except StopIteration:
            raise
        except exceptions.FrictionlessException:
            raise
        except config.COMPRESSION_EXCEPTIONS as exception:
            error = errors.CompressionError(note=str(exception))
            raise exceptions.FrictionlessException(error)
        except UnicodeDecodeError as exception:
            error = errors.EncodingError(note=str(exception))
            raise exceptions.FrictionlessException(error) from exception
        except Exception as exception:
            error = errors.SourceError(note=str(exception))
            raise exceptions.FrictionlessException(error) from exception
