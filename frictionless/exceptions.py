class FrictionlessException(Exception):
    """Main Frictionless exception

    # Arguments
        error

    """

    def __init__(self, error):
        self.__error = error
        super().__init__(self.__error.message)

    @property
    def error(self):
        return self.__error


# TODO: remove
# Tabulator


class TabulatorException(Exception):
    """Base class for all tabulator exceptions.
    """

    pass


class SourceError(TabulatorException):
    """The source file could not be parsed correctly.
    """

    pass


class SchemeError(TabulatorException):
    """The file scheme is not supported.
    """

    pass


class FormatError(TabulatorException):
    """The file format is unsupported or invalid.
    """

    pass


class EncodingError(TabulatorException):
    """Encoding error
    """

    pass


class CompressionError(TabulatorException):
    """Compression error
    """

    pass


# Deprecated

OptionsError = TabulatorException
ResetError = TabulatorException


class IOError(SchemeError):
    """Local loading error
    """

    pass


class LoadingError(IOError):
    """Local loading error
    """

    pass


class HTTPError(LoadingError):
    """Remote loading error
    """

    pass
