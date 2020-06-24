from .helpers import cached_property
from . import helpers
from . import config


class Location:
    def __init__(
        self,
        source,
        *,
        scheme=None,
        format=None,
        hashing=None,
        encoding=None,
        compression=None,
        compression_file=None
    ):
        self.__source = source
        self.__scheme = scheme
        self.__format = format
        self.__hashing = hashing
        self.__encoding = encoding
        self.__compression = compression
        self.__compression_file = compression_file
        detect = helpers.detect_source_scheme_and_format(source)
        if detect[1] in config.SUPPORTED_COMPRESSION:
            self.__detected_compression = detect[1]
            detect = helpers.detect_source_scheme_and_format(source[: -len(detect[1])])
        self.__detected_scheme = detect[0]
        self.__detected_format = detect[1]

    @cached_property
    def path(self):
        return self.source if isinstance(self.source, str) else 'memory'

    @cached_property
    def source(self):
        return self.__source

    @cached_property
    def scheme(self):
        return self.__scheme or self.__detected_scheme

    @cached_property
    def format(self):
        return self.__format or format

    @cached_property
    def hashing(self):
        return self.__hashing or config.DEFAULT_HASHING

    @cached_property
    def encoding(self):
        return self.__encoding

    @cached_property
    def compression(self):
        return self.__compression or self.__detected_compression

    @cached_property
    def compression_file(self):
        return self.__compression_file
