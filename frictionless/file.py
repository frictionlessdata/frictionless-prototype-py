import os
import stringcase
from .helpers import cached_property
from .metadata import ControlledMetadata
from .controls import Control
from .dialects import Dialect
from . import helpers
from . import config


class File(ControlledMetadata):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'required': ['source'],
        'properties': {
            'source': {},
            'scheme': {'type': 'string'},
            'format': {'type': 'string'},
            'hashing': {'type': 'string'},
            'encoding': {'type': 'string'},
            'compression': {'type': 'string'},
            'compressionPath': {'type': 'string'},
            'contorl': {'type': 'object'},
            'dialect': {'type': 'object'},
            'newline': {'type': 'string'},
            'stats': {
                'type': 'object',
                'required': ['hash', 'bytes'],
                'properties': {'hash': {'type': 'string'}, 'bytes': {'type': 'number'}},
            },
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        source=None,
        scheme=None,
        format=None,
        hashing=None,
        encoding=None,
        compression=None,
        compression_path=None,
        control=None,
        dialect=None,
        newline=None,
        stats=None,
    ):
        self.setdefined('source', source)
        self.setdefined('scheme', scheme)
        self.setdefined('format', format)
        self.setdefined('hashing', hashing)
        self.setdefined('encoding', encoding)
        self.setdefined('compression', compression)
        self.setdefined('compressionPath', compression_path)
        self.setdefined('control', control)
        self.setdefined('dialect', dialect)
        self.setdefined('newline', newline)
        self.setdefined('stats', stats)
        super().__init__(descriptor)
        # Infer from source
        infer = helpers.infer_source_scheme_and_format(source)
        self.__infered_compression = config.DEFAULT_COMPRESSION
        if infer[1] in config.COMPRESSION_FORMATS:
            self.__infered_compression = infer[1]
            source = source[: -len(infer[1]) - 1]
            if compression_path:
                source = os.path.join(source, compression_path)
            infer = helpers.infer_source_scheme_and_format(source)
        self.__infered_scheme = infer[0] or config.DEFAULT_SCHEME
        self.__infered_format = infer[1] or config.DEFAULT_FORMAT

    def __setattr__(self, name, value):
        if name in [
            'scheme',
            'format',
            'hashing',
            'encoding',
            'compression',
            'compressionPath',
            'control',
            'dialect',
            'newline',
            'stats',
        ]:
            self[stringcase.camelcase(name)] = value
        super().__setattr__(name, value)

    @cached_property
    def path(self):
        return self.source if isinstance(self.source, str) else 'memory'

    @cached_property
    def source(self):
        return self.get('source')

    @cached_property
    def scheme(self):
        return self.get('scheme', self.__infered_scheme)

    @cached_property
    def format(self):
        return self.get('format', self.__infered_format)

    @cached_property
    def hashing(self):
        return self.get('hashing', config.DEFAULT_HASHING)

    @cached_property
    def encoding(self):
        return self.get('encoding', config.DEFAULT_ENCODING)

    @cached_property
    def compression(self):
        return self.get('compression', self.__infered_compression)

    @cached_property
    def compression_path(self):
        return self.get('compressionPath')

    @cached_property
    def control(self):
        return self.get('control')

    @cached_property
    def dialect(self):
        return self.get('dialect')

    @cached_property
    def newline(self):
        return self.get('newline')

    @cached_property
    def stats(self):
        return self.get('stats')

    # Expand

    def expand(self):
        self.setdetault('scheme', self.scheme)
        self.setdetault('format', self.format)
        self.setdetault('hashing', self.hashing)
        self.setdetault('encoding', self.hashing)
        self.setdetault('compression', self.compression)

    # Metadata

    def metadata_process(self):
        for name, Class in [('control', Control), ('dialect', Dialect)]:
            value = self.get(name)
            if value is not None and not isinstance(value, Class):
                value = Class(
                    value,
                    metadata_root=self.metadata_root,
                    #  metadata_strict=self.metadata_strict,
                )
                dict.__setitem__(self, name, value)
        super().metadata_process(skip=['source'])
