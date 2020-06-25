import io
import os
import gzip
import codecs
import atexit
import hashlib
import chardet
import zipfile
import tempfile
from . import exceptions
from . import errors
from . import config


class Loader:
    """Loader representation

    # Arguments
        file (File): file

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    Control = None
    network = False

    def __init__(self, file):
        self.__file = file
        self.__file.control = self.Control(self.__file.control)
        self.__byte_stream = None

    @property
    def file(self):
        return self.__file

    @property
    def byte_stream(self):
        return self.__byte_stream

    # Close

    @property
    def closed(self):
        return not self.byte_stream or self.byte_stream.closed

    def close(self):
        if self.byte_stream:
            self.byte_stream.close()

    # Read

    def read_text_stream(self):
        """Create texts stream

        # Returns
            TextIO: I/O stream

        """
        byte_stream = self.read_byte_stream()
        byte_stream = self.read_byte_stream_detect_encoding(byte_stream)
        text_stream = io.TextIOWrapper(byte_stream, self.encoding)
        return text_stream

    def read_byte_stream(self):
        """Create bytes stream

        # Returns
            BinaryIO: I/O stream

        """
        self.close()
        try:
            byte_stream = self.read_byte_stream_create()
        except IOError as exception:
            error = errors.SchemeError(note=str(exception))
            raise exceptions.FrictionlessException(error)
        byte_stream = self.read_byte_stream_decompress(byte_stream)
        byte_stream = self.read_byte_stream_detect_stats(byte_stream)
        self.__byte_stream
        return byte_stream

    def read_byte_stream_create(self):
        raise NotImplementedError

    def read_byte_stream_decompress(self, byte_stream):
        if self.compression == 'zip':
            self.network = False
            with zipfile.ZipFile(byte_stream) as archive:
                name = self.compression_file or archive.namelist()[0]
                with archive.open(name) as file:
                    byte_stream = tempfile.NamedTemporaryFile(suffix='.' + name)
                    atexit.register(os.remove, byte_stream.name)
                    for line in file:
                        byte_stream.write(line)
                    byte_stream.seek(0)
            return byte_stream
        if self.compression == 'gz':
            byte_stream = gzip.open(byte_stream)
            return byte_stream
        if self.compression == 'no':
            return byte_stream
        note = f'Compression "{self.compression}" is not supported'
        raise exceptions.FrictionlessException(errors.CompressionError(note=note))

    def read_byte_stream_detect_stats(self, byte_stream):
        byte_stream = ByteStreamWithStats(byte_stream, hashing=self.hashing)
        self.file.stats = byte_stream.stats
        return byte_stream

    def read_byte_stream_detect_encoding(self, byte_stream):
        encoding = self.file.encoding
        if encoding is None:
            sample = byte_stream.read(config.DETECT_ENCODING_VOLUME)
            sample = sample[: config.DETECT_ENCODING_VOLUME]
            byte_stream.seek(0)
            result = chardet.detect(sample)
            confidence = result['confidence'] or 0
            encoding = result['encoding'] or 'ascii'
            if confidence < config.DETECT_ENCODING_CONFIDENCE:
                encoding = config.DEFAULT_ENCODING
            if encoding == 'ascii':
                encoding = config.DEFAULT_ENCODING
        encoding = codecs.lookup(encoding).name
        # Work around 'Incorrect detection of utf-8-sig encoding'
        # <https://github.com/PyYoshi/cChardet/issues/28>
        if encoding == 'utf-8':
            if sample.startswith(codecs.BOM_UTF8):
                encoding = 'utf-8-sig'
        # Use the BOM stripping name (without byte-order) for UTF-16 encodings
        elif encoding == 'utf-16-be':
            if sample.startswith(codecs.BOM_UTF16_BE):
                encoding = 'utf-16'
        elif encoding == 'utf-16-le':
            if sample.startswith(codecs.BOM_UTF16_LE):
                encoding = 'utf-16'
        self.file.encoding = encoding
        return byte_stream


# Internal


class ByteStreamWithStats(object):
    """This class is intended to be used as

    stats = {'size': 0, 'hash': ''}
    bytes = BytesStatsWrapper(bytes, stats)

    It will be updating the stats during reading.

    # Arguments
        byte_stream
        hashing

    """

    def __init__(self, byte_stream, *, hashing=None):
        try:
            self.__hasher = getattr(hashlib, hashing)() if hashing else None
        except Exception as exception:
            error = errors.HashingError(note=str(exception))
            raise exceptions.FrictionlessException(error)
        self.__byte_stream = byte_stream
        self.__stats = {'size': 0, 'hash': ''}

    def __getattr__(self, name):
        return getattr(self.__byte_stream, name)

    @property
    def stats(self):
        return self.__stats

    @property
    def closed(self):
        return self.__byte_stream.closed

    def read1(self, size=None):
        chunk = self.__byte_stream.read1(size)
        self.__stats['size'] += len(chunk)
        if self.__hasher:
            self.__hasher.update(chunk)
            self.__stats['hash'] = self.__hasher.hexdigest()
        return chunk
