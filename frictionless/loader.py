import io
import gzip
import codecs
import shutil
import hashlib
import zipfile
import tempfile
from . import exceptions
from . import errors
from . import config


class Loader:
    """Loader representation

    API      | Usage
    -------- | --------
    Public   | `from frictionless import Loader`

    Parameters:
        file (File): file

    """

    remote = False

    def __init__(self, file):
        self.__file = file
        self.__byte_stream = None
        self.__text_stream = None

    def __enter__(self):
        if self.closed:
            self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def file(self):
        """
        Returns:
            file (File): file
        """
        return self.__file

    @property
    def byte_stream(self):
        """File byte stream

        The stream is available after opening the loader

        Returns:
            io.ByteStream: file byte stream
        """
        return self.__byte_stream

    @property
    def text_stream(self):
        """File text stream

        The stream is available after opening the loader

        Returns:
            io.TextStream: file text stream
        """
        if not self.__text_stream:
            self.__text_stream = self.read_text_stream()
        return self.__text_stream

    # Open/Close

    def open(self):
        """Open the loader as "io.open" does"""
        self.close()
        if self.__file.control.metadata_errors:
            error = self.__file.control.metadata_errors[0]
            raise exceptions.FrictionlessException(error)
        try:
            self.__byte_stream = self.read_byte_stream()
            return self
        except Exception:
            self.close()
            raise

    def close(self):
        """Close the loader as "filelike.close" does"""
        if self.__byte_stream:
            self.__byte_stream.close()
        self.__byte_stream = None

    @property
    def closed(self):
        """Whether the loader is closed

        Returns:
            bool: if closed
        """
        return self.__byte_stream is None

    # Read

    def read_byte_stream(self):
        """Read bytes stream

        Returns:
            io.ByteStream: file byte stream
        """
        try:
            byte_stream = self.read_byte_stream_create()
            byte_stream = self.read_byte_stream_infer_stats(byte_stream)
            byte_stream = self.read_byte_stream_decompress(byte_stream)
        except IOError as exception:
            error = errors.SchemeError(note=str(exception))
            raise exceptions.FrictionlessException(error)
        except config.COMPRESSION_EXCEPTIONS as exception:
            error = errors.CompressionError(note=str(exception))
            raise exceptions.FrictionlessException(error)
        return byte_stream

    def read_byte_stream_create(self):
        """Create bytes stream

        Returns:
            io.ByteStream: file byte stream
        """
        raise NotImplementedError

    def read_byte_stream_infer_stats(self, byte_stream):
        """Infer byte stream stats

        Parameters:
            byte_stream (io.ByteStream): file byte stream

        Returns:
            io.ByteStream: file byte stream
        """
        if not self.file.stats:
            return byte_stream
        return ByteStreamWithStatsHandling(
            byte_stream,
            hashing=self.file.hashing,
            stats=self.file.stats if not self.file.stats["hash"] else {},
        )

    def read_byte_stream_decompress(self, byte_stream):
        """Decompress byte stream

        Parameters:
            byte_stream (io.ByteStream): file byte stream

        Returns:
            io.ByteStream: file byte stream
        """
        if self.file.compression == "zip":
            # Remote
            if self.remote:
                self.remote = False
                target = tempfile.NamedTemporaryFile()
                shutil.copyfileobj(byte_stream, target)
                target.seek(0)
                byte_stream = target
            # Stats
            else:
                bytes = True
                while bytes:
                    bytes = byte_stream.read1(io.DEFAULT_BUFFER_SIZE)
                byte_stream.seek(0)
            # Unzip
            with zipfile.ZipFile(byte_stream) as archive:
                name = self.file.compression_path or archive.namelist()[0]
                with archive.open(name) as file:
                    target = tempfile.NamedTemporaryFile()
                    shutil.copyfileobj(file, target)
                    target.seek(0)
                byte_stream = target
                self.file["compressionPath"] = name
            return byte_stream
        if self.file.compression == "gz":
            byte_stream = gzip.open(byte_stream)
            return byte_stream
        if self.file.compression == "no":
            return byte_stream
        note = f'compression "{self.file.compression}" is not supported'
        raise exceptions.FrictionlessException(errors.CompressionError(note=note))

    def read_text_stream(self):
        """Read text stream

        Returns:
            io.TextStream: file text stream
        """
        try:
            self.read_text_stream_infer_encoding(self.byte_stream)
        except (LookupError, UnicodeDecodeError) as exception:
            error = errors.EncodingError(note=str(exception))
            raise exceptions.FrictionlessException(error) from exception
        return self.read_text_stream_decode(self.byte_stream)

    def read_text_stream_infer_encoding(self, byte_stream):
        """Infer text stream encoding

        Parameters:
            byte_stream (io.ByteStream): file byte stream
        """
        control = self.file.control
        encoding = self.file.get("encoding")
        sample = byte_stream.read(config.DEFAULT_INFER_ENCODING_VOLUME)
        sample = sample[: config.DEFAULT_INFER_ENCODING_VOLUME]
        byte_stream.seek(0)
        if encoding is None:
            encoding = control.detect_encoding(sample)
        encoding = codecs.lookup(encoding).name
        # Work around  for incorrect inferion of utf-8-sig encoding
        if encoding == "utf-8":
            if sample.startswith(codecs.BOM_UTF8):
                encoding = "utf-8-sig"
        # Use the BOM stripping name (without byte-order) for UTF-16 encodings
        elif encoding == "utf-16-be":
            if sample.startswith(codecs.BOM_UTF16_BE):
                encoding = "utf-16"
        elif encoding == "utf-16-le":
            if sample.startswith(codecs.BOM_UTF16_LE):
                encoding = "utf-16"
        self.file["encoding"] = encoding

    def read_text_stream_decode(self, byte_stream):
        """Decode text stream

        Parameters:
            byte_stream (io.ByteStream): file byte stream

        Returns:
            text_stream (io.TextStream): file text stream
        """
        return io.TextIOWrapper(
            byte_stream, self.file.encoding, newline=self.file.newline
        )


# Internal


# NOTE: implement __iter__
# NOTE: review read/read1 usage
# NOTE: Try buffering byte sample especially for remote
class ByteStreamWithStatsHandling:
    def __init__(self, byte_stream, *, hashing, stats):
        try:
            self.__hasher = hashlib.new(hashing) if hashing else None
        except Exception as exception:
            error = errors.HashingError(note=str(exception))
            raise exceptions.FrictionlessException(error)
        self.__byte_stream = byte_stream
        self.__stats = stats or {}

    def __getattr__(self, name):
        return getattr(self.__byte_stream, name)

    @property
    def closed(self):
        return self.__byte_stream.closed

    def read1(self, size=None):
        chunk = self.__byte_stream.read1(size)
        self.__stats["bytes"] += len(chunk)
        if self.__hasher:
            self.__hasher.update(chunk)
            self.__stats["hash"] = self.__hasher.hexdigest()
        return chunk
