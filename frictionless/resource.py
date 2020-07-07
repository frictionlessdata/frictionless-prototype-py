import io
import os
from urllib.request import urlopen
from .helpers import cached_property
from .metadata import Metadata
from .dialects import Dialect
from .schema import Schema
from .system import system
from .table import Table
from .file import File
from . import exceptions
from . import dialects
from . import helpers
from . import errors
from . import config


class Resource(Metadata):
    # TODO: make profile a property
    metadata_Error = errors.ResourceError
    metadata_profile = config.RESOURCE_PROFILE
    metadata_setters = {
        "path": "path",
        "data": "data",
        "scheme": "scheme",
        "format": "format",
        "hashing": "hashing",
        "encoding": "encoding",
        "compression": "compression",
        "compression_path": "compressionPath",
        "dialect": "dialect",
        "schema": "schema",
    }

    def __init__(
        self,
        descriptor=None,
        *,
        path=None,
        data=None,
        scheme=None,
        format=None,
        hashing=None,
        encoding=None,
        compression=None,
        compression_path=None,
        dialect=None,
        schema=None,
        basepath=None,
    ):
        self.setinitial("path", path)
        self.setinitial("data", data)
        self.setinitial("scheme", scheme)
        self.setinitial("format", format)
        self.setinitial("hashing", hashing)
        self.setinitial("encoding", encoding)
        self.setinitial("compression", compression)
        self.setinitial("compressionPath", compression_path)
        self.setinitial("dialect", dialect)
        self.setinitial("schema", schema)
        self.__basepath = basepath
        if basepath is None:
            self.__basepath = helpers.detect_basepath(descriptor)
        super().__init__(descriptor)

    @cached_property
    def path(self):
        return self.get("path")

    @cached_property
    def data(self):
        data = self.get("data")
        return self.metadata_attach("data", data)

    @cached_property
    def source(self):
        path = self.path
        if path and not helpers.is_safe_path(path):
            note = f'path "{path}" is not safe'
            raise exceptions.FrictionlessException(errors.ResourceError(note=note))
        if path and not helpers.is_remote_path(path):
            path = os.path.join(self.basepath, path)
        source = self.data if self.inline else path
        return source or []

    @cached_property
    def basepath(self):
        return self.__basepath

    @cached_property
    def profile(self):
        return self.get("profile")

    @cached_property
    def inline(self):
        return "data" in self

    @cached_property
    def tabular(self):
        try:
            self.table.open()
        except Exception as exception:
            if exception.error.code == "format-error":
                return False
        except Exception:
            pass
        return True

    @cached_property
    def multipart(self):
        # TODO: implement
        return False

    @cached_property
    def scheme(self):
        return self.get("scheme")

    @cached_property
    def format(self):
        return self.get("format")

    # TODO: infer from provided hash
    @cached_property
    def hashing(self):
        return self.get("hashing")

    @cached_property
    def encoding(self):
        return self.get("encoding")

    @cached_property
    def compression(self):
        return self.get("compression")

    @cached_property
    def compression_path(self):
        return self.get("compressionPath")

    @cached_property
    def dialect(self):
        dialect = self.get("dialect")
        if dialect is None:
            dialect = self.metadata_attach("dialect", Dialect())
        elif isinstance(dialect, str):
            if not helpers.is_safe_path(dialect):
                note = f'dialect path "{dialect}" is not safe'
                raise exceptions.FrictionlessException(errors.ResourceError(note=note))
            dialect = Dialect(os.path.join(self.basepath, dialect))
        return dialect

    @cached_property
    def schema(self):
        schema = self.get("schema")
        if schema is None:
            schema = self.metadata_attach("schema", Schema())
        elif isinstance(schema, str):
            if not helpers.is_safe_path(schema):
                note = f'schema path "{schema}" is not safe'
                raise exceptions.FrictionlessException(errors.ResourceError(note=note))
            schema = Schema(os.path.join(self.basepath, schema))
        return schema

    @cached_property
    def table(self):
        return Table(
            self.source,
            scheme=self.scheme,
            format=self.format,
            hashing=self.hashing,
            encoding=self.encoding,
            compression=self.compression,
            compression_path=self.compression_path,
            dialect=self.dialect,
            schema=self.schema,
        )

    @cached_property
    def __file(self):
        return File(
            source=self.source,
            scheme=self.scheme,
            format=self.format,
            hashing=self.hashing,
            encoding=self.encoding,
            compression=self.compression,
            compression_path=self.compression_path,
            stats={"hash": "", "bytes": 0, "rows": 0},
        )

    # Expand

    def expand(self):
        self.setdefault("profile", config.DEFAULT_RESOURCE_PROFILE)
        if "dialect" in self:
            self.dialect.expand()
        if "schema" in self:
            self.schema.expand()

    # Infer

    def infer():
        pass

    # Read

    def read_bytes(self):
        byte_stream = self.read_byte_stream()
        bytes = byte_stream.read()
        return bytes

    def read_byte_stream(self):
        if self.inline:
            return io.BytesIO(b"")
        loader = system.create_loader(self.__file)
        loader.open()
        return loader.byte_stream

    def read_data(self):
        data = list(self.read_data_stream())
        return data

    def read_data_stream(self):
        with self.table as table:
            for cells in table.data_stream:
                yield cells

    def read_rows(self):
        rows = list(self.read_row_stream())
        return rows

    def read_row_stream(self):
        with self.table as table:
            for row in table.row_stream:
                yield row

    def read_headers(self):
        with self.table as table:
            return table.headers

    def read_sample(self):
        with self.table as table:
            return table.sample

    def read_stats(self):
        try:
            for cells in self.read_data_stream():
                pass
            return self.table.stats
        except Exception:
            bytes = True
            byte_stream = self.read_byte_stream()
            while bytes:
                bytes = byte_stream.read1(io.DEFAULT_BUFFER_SIZE)
            return self.__file.stats

    # Save

    # TODO: save metadata + data to zip
    def save(self, target):
        self.metadata_save(target)

    # Metadata

    def metadata_process(self):

        # Dialect
        dialect = self.get("dialect")
        if not isinstance(dialect, (str, type(None), Dialect)):
            # TODO: rebase on proper system.create_dialect
            Class = dialects.CsvDialect if "delimiter" in dialect else Dialect
            dialect = Class(dialect)
            dict.__setitem__(self, "dialect", dialect)

        # Schema
        schema = self.get("schema")
        if not isinstance(schema, (str, type(None), Schema)):
            schema = Schema(schema)
            dict.__setitem__(self, "schema", schema)


# Internal


class MultipartSource:
    def __init__(self, source, remote=False):
        self.__source = source
        self.__remote = remote
        self.__rows = self.__iter_rows()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __iter__(self):
        return self.__rows

    @property
    def closed(self):
        return False

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def close(self):
        pass

    def flush(self):
        pass

    def read1(self, size):
        return self.read(size)

    def seek(self, offset):
        assert offset == 0
        self.__rows = self.__iter_rows()

    def read(self, size):
        res = b""
        while True:
            try:
                res += next(self.__rows)
            except StopIteration:
                break
            if len(res) > size:
                break
        return res

    def __iter_rows(self):
        streams = []
        if self.__remote:
            streams = [urlopen(chunk) for chunk in self.__source]
        else:
            streams = [io.open(chunk, "rb") for chunk in self.__source]
        for stream in streams:
            for row in stream:
                if not row.endswith(b"\n"):
                    row += b"\n"
                yield row
