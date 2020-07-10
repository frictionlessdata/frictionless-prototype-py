import io
import os
import json
import zipfile
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
    metadata_Error = errors.ResourceError
    metadata_duplicate = True
    metadata_setters = {
        "name": "name",
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
        "profile": "profile",
    }

    def __init__(
        self,
        descriptor=None,
        *,
        name=None,
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
        profile=None,
        basepath=None,
        trusted=False,
        package=None,
    ):

        # Handle zip
        if helpers.is_zip_descriptor(descriptor):
            descriptor = helpers.unzip_descriptor(descriptor, "dataresource.json")

        # Set attributes
        self.setinitial("name", name)
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
        self.setinitial("profile", profile)
        self.__basepath = basepath or helpers.detect_basepath(descriptor)
        self.__trusted = trusted
        self.__package = package
        super().__init__(descriptor)

        # Set hashing
        hashing, hash = helpers.parse_resource_hash(self.get("hash"))
        if hashing != config.DEFAULT_HASHING:
            self["hashing"] = hashing

    @cached_property
    def name(self):
        return self.get("name", "resource")

    # TODO: should it be memory for inline?
    @cached_property
    def path(self):
        return self.get("path")

    @cached_property
    def data(self):
        data = self.get("data")
        return self.metadata_attach("data", data)

    # TODO: rewrite this method
    @cached_property
    def source(self):
        path = self.path
        if self.inline:
            return self.data
        if not path:
            return []
        for path_item in path if isinstance(path, list) else [path]:
            if not self.__trusted and not helpers.is_safe_path(path_item):
                note = f'path "{path_item}" is not safe'
                raise exceptions.FrictionlessException(errors.ResourceError(note=note))
        if self.multipart:
            drop_headers = False
            if path[0].endswith(".csv"):
                dialect = dialects.CsvDialect(self.get("dialect"))
                if dialect.header:
                    drop_headers = True
            for index, path_item in enumerate(path):
                if not helpers.is_remote_path(path_item):
                    path[index] = os.path.join(self.basepath, path_item)
            return MultipartSource(path, drop_headers=drop_headers)
        if not helpers.is_remote_path(path):
            return os.path.join(self.basepath, path)
        return path

    @cached_property
    def basepath(self):
        return self.__basepath

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
        finally:
            self.table.close()
        return True

    @cached_property
    def network(self):
        if self.inline:
            return False
        return helpers.is_remote_path(self.path[0] if self.multipart else self.path)

    @cached_property
    def multipart(self):
        return bool(self.path and isinstance(self.path, list) and len(self.path) >= 2)

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
    def stats(self):
        stats = {}
        for name in ["hash", "bytes", "rows"]:
            value = self.get(name)
            if value is not None:
                if name == "hash":
                    value = helpers.parse_resource_hash(value)[1]
                stats[name] = value
        return stats

    @cached_property
    def dialect(self):
        dialect = self.get("dialect")
        if dialect is None:
            # TODO: metadata_attach re-create it as a ControlledDict
            dialect = self.metadata_attach("dialect", Dialect())
        elif isinstance(dialect, str):
            if not self.__trusted and not helpers.is_safe_path(dialect):
                note = f'dialect path "{dialect}" is not safe'
                raise exceptions.FrictionlessException(errors.ResourceError(note=note))
            dialect = Dialect(os.path.join(self.basepath, dialect))
        return dialect

    @cached_property
    def schema(self):
        schema = self.get("schema")
        if schema is None:
            # TODO: metadata_attach re-create it as a ControlledDict
            schema = self.metadata_attach("schema", Schema())
        elif isinstance(schema, str):
            if not self.__trusted and not helpers.is_safe_path(schema):
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

    # TODO: review the file/table situation
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
        if isinstance(self.get("dialect"), Dialect):
            self.dialect.expand()
        if isinstance(self.get("schema"), Schema):
            self.schema.expand()

    # Infer

    # TODO: optimize this logic/don't re-open
    def infer(self, path=None, *, only_sample=False):
        patch = {}

        # From path
        if path:
            self.path = path

        # Tabular
        if self.tabular:
            with self.table:
                patch["profile"] = "tabular-data-resource"
                patch["name"] = self.get("name", helpers.detect_name(self.table.path))
                patch["scheme"] = self.table.scheme
                patch["format"] = self.table.format
                patch["hashing"] = self.table.hashing
                patch["encoding"] = self.table.encoding
                patch["compression"] = self.table.compression
                patch["compressionPath"] = self.table.compression_path
                patch["dialect"] = self.table.dialect
                patch["schema"] = self.table.schema

        # General
        else:
            patch["profile"] = "data-resource"
            patch["name"] = self.get("name", helpers.detect_name(self.__file.path))
            patch["scheme"] = self.__file.scheme
            patch["format"] = self.__file.format
            patch["hashing"] = self.__file.hashing
            patch["encoding"] = self.__file.encoding
            patch["compression"] = self.__file.compression
            patch["compressionPath"] = self.__file.compression_path

        # Stats
        if not only_sample:
            stats = self.read_stats()
            patch.update(stats)
            if patch["hashing"] != config.DEFAULT_HASHING:
                patch["hash"] = ":".join([patch["hashing"], patch["hash"]])

        # Apply/expand
        self.update(patch)

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

    # TODO: move integrity to Table?
    def read_row_stream(self):
        lookup = self.read_lookup()
        check = system.create_check("integrity", descriptor={"lookup": lookup})
        with self.table as table:
            check.connect(table)
            check.prepare()
            for row in table.row_stream:
                row.errors.extend(check.validate_row(row))
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
            # TODO: make loader.ByteStreamWithStatsHandling iterable?
            bytes = True
            byte_stream = self.read_byte_stream()
            while bytes:
                bytes = byte_stream.read1(io.DEFAULT_BUFFER_SIZE)
            return self.__file.stats

    def read_lookup(self):
        lookup = {}
        # TODO: remove
        if not self.schema:
            return lookup
        for fk in self.schema.foreign_keys:
            source_name = fk["reference"]["resource"]
            source_key = tuple(fk["reference"]["fields"])
            source_res = self.__package.get_resource(source_name) if source_name else self
            if source_name != "" and not self.__package:
                continue
            lookup.setdefault(source_name, {})
            if source_key in lookup[source_name]:
                continue
            lookup[source_name][source_key] = set()
            if not source_res:
                continue
            with source_res.table as table:
                for row in table.row_stream:
                    cells = tuple(row.get(field_name) for field_name in source_key)
                    if set(cells) == {None}:
                        continue
                    lookup[source_name][source_key].add(cells)
        return lookup

    # Save

    # TODO: support multipart
    def save(self, target, *, only_descriptor=False):

        # Descriptor
        if only_descriptor:
            return self.metadata_save(target)

        # Resource
        try:
            with zipfile.ZipFile(target, "w") as zip:
                descriptor = self.copy()
                for resource in [self]:
                    if resource.inline:
                        continue
                    if resource.multipart:
                        continue
                    if resource.network:
                        continue
                    if not helpers.is_safe_path(resource.path):
                        continue
                    zip.write(resource.source, resource.path)
                descriptor = json.dumps(descriptor, indent=2, ensure_ascii=False)
                zip.writestr("dataresource.json", descriptor)
        except (IOError, zipfile.BadZipfile, zipfile.LargeZipFile) as exception:
            error = errors.ResourceError(note=str(exception))
            raise exceptions.FrictionlessException(error) from exception

    # Metadata

    @cached_property
    def profile(self):
        return self.get("profile", config.DEFAULT_RESOURCE_PROFILE)

    @cached_property
    def metadata_profile(self):
        if self.profile == "tabular-data-resource":
            return config.TABULAR_RESOURCE_PROFILE
        return config.RESOURCE_PROFILE

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
    def __init__(self, source, *, drop_headers):
        self.__source = source
        self.__drop_headers = drop_headers
        self.__line_stream = self.read_line_stream()

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
        self.__line_stream = self.read_line_stream()

    def read(self, size):
        res = b""
        while True:
            try:
                res += next(self.__line_stream)
            except StopIteration:
                break
            if len(res) > size:
                break
        return res

    def read_line_stream(self):
        streams = []
        if helpers.is_remote_path(self.__source[0]):
            streams = [urlopen(chunk) for chunk in self.__source]
        else:
            streams = [io.open(chunk, "rb") for chunk in self.__source]
        for stream_number, stream in enumerate(streams, start=1):
            for line_number, line in enumerate(stream, start=1):
                if not line.endswith(b"\n"):
                    line += b"\n"
                if self.__drop_headers and stream_number > 1 and line_number == 1:
                    continue
                yield line
