import re
from pathlib import Path
from itertools import chain
from copy import copy, deepcopy
from collections import deque
from .headers import Headers
from .schema import Schema
from .system import system
from .file import File
from .row import Row
from . import exceptions
from . import errors
from . import helpers
from . import config


class Table:
    """Table representation

    This is the main `tabulator` class. It loads a data source, and allows you
    to stream its parsed contents.

    # Arguments

        source (str):
            Path to file as ``<scheme>\\://path/to/file.<format>``.
            If not explicitly set, the scheme (file, http, ...) and
            format (csv, xls, ...) are inferred from the source string.

        headers (Union[int, List[int], List[str]], optional):
            Either a row
            number or list of row numbers (in case of multi-line headers) to be
            considered as headers (rows start counting at 1), or the actual
            headers defined a list of strings. If not set, all rows will be
            treated as containing values.

        scheme (str, optional):
            Scheme for loading the file (file, http, ...).
            If not set, it'll be inferred from `source`.

        format (str, optional):
            File source's format (csv, xls, ...). If not
            set, it'll be inferred from `source`. inferred

        encoding (str, optional):
            Source encoding. If not set, it'll be inferred.

        compression (str, optional):
            Source file compression (zip, ...). If not set, it'll be inferred.

        pick_rows (List[Union[int, str, dict]], optional):
            The same as `skip_rows` but it's for picking rows instead of skipping.

        skip_rows (List[Union[int, str, dict]], optional):
            List of row numbers, strings and regex patterns as dicts to skip.
            If a string, it'll skip rows that their first cells begin with it e.g. '#' and '//'.
            To skip only completely blank rows use `{'type'\\: 'preset', 'value'\\: 'blank'}`
            To provide a regex pattern use  `{'type'\\: 'regex', 'value'\\: '^#'}`
            For example\\: `skip_rows=[1, '# comment', {'type'\\: 'regex', 'value'\\: '^# (regex|comment)'}]`

        pick_fields (List[Union[int, str]], optional):
            When passed, ignores all columns with headers
            that the given list DOES NOT include

        skip_fields (List[Union[int, str]], optional):
            When passed, ignores all columns with headers
            that the given list includes. If it contains an empty string it will skip
            empty headers

        sample_size (int, optional):
            Controls the number of sample rows used to
            infer properties from the data (headers, encoding, etc.). Set to
            ``0`` to disable sampling, in which case nothing will be inferred
            from the data. Defaults to ``config.DEFAULT_SAMPLE_SIZE``.

        allow_html (bool, optional):
            Allow the file source to be an HTML page.
            If False, raises ``exceptions.FormatError`` if the loaded file is
            an HTML page. Defaults to False.

        multiline_headers_joiner (str, optional):
            When passed, it's used to join multiline headers
            as `<passed-value>.join(header1_1, header1_2)`
            Defaults to ' ' (space).

        multiline_headers_duplicates (bool, optional):
            By default tabulator will exclude a cell of a miltilne header from joining
            if it's exactly the same as the previous seen value in this field.
            Enabling this option will force duplicates inclusion
            Defaults to False.

        hashing_algorithm (func, optional):
            It supports: md5, sha1, sha256, sha512
            Defaults to sha256

        force_strings (bool, optional):
            When True, casts all data to strings.
            Defaults to False.

        post_parse (List[function], optional):
            List of generator functions that
            receives a list of rows and headers, processes them, and yields
            them (or not). Useful to pre-process the data. Defaults to None.

        custom_loaders (dict, optional):
            Dictionary with keys as scheme names,
            and values as their respective ``Loader`` class implementations.
            Defaults to None.

        custom_parsers (dict, optional):
            Dictionary with keys as format names,
            and values as their respective ``Parser`` class implementations.
            Defaults to None.

        custom_loaders (dict, optional):
            Dictionary with keys as writer format
            names, and values as their respective ``Writer`` class
            implementations. Defaults to None.

        **options (Any, optional): Extra options passed to the loaders and parsers.

    """

    # Public

    def __init__(
        self,
        source,
        *,
        # Table
        scheme=None,
        format=None,
        hashing=None,
        encoding=None,
        compression=None,
        compression_path=None,
        control=None,
        dialect=None,
        headers=config.DEFAULT_HEADERS_ROW,
        pick_fields=None,
        skip_fields=None,
        limit_fields=None,
        offset_fields=None,
        pick_rows=None,
        skip_rows=None,
        limit_rows=None,
        offset_rows=None,
        # Schema
        schema=None,
        sync_schema=False,
        patch_schema=False,
        infer_type=None,
        infer_names=None,
        infer_volume=config.DEFAULT_INFER_VOLUME,
        infer_confidence=config.DEFAULT_INFER_CONFIDENCE,
    ):

        # Update source
        # TODO: move to File
        if isinstance(source, Path):
            source = str(source)

        # Update dialect
        if headers != config.DEFAULT_HEADERS_ROW:
            dialect = dialect or {}
            if isinstance(headers, (type(None), int)):
                dialect["headersRow"] = headers
            elif isinstance(headers[0], int):
                dialect["headersRow"] = headers
            elif isinstance(headers[1], str):
                dialect["headersRow"] = headers[0]
                dialect["headersJoiner"] = headers[1]

        # Store state
        self.__parser = None
        self.__sample = None
        self.__headers = None
        self.__row_stream = None
        self.__data_stream = None
        self.__field_positions = None
        self.__sample_positions = None
        self.__pick_fields = pick_fields
        self.__skip_fields = skip_fields
        self.__limit_fields = limit_fields
        self.__offset_fields = offset_fields
        self.__pick_rows = pick_rows
        self.__skip_rows = skip_rows
        self.__limit_rows = limit_rows
        self.__offset_rows = offset_rows
        self.__schema = schema
        self.__sync_schema = sync_schema
        self.__patch_schema = patch_schema
        self.__infer_type = infer_type
        self.__infer_names = infer_names
        self.__infer_volume = infer_volume
        self.__infer_confidence = infer_confidence

        # Create file
        self.__file = File(
            source=source,
            scheme=scheme,
            format=format,
            hashing=hashing,
            encoding=encoding,
            compression=compression,
            compression_path=compression_path,
            control=control,
            dialect=dialect,
        )

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        if not self.__row_stream:
            note = 'Table is closed. Please call "table.open()" first.'
            raise exceptions.FrictionlessException(errors.Error(note=note))
        return iter(self.__row_stream)

    @property
    def path(self):
        """Path

        # Returns
            any: stream path

        """
        return self.__file.path

    @property
    def source(self):
        """Source

        # Returns
            any: stream source

        """
        return self.__file.source

    @property
    def scheme(self):
        """Path's scheme

        # Returns
            str: scheme

        """
        return self.__file.scheme

    @property
    def format(self):
        """Path's format

        # Returns
            str: format

        """
        return self.__file.format

    @property
    def hashing(self):
        """Stream's encoding

        # Returns
            str: encoding

        """
        return self.__file.hashing

    @property
    def encoding(self):
        """Stream's encoding

        # Returns
            str: encoding

        """
        return self.__file.encoding

    @property
    def compression(self):
        """Stream's compression ("no" if no compression)

        # Returns
            str: compression

        """
        return self.__file.compression

    @property
    def compression_path(self):
        """Stream's compression path

        # Returns
            str: compression

        """
        return self.__file.compression_path

    @property
    def control(self):
        """Control (if available)

        # Returns
            dict/None: dialect

        """
        return self.__file.control

    @property
    def dialect(self):
        """Dialect (if available)

        # Returns
            dict/None: dialect

        """
        return self.__file.dialect

    @property
    def newline(self):
        """Newline (if available)

        # Returns
            dict/None: newline

        """
        return self.__file.newline

    @property
    def stats(self):
        """Returns stats

        # Returns
            int/None: BYTE count

        """
        return self.__file.stats

    @property
    def headers(self):
        """Headers

        # Returns
            str[]/None: headers if available

        """
        return self.__headers

    @property
    def sample(self):
        """Returns the stream's rows used as sample.

        These sample rows are used internally to infer characteristics of the
        source file (e.g. encoding, headers, ...).

        # Returns
            list[]: sample

        """
        return self.__sample

    @property
    def schema(self):
        """Schema

        # Returns
            str[]/None: schema

        """
        return self.__schema

    @property
    def data_stream(self):
        """Data stream

        # Returns
            str[]/None: data_stream

        """
        return self.__data_stream

    @property
    def row_stream(self):
        """Row stream

        # Returns
            str[]/None: row_stream

        """
        return self.__row_stream

    # Manage

    def open(self):
        """Opens the stream for reading.

        # Raises:
            TabulatorException: if an error

        """
        self.close()
        self.__file.stats = {"hash": "", "bytes": 0}
        self.__parser = system.create_parser(self.__file)
        self.__parser.open()
        self.__data_stream = self.__read_data_stream()
        self.__row_stream = self.__read_row_stream()
        return self

    def close(self):
        """Closes the stream.
        """
        if self.__parser:
            self.__parser.close()
            self.__parser = None

    # Read

    def read_data(self):
        if not self.__data_stream:
            note = 'Table is closed. Please call "table.open()" first.'
            raise exceptions.FrictionlessException(errors.Error(note=note))
        return list(self.__data_stream)

    def __read_data_stream(self):
        self.__read_data_stream_infer()
        return self.__read_data_stream_create()

    def __read_data_stream_create(self):
        yield from self.__sample
        for cells in self.__parser.data_stream:
            # TODO: filter rows
            yield cells

    def __read_data_stream_infer(self):
        dialect = self.__file.dialect

        # Prepare state
        sample = []
        headers = None
        field_positions = []
        sample_positions = []
        schema = Schema(self.__schema)

        # Infer table
        headers_ready = False
        headers_memory = {}
        for row_position, cells in enumerate(self.__parser.data_stream, start=1):
            # TODO: filter rows

            # Headers
            # TODO: move to infer_headers
            if not headers_ready:
                if not dialect.headers_row:
                    headers_ready = True
                    headers = None
                elif isinstance(dialect.headers_row, int):
                    if dialect.headers_row == row_position:
                        headers_ready = True
                        headers = []
                        for cell in cells:
                            cell = str(cell).strip() if cell is not None else ""
                            headers.append(cell)
                elif isinstance(dialect.headers_row, (tuple, list)):
                    if row_position in dialect.headers_row:
                        headers = headers or []
                        for index, cell in enumerate(cells):
                            cell = str(cell).strip() if cell is not None else ""
                            joiner = dialect.headers_joiner
                            if headers_memory.get(index) != cell:
                                try:
                                    headers[index] = joiner.join([headers[index], cell])
                                except IndexError:
                                    headers.append(cell)
                            headers_memory[index] = cell
                        if row_position == max(dialect.headers_row):
                            headers_ready = True
                if headers_ready:
                    infer = self.__read_data_stream_infer_field_positions
                    field_positions = infer(headers, cells=cells)
                if not headers_ready or headers is not None:
                    continue

            # Sample
            sample.append(cells)
            sample_positions.append(row_position)
            if len(sample) >= self.__infer_volume:
                break

        # Infer schema
        if not schema.fields:
            schema.infer(
                sample,
                type=self.__infer_type,
                names=self.__infer_names or headers,
                confidence=self.__infer_confidence,
            )

        # Sync schema
        if self.__sync_schema:
            fields = []
            mapping = {field.get("name"): field for field in schema.fields}
            for name in headers:
                fields.append(mapping.get(name, {"name": name, "type": "any"}))
            schema.fields = fields

        # Patch schema
        if self.__patch_schema:
            patch_schema = deepcopy(self.__patch_schema)
            fields = patch_schema.pop("fields", {})
            schema.update(patch_schema)
            for field in schema.fields:
                field.update((fields.get(field.get("name"), {})))

        # Confirm schema
        if len(schema.field_names) != len(set(schema.field_names)):
            note = "Schemas with duplicate field names are not supported"
            raise exceptions.FrictionlessException(errors.SchemaError(note=note))

        # Store state
        self.__sample = sample
        self.__schema = schema
        self.__field_positions = field_positions
        self.__sample_positions = sample_positions
        self.__headers = (
            Headers(headers, fields=schema.fields, field_positions=field_positions)
            if headers is not None
            else None
        )

    def __read_data_stream_infer_field_positions(self, headers, *, cells):
        # TODO: Filter headers in-place
        # TODO: apply pick/skip/limit/offset
        field_positions = list(range(1, len(cells) + 1))
        return field_positions

    def read_rows(self):
        if not self.__row_stream:
            note = 'Table is closed. Please call "table.open()" first.'
            raise exceptions.FrictionlessException(errors.Error(note=note))
        return list(self.__row_stream)

    def __read_row_stream(self):
        init = zip(self.__sample_positions, self.__sample)
        rest = enumerate(self.__parser.data_stream)
        for row_number, (row_position, cells) in enumerate(chain(init, rest), start=1):
            # TODO: filter rows or reabase on self.__data_stream
            yield Row(
                cells,
                fields=self.__schema.fields,
                field_positions=self.__field_positions,
                row_position=row_position,
                row_number=row_number,
            )
