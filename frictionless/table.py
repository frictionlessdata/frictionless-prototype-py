import typing
from pathlib import Path
from copy import deepcopy
from itertools import chain
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
        headers=None,
        # File
        scheme=None,
        format=None,
        hashing=None,
        encoding=None,
        compression=None,
        compression_path=None,
        control=None,
        dialect=None,
        query=None,
        # Schema
        schema=None,
        sync_schema=False,
        patch_schema=False,
        infer_type=None,
        infer_names=None,
        infer_volume=config.DEFAULT_INFER_VOLUME,
        infer_confidence=config.DEFAULT_INFER_CONFIDENCE,
        infer_missing_values=config.DEFAULT_MISSING_VALUES,
        lookup=None,
    ):

        # Update source
        if isinstance(source, Path):
            source = str(source)

        # Update dialect
        if headers is not None:
            dialect = (dialect or {}).copy()
            if not headers:
                dialect["header"] = False
            elif isinstance(headers, int):
                dialect["headerRows"] = [headers]
            elif isinstance(headers, list):
                dialect["headerRows"] = headers
                if isinstance(headers[0], list):
                    dialect["headerRows"] = headers[0]
                    dialect["headerJoin"] = headers[1]

        # Store state
        self.__parser = None
        self.__sample = None
        self.__schema = None
        self.__headers = None
        self.__data_stream = None
        self.__row_stream = None
        self.__row_number = None
        self.__row_position = None
        self.__field_positions = None
        self.__sample_positions = None

        # Store params
        self.__init_schema = schema
        self.__sync_schema = sync_schema
        self.__patch_schema = patch_schema
        self.__infer_type = infer_type
        self.__infer_names = infer_names
        self.__infer_volume = infer_volume
        self.__infer_confidence = infer_confidence
        self.__infer_missing_values = infer_missing_values
        self.__lookup = lookup

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
            query=query,
        )

    def __enter__(self):
        if self.closed:
            self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        self.__read_row_stream_raise_closed()
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
        """Dialect

        # Returns
            dict/None: dialect

        """
        return self.__file.dialect

    @property
    def query(self):
        """Query

        # Returns
            dict/None: query

        """
        return self.__file.query

    @property
    def schema(self):
        """Schema

        # Returns
            str[]/None: schema

        """
        return self.__schema

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

    @property
    def stats(self):
        """Returns stats

        # Returns
            int/None: BYTE count

        """
        return self.__file.stats

    # Open/Close

    def open(self):
        """Opens the stream for reading.

        # Raises:
            TabulatorException: if an error

        """
        self.close()
        if self.__file.query.metadata_errors:
            error = self.__file.query.metadata_errors[0]
            raise exceptions.FrictionlessException(error)
        try:
            self.__file.stats = {"hash": "", "bytes": 0, "rows": 0}
            self.__parser = system.create_parser(self.__file)
            self.__parser.open()
            self.__data_stream = self.__read_data_stream()
            self.__row_stream = self.__read_row_stream()
            self.__row_number = 0
            self.__row_position = 0
            return self
        except exceptions.FrictionlessException as exception:
            self.close()
            # Ensure not found file is a scheme error
            if exception.error.code == "format-error":
                loader = system.create_loader(self.__file)
                loader.open()
                loader.close()
            raise
        except Exception:
            self.close()
            raise

    def close(self):
        """Closes the stream.
        """
        if self.__parser:
            self.__parser.close()
            self.__parser = None

    @property
    def closed(self):
        return self.__parser is None

    # Read

    def read_data(self):
        self.__read_data_stream_raise_closed()
        return list(self.__data_stream)

    def __read_data_stream(self):
        self.__read_data_stream_infer()
        return self.__read_data_stream_create()

    def __read_data_stream_create(self):
        stats = self.__file.stats
        limit = self.__file.query.limit_rows
        offset = self.__file.query.offset_rows or 0
        sample_iterator = self.__read_data_stream_create_sample_iterator()
        parser_iterator = self.__read_data_stream_create_parser_iterator()
        for row_position, cells in chain(sample_iterator, parser_iterator):
            self.__row_position = row_position
            if offset:
                offset -= 1
                continue
            self.__row_number += 1
            stats["rows"] = self.__row_number
            yield cells
            if limit and limit <= stats["rows"]:
                break

    def __read_data_stream_create_sample_iterator(self):
        return zip(self.__sample_positions, self.__sample)

    def __read_data_stream_create_parser_iterator(self):
        start = max(self.__sample_positions or [0]) + 1
        iterator = enumerate(self.__parser.data_stream, start=start)
        for row_position, cells in iterator:
            if self.__read_data_stream_pick_skip_row(row_position, cells):
                cells = self.__read_data_stream_filter_data(cells, self.__field_positions)
                yield row_position, cells

    def __read_data_stream_infer(self):

        # Create state
        sample = []
        headers = []
        field_positions = []
        sample_positions = []
        schema = Schema(self.__init_schema)

        # Prepare header
        buffer = []
        widths = []
        for row_position, cells in enumerate(self.__parser.data_stream, start=1):
            buffer.append(cells)
            if self.__read_data_stream_pick_skip_row(row_position, cells):
                widths.append(len(cells))
                if len(widths) >= self.__infer_volume:
                    break

        # Infer header
        row_number = 0
        dialect = self.__file.dialect
        if dialect.get("header") is None and dialect.get("headerRows") is None and widths:
            dialect["header"] = False
            width = round(sum(widths) / len(widths))
            drift = max(round(width * 0.1), 1)
            match = list(range(width - drift, width + drift + 1))
            for row_position, cells in enumerate(buffer, start=1):
                if self.__read_data_stream_pick_skip_row(row_position, cells):
                    row_number += 1
                    if len(cells) not in match:
                        continue
                    if not helpers.is_only_strings(cells):
                        continue
                    del dialect["header"]
                    if row_number != config.DEFAULT_HEADER_ROWS[0]:
                        dialect["headerRows"] = [row_number]
                    break

        # Infer table
        row_number = 0
        headers_data = []
        headers_ready = False
        headers_numbers = dialect.header_rows or config.DEFAULT_HEADER_ROWS
        iterator = chain(buffer, self.__parser.data_stream)
        for row_position, cells in enumerate(iterator, start=1):
            if self.__read_data_stream_pick_skip_row(row_position, cells):
                row_number += 1

                # Headers
                if not headers_ready:
                    if row_number in headers_numbers:
                        headers_data.append(helpers.stringify_headers(cells))
                    if row_number >= max(headers_numbers):
                        infer = self.__read_data_stream_infer_headers
                        headers, field_positions = infer(headers_data)
                        headers_ready = True
                    if not headers_ready or dialect.header:
                        continue

                # Sample
                sample.append(self.__read_data_stream_filter_data(cells, field_positions))
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
                missing_values=self.__infer_missing_values,
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
        self.__headers = Headers(headers, schema=schema, field_positions=field_positions)

    def __read_data_stream_infer_headers(self, headers_data):
        dialect = self.__file.dialect

        # No headers
        if not dialect.header:
            return [], list(range(1, len(headers_data[0]) + 1))

        # Get headers
        headers = []
        prev_cells = {}
        for cells in headers_data:
            for index, cell in enumerate(cells):
                if prev_cells.get(index) == cell:
                    continue
                prev_cells[index] = cell
                if len(headers) <= index:
                    headers.append(cell)
                    continue
                headers[index] = dialect.header_join.join([headers[index], cell])

        # Filter headers
        filter_headers = []
        field_positions = []
        limit = self.__file.query.limit_fields
        offset = self.__file.query.offset_fields or 0
        for field_position, header in enumerate(headers, start=1):
            if self.__read_data_stream_pick_skip_field(field_position, header):
                if offset:
                    offset -= 1
                    continue
                filter_headers.append(header)
                field_positions.append(field_position)
                if limit and limit <= len(filter_headers):
                    break

        return filter_headers, field_positions

    def __read_data_stream_pick_skip_field(self, field_position, header):
        match = True
        for name in ["pick", "skip"]:
            if name == "pick":
                items = self.__file.query.pick_fields_compiled
            else:
                items = self.__file.query.skip_fields_compiled
            if not items:
                continue
            match = match and name == "skip"
            for item in items:
                if item == "<blank>" and header == "":
                    match = not match
                elif isinstance(item, str) and item == header:
                    match = not match
                elif isinstance(item, int) and item == field_position:
                    match = not match
                elif isinstance(item, typing.Pattern) and item.match(header):
                    match = not match
        return match

    def __read_data_stream_pick_skip_row(self, row_position, cells):
        match = True
        cell = cells[0] if cells else None
        cell = "" if cell is None else str(cell)
        for name in ["pick", "skip"]:
            if name == "pick":
                items = self.__file.query.pick_rows_compiled
            else:
                items = self.__file.query.skip_rows_compiled
            if not items:
                continue
            match = match and name == "skip"
            for item in items:
                if item == "<blank>":
                    if not any(cell for cell in cells if cell not in ["", None]):
                        match = not match
                elif isinstance(item, str):
                    if item == cell or (item and cell.startswith(item)):
                        match = not match
                elif isinstance(item, int) and item == row_position:
                    match = not match
                elif isinstance(item, typing.Pattern) and item.match(cell):
                    match = not match
        return match

    def __read_data_stream_filter_data(self, cells, field_positions):
        if self.__file.query.is_field_filtering:
            result = []
            for field_position, cell in enumerate(cells, start=1):
                if field_position in field_positions:
                    result.append(cell)
            return result
        return cells

    def __read_data_stream_raise_closed(self):
        if not self.__data_stream:
            note = 'the table has not been opened by "table.open()"'
            raise exceptions.FrictionlessException(errors.Error(note=note))

    def read_rows(self):
        self.__read_row_stream_raise_closed()
        return list(self.__row_stream)

    def __read_row_stream(self):
        return self.__read_row_stream_create()

    def __read_row_stream_create(self):
        schema = self.schema

        # Create state
        memory_unique = {}
        memory_primary = {}
        foreign_groups = []
        for field in self.schema.fields:
            if field.constraints.get("unique"):
                memory_unique[field.name] = {}
        if self.__lookup:
            for fk in self.schema.foreign_keys:
                group = {}
                group["sourceName"] = fk["reference"]["resource"]
                group["sourceKey"] = tuple(fk["reference"]["fields"])
                group["targetKey"] = tuple(fk["fields"])
                foreign_groups.append(group)

        # Stream rows
        for cells in self.__data_stream:

            # Create row
            row = Row(
                cells,
                schema=self.__schema,
                field_positions=self.__field_positions,
                row_position=self.__row_position,
                row_number=self.__file.stats["rows"],
            )

            # Unique Error
            if memory_unique:
                for field_name in memory_unique.keys():
                    cell = row[field_name]
                    if cell is not None:
                        match = memory_unique[field_name].get(cell)
                        memory_unique[field_name][cell] = row.row_position
                        if match:
                            Error = errors.UniqueError
                            note = "the same as in the row at position %s" % match
                            error = Error.from_row(row, note=note, field_name=field_name)
                            row.errors.append(error)

            # Primary Key Error
            if schema.primary_key:
                cells = tuple(row[field_name] for field_name in schema.primary_key)
                if set(cells) == {None}:
                    note = 'cells composing the primary keys are all "None"'
                    error = errors.PrimaryKeyError.from_row(row, note=note)
                    row.errors.append(error)
                else:
                    match = memory_primary.get(cells)
                    memory_primary[cells] = row.row_position
                    if match:
                        if match:
                            note = "the same as in the row at position %s" % match
                            error = errors.PrimaryKeyError.from_row(row, note=note)
                            row.errors.append(error)

            # Foreign Key Error
            if foreign_groups:
                for group in foreign_groups:
                    group_lookup = self.__lookup.get(group["sourceName"])
                    if group_lookup:
                        cells = tuple(row[name] for name in group["targetKey"])
                        if set(cells) == {None}:
                            continue
                        match = cells in group_lookup.get(group["sourceKey"], set())
                        if not match:
                            note = "not found in the lookup table"
                            error = errors.ForeignKeyError.from_row(row, note=note)
                            row.errors.append(error)

            # Stream row
            yield row

    def __read_row_stream_raise_closed(self):
        if not self.__row_stream:
            note = 'the table has not been opened by "table.open()"'
            raise exceptions.FrictionlessException(errors.Error(note=note))

    # Write

    # NOTE: implement proper usage of loaders (e.g. write to s3)
    # NOTE: allow None target and return result for inline/pandas/etc?
    def write(
        self,
        target,
        *,
        scheme=None,
        format=None,
        hashing=None,
        encoding=None,
        compression=None,
        compression_path=None,
        control=None,
        dialect=None,
    ):

        # Create file
        file = File(
            source=target,
            scheme=scheme,
            format=format,
            hashing=hashing,
            encoding=encoding,
            compression=compression,
            compression_path=compression_path,
            control=control,
            dialect=dialect,
        )

        # Write file
        row_stream = self.__write_row_stream_create()
        parser = system.create_parser(file)
        parser.write(row_stream)

    def __write_row_stream_create(self):
        self.__read_data_stream_raise_closed()
        yield from self.row_stream
