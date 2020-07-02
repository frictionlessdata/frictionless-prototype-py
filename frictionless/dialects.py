import csv
import stringcase
from .metadata import ControlledMetadata
from . import config


class Dialect(ControlledMetadata):
    """Dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        headers_row? (int|int[]): headers_row
        headers_joiner? (str): headers_joiner

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    # TODO: add not additionalProperties?
    metadata_profile = {  # type: ignore
        "type": "object",
        "properties": {
            "headersRow": {"type": ["number", "array", "null"]},
            "headersJoiner": {"type": "string"},
        },
    }

    def __init__(
        self, descriptor=None, headers_row=None, headers_joiner=None, metadata_root=None
    ):
        self.setdefined("headersRow", headers_row)
        self.setdefined("headersJoiner", headers_joiner)
        super().__init__(descriptor, metadata_root=metadata_root)

    def __setattr__(self, name, value):
        if name in ["headers_row", "headers_joiner"]:
            self[stringcase.camelcase(name)] = value
        else:
            super().__setattr__(name, value)

    @property
    def headers_row(self):
        headers_row = self.get("headersRow", config.DEFAULT_HEADERS_ROW)
        if isinstance(headers_row, int):
            headers_row = [headers_row]
        if headers_row is not None:
            self.metadata_attach("headersRow", headers_row)
        return headers_row

    @property
    def headers_joiner(self):
        return self.get("headersJoiner", config.DEFAULT_HEADERS_JOINER)

    # Expand

    def expand(self):
        self.setdetault("headersRow", self.headers_row)
        self.setdetault("headersJoiner", self.headers_joiner)


class CsvDialect(Dialect):
    """Csv dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        delimiter? (str): delimiter
        line_terminator? (str): line_terminator
        quote_char? (str): quote_char
        double_quote? (bool): double_quote
        escape_char? (str): escape_char
        null_sequence? (str): null_sequence
        skip_initial_space? (bool): skip_initial_space
        header? (bool): header
        comment_char? (str): comment_char
        case_sensitive_header? (bool): case_sensitive_header

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "delimiter": {"type": "string"},
            "lineTerminator": {"type": "string"},
            "quoteChar": {"type": "string"},
            "doubleQuote": {"type": "boolean"},
            "escapeChar": {"type": "string"},
            "nullSequence": {"type": "string"},
            "skipInitialSpace": {"type": "boolean"},
            "header": {"type": "boolean"},
            "commentChar": {"type": "string"},
            "caseSensitiveHeader": {"type": "boolean"},
            "headersRow": {"type": ["number", "array", "null"]},
            "headersJoiner": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        delimiter=None,
        line_terminator=None,
        quote_char=None,
        double_quote=None,
        escape_char=None,
        null_sequence=None,
        skip_initial_space=None,
        header=None,
        comment_char=None,
        case_sensitive_header=None,
        headers_row=None,
        headers_joiner=None,
        metadata_root=None,
    ):
        self.setdefined("delimiter", delimiter)
        self.setdefined("lineTerminator", line_terminator)
        self.setdefined("quoteChar", quote_char)
        self.setdefined("doubleQuote", double_quote)
        self.setdefined("escapeChar", escape_char)
        self.setdefined("nullSequence", null_sequence)
        self.setdefined("skipInitialSpace", skip_initial_space)
        self.setdefined("header", header)
        self.setdefined("commentChar", comment_char)
        self.setdefined("caseSensitiveHeader", case_sensitive_header)
        super().__init__(
            descriptor,
            headers_row=headers_row,
            headers_joiner=headers_joiner,
            metadata_root=metadata_root,
        )

    # TODO: Find a better way like to_native/from_native
    def __getattr__(self, name):
        # Interoperability with native
        if name == "lineterminator":
            return self.line_terminator
        if name == "doublequote":
            return self.double_quote
        elif name == "escapechar":
            return self.escape_char
        elif name == "quotechar":
            return self.quote_char
        elif name == "quoting":
            return csv.QUOTE_NONE if self.quote_char == "" else csv.QUOTE_MINIMAL
        elif name == "skipinitialspace":
            return self.skip_initial_space
        return super().__getattr__(name)

    @property
    def delimiter(self):
        return self.get("delimiter", ",")

    @property
    def line_terminator(self):
        return self.get("lineTerminator", "\r\n")

    @property
    def quote_char(self):
        return self.get("quoteChar", '"')

    @property
    def double_quote(self):
        return self.get("doubleQuote", True)

    @property
    def escape_char(self):
        return self.get("escapeChar")

    @property
    def null_sequence(self):
        return self.get("nullSequence")

    @property
    def skip_initial_space(self):
        return self.get("skipInitialSpace", True)

    @property
    def header(self):
        return self.get("header", True)

    @property
    def comment_char(self):
        return self.get("commentChar")

    @property
    def case_sensitive_header(self):
        return self.get("caseSensitiveHeader", False)

    # Expand

    def expand(self):
        super().expand()
        self.setdetault("delimiter", self.delimiter)
        self.setdetault("lineTerminator", self.line_terminator)
        self.setdetault("quoteChar", self.quote_char)
        self.setdetault("doubleQuote", self.double_quote)
        self.setdetault("skipInitialSpace", self.skip_initial_space)
        self.setdetault("header", self.header)
        self.setdetault("caseSensitiveHeader", self.case_sensitive_header)


class ExcelDialect(Dialect):
    """Excel dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        sheet? (int|str): sheet
        workbook_cache? (dict): workbook_cache
        fill_merged_cells? (bool): fill_merged_cells
        preserve_formatting? (bool): preserve_formatting
        adjust_floating_point_error? (bool): adjust_floating_point_error

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "sheet": {"type": ["number", "string"]},
            "workbookCache": {"type": "object"},
            "fillMergedCells": {"type": "boolean"},
            "preserveFormatting": {"type": "boolean"},
            "adjustFloatingPointError": {"type": "boolean"},
            "headersRow": {"type": ["number", "array", "null"]},
            "headersJoiner": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        sheet=None,
        workbook_cache=None,
        fill_merged_cells=None,
        preserve_formatting=None,
        adjust_floating_point_error=None,
        headers_row=None,
        headers_joiner=None,
        metadata_root=None,
    ):
        self.setdefined("sheet", sheet)
        self.setdefined("workbookCache", workbook_cache)
        self.setdefined("fillMergedCells", fill_merged_cells)
        self.setdefined("preserveFormatting", preserve_formatting)
        self.setdefined("adjustFloatingPointError", adjust_floating_point_error)
        super().__init__(
            descriptor,
            headers_row=headers_row,
            headers_joiner=headers_joiner,
            metadata_root=metadata_root,
        )

    @property
    def sheet(self):
        return self.get("sheet", 1)

    @property
    def workbook_cache(self):
        return self.get("workbookCache")

    @property
    def fill_merged_cells(self):
        return self.get("fillMergedCells", False)

    @property
    def preserve_formatting(self):
        return self.get("preserveFormatting", False)

    @property
    def adjust_floating_point_error(self):
        return self.get("adjustFloatingPointError", False)

    # Expand

    def expand(self):
        super().expand()
        self.setdetault("sheet", self.sheet)
        self.setdetault("fillMergedCells", self.fill_merged_cells)
        self.setdetault("preserveFormatting", self.preserve_formatting)
        self.setdetault("adjustFloatingPointError", self.adjust_floating_point_error)

    # Metadata

    # TODO: remove
    def metadata_process(self):
        super().metadata_process(skip=["workbookCache"])


# TODO: Consider headers prop for keyed sources
class InlineDialect(Dialect):
    """Inline dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        keyed? (bool): keyes
        forced? (bool): forced

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "keyed": {"type": "boolean"},
            "headersRow": {"type": ["number", "array", "null"]},
            "headersJoiner": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        keyed=None,
        headers_row=None,
        headers_joiner=None,
        metadata_root=None,
    ):
        self.setdefined("keyed", keyed)
        super().__init__(
            descriptor,
            headers_row=headers_row,
            headers_joiner=headers_joiner,
            metadata_root=metadata_root,
        )

    @property
    def keyed(self):
        return self.get("keyed", False)

    # Expand

    def expand(self):
        self.setdetault("keyed", self.keyed)


# TODO: Consider headers prop for keyed sources
class JsonDialect(Dialect):
    """Json dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        keyed? (bool): keyes
        property? (str): property

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "keyed": {"type": "boolean"},
            "property": {"type": "string"},
            "headersRow": {"type": ["number", "array", "null"]},
            "headersJoiner": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        keyed=None,
        property=None,
        headers_row=None,
        headers_joiner=None,
        metadata_root=None,
    ):
        self.setdefined("keyed", keyed)
        self.setdefined("property", property)
        super().__init__(
            descriptor,
            headers_row=headers_row,
            headers_joiner=headers_joiner,
            metadata_root=metadata_root,
        )

    @property
    def keyed(self):
        return self.get("keyed", False)

    @property
    def property(self):
        return self.get("property")

    # Expand

    def expand(self):
        super().expand()
        self.setdetault("keyed", self.keyed)
