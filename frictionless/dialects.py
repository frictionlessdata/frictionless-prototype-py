import csv
from .metadata import Metadata
from . import errors
from . import config


class Dialect(Metadata):
    """Dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        headers? (int|list): headers

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_strict = True
    metadata_Error = errors.DialectError
    metadata_profile = {  # type: ignore
        "type": "object",
        "properties": {
            "headers": {
                "type": "object",
                "requried": ["rows"],
                "properties": {
                    "rows": {"type": "array", "items": {"type": "number"}},
                    "join": {"type": "string"},
                },
            }
        },
    }

    def __init__(self, descriptor=None, headers=None):
        self.setinitial("headers", headers)
        super().__init__(descriptor)

    @property
    def headers(self):
        headers = self.get("headers", {})
        headers.setdefault("rows", config.DEFAULT_HEADERS_ROWS)
        headers.setdefault("join", config.DEFAULT_HEADERS_JOIN)
        return self.metadata_attach("headers", headers)

    # Expand

    def expand(self):
        self.setdefault("headers", self.headers)


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
            "headers": {
                "type": "object",
                "requried": ["rows"],
                "properties": {
                    "rows": {"type": "array", "items": {"type": "number"}},
                    "join": {"type": "string"},
                },
            },
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
        headers=None,
    ):
        self.setinitial("delimiter", delimiter)
        self.setinitial("lineTerminator", line_terminator)
        self.setinitial("quoteChar", quote_char)
        self.setinitial("doubleQuote", double_quote)
        self.setinitial("escapeChar", escape_char)
        self.setinitial("nullSequence", null_sequence)
        self.setinitial("skipInitialSpace", skip_initial_space)
        self.setinitial("header", header)
        self.setinitial("commentChar", comment_char)
        self.setinitial("caseSensitiveHeader", case_sensitive_header)
        super().__init__(descriptor, headers=headers)

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
    def headers(self):
        if self.header is False:
            join = config.DEFAULT_HEADERS_JOIN
            return {"rows": [], "join": join}
        return super().headers

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
        self.setdefault("delimiter", self.delimiter)
        self.setdefault("lineTerminator", self.line_terminator)
        self.setdefault("quoteChar", self.quote_char)
        self.setdefault("doubleQuote", self.double_quote)
        self.setdefault("skipInitialSpace", self.skip_initial_space)
        self.setdefault("header", self.header)
        self.setdefault("caseSensitiveHeader", self.case_sensitive_header)


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
            "headers": {
                "type": "object",
                "requried": ["rows"],
                "properties": {
                    "rows": {"type": "array", "items": {"type": "number"}},
                    "join": {"type": "string"},
                },
            },
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
        headers=None,
    ):
        self.setinitial("sheet", sheet)
        self.setinitial("workbookCache", workbook_cache)
        self.setinitial("fillMergedCells", fill_merged_cells)
        self.setinitial("preserveFormatting", preserve_formatting)
        self.setinitial("adjustFloatingPointError", adjust_floating_point_error)
        super().__init__(descriptor, headers=headers)

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
        self.setdefault("sheet", self.sheet)
        self.setdefault("fillMergedCells", self.fill_merged_cells)
        self.setdefault("preserveFormatting", self.preserve_formatting)
        self.setdefault("adjustFloatingPointError", self.adjust_floating_point_error)


class InlineDialect(Dialect):
    """Inline dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        keyed? (bool): keyed

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "keys": {"type": "array"},
            "keyed": {"type": "boolean"},
            "headers": {
                "type": "object",
                "requried": ["rows"],
                "properties": {
                    "rows": {"type": "array", "items": {"type": "number"}},
                    "join": {"type": "string"},
                },
            },
        },
    }

    def __init__(self, descriptor=None, *, keys=None, keyed=None, headers=None):
        self.setinitial("keys", keys)
        self.setinitial("keyed", keyed)
        super().__init__(descriptor, headers=headers)

    @property
    def keys(self):
        return self.get("keys")

    @property
    def keyed(self):
        return self.get("keyed", False)

    # Expand

    def expand(self):
        self.setdefault("keyed", self.keyed)


class JsonDialect(Dialect):
    """Json dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        keyed? (bool): keyed
        property? (str): property

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "keys": {"type": "array"},
            "keyed": {"type": "boolean"},
            "property": {"type": "string"},
            "headers": {
                "type": "object",
                "requried": ["rows"],
                "properties": {
                    "rows": {"type": "array", "items": {"type": "number"}},
                    "join": {"type": "string"},
                },
            },
        },
    }

    def __init__(
        self, descriptor=None, *, keys=None, keyed=None, property=None, headers=None,
    ):
        self.setinitial("keys", keys)
        self.setinitial("keyed", keyed)
        self.setinitial("property", property)
        super().__init__(descriptor, headers=headers)

    @property
    def keys(self):
        return self.get("keys")

    @property
    def keyed(self):
        return self.get("keyed", False)

    @property
    def property(self):
        return self.get("property")

    # Expand

    def expand(self):
        super().expand()
        self.setdefault("keyed", self.keyed)
