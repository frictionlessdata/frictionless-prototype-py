import csv
from .metadata import Metadata
from . import config


class Dialect(Metadata):
    """Dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        headers? (int|list): headers

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    # TODO: add not additionalProperties?
    # TODO: make headers validation stricter
    metadata_profile = {  # type: ignore
        "type": "object",
        "properties": {"headers": {"type": ["object", "array", "number", "null"]}},
    }

    def __init__(self, descriptor=None, headers=None):
        self.setinitial("headers", headers)
        super().__init__(descriptor)

    @property
    def headers(self):
        row = config.DEFAULT_HEADERS_ROW
        join = config.DEFAULT_HEADERS_JOIN
        headers = self.get("headers", row)
        if not headers:
            headers = {"rows": [], "join": join}
        elif isinstance(headers, int):
            headers = {"rows": [headers], "join": join}
        elif isinstance(headers, list):
            headers = {"rows": headers, "join": join}
        return self.metadata_attach("headers", headers)

    # Expand

    def expand(self):
        self.setdetault("headers", self.headers)


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
            "headers": {"type": ["object", "array", "number", "null"]},
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
            "headers": {"type": ["object", "array", "number", "null"]},
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
        self.setdetault("sheet", self.sheet)
        self.setdetault("fillMergedCells", self.fill_merged_cells)
        self.setdetault("preserveFormatting", self.preserve_formatting)
        self.setdetault("adjustFloatingPointError", self.adjust_floating_point_error)


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
            "headers": {"type": ["object", "array", "number", "null"]},
        },
    }

    def __init__(self, descriptor=None, *, keyed=None, headers=None):
        self.setinitial("keyed", keyed)
        super().__init__(descriptor, headers=headers)

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
            "headers": {"type": ["object", "array", "number", "null"]},
        },
    }

    def __init__(
        self, descriptor=None, *, keyed=None, property=None, headers=None,
    ):
        self.setinitial("keyed", keyed)
        self.setinitial("property", property)
        super().__init__(descriptor, headers=headers)

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
