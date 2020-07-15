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

    metadata_Error = errors.DialectError
    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
        },
    }

    def __init__(self, descriptor=None, header=None, header_rows=None, header_join=None):
        self.setinitial("header", header)
        self.setinitial("headerRows", header_rows)
        self.setinitial("headerJoin", header_join)
        super().__init__(descriptor)

    @Metadata.property
    def header(self):
        return self.get("header", config.DEFAULT_HEADER)

    @Metadata.property
    def header_rows(self):
        return self.get("headerRows", config.DEFAULT_HEADER_ROWS)

    @Metadata.property
    def header_join(self):
        return self.get("headerJoin", config.DEFAULT_HEADER_JOIN)

    # Expand

    def expand(self):
        self.setdefault("header", self.header)
        self.setdefault("headerRows", self.header_rows)
        self.setdefault("headerJoin", self.header_join)

    # Import/Export

    def to_dict(self, expand=False):
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result


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
            "commentChar": {"type": "string"},
            "caseSensitiveHeader": {"type": "boolean"},
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
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
        comment_char=None,
        case_sensitive_header=None,
        header=None,
        header_rows=None,
        header_join=None,
    ):
        self.setinitial("delimiter", delimiter)
        self.setinitial("lineTerminator", line_terminator)
        self.setinitial("quoteChar", quote_char)
        self.setinitial("doubleQuote", double_quote)
        self.setinitial("escapeChar", escape_char)
        self.setinitial("nullSequence", null_sequence)
        self.setinitial("skipInitialSpace", skip_initial_space)
        self.setinitial("commentChar", comment_char)
        self.setinitial("caseSensitiveHeader", case_sensitive_header)
        super().__init__(
            descriptor=descriptor,
            header=header,
            header_rows=header_rows,
            header_join=header_join,
        )

    @Metadata.property
    def delimiter(self):
        return self.get("delimiter", ",")

    @Metadata.property
    def line_terminator(self):
        return self.get("lineTerminator", "\r\n")

    @Metadata.property
    def quote_char(self):
        return self.get("quoteChar", '"')

    @Metadata.property
    def double_quote(self):
        return self.get("doubleQuote", True)

    @Metadata.property
    def escape_char(self):
        return self.get("escapeChar")

    @Metadata.property
    def null_sequence(self):
        return self.get("nullSequence")

    @Metadata.property
    def skip_initial_space(self):
        return self.get("skipInitialSpace", False)

    @Metadata.property
    def comment_char(self):
        return self.get("commentChar")

    @Metadata.property
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
        self.setdefault("caseSensitiveHeader", self.case_sensitive_header)

    # Import/Export

    def to_python(self):
        dialect = csv.excel()
        dialect.delimiter = self.delimiter
        dialect.doublequote = self.double_quote if self.escape_char else True
        dialect.escapechar = self.escape_char
        dialect.lineterminator = self.line_terminator
        dialect.quotechar = self.quote_char
        dialect.quoting = csv.QUOTE_NONE if self.quote_char == "" else csv.QUOTE_MINIMAL
        dialect.skipinitialspace = self.skip_initial_space
        return dialect


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
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
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
        header=None,
        header_rows=None,
        header_join=None,
    ):
        self.setinitial("sheet", sheet)
        self.setinitial("workbookCache", workbook_cache)
        self.setinitial("fillMergedCells", fill_merged_cells)
        self.setinitial("preserveFormatting", preserve_formatting)
        self.setinitial("adjustFloatingPointError", adjust_floating_point_error)
        super().__init__(
            descriptor=descriptor,
            header=header,
            header_rows=header_rows,
            header_join=header_join,
        )

    @Metadata.property
    def sheet(self):
        return self.get("sheet", 1)

    @Metadata.property
    def workbook_cache(self):
        return self.get("workbookCache")

    @Metadata.property
    def fill_merged_cells(self):
        return self.get("fillMergedCells", False)

    @Metadata.property
    def preserve_formatting(self):
        return self.get("preserveFormatting", False)

    @Metadata.property
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
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        keys=None,
        keyed=None,
        header=None,
        header_rows=None,
        header_join=None,
    ):
        self.setinitial("keys", keys)
        self.setinitial("keyed", keyed)
        super().__init__(
            descriptor=descriptor,
            header=header,
            header_rows=header_rows,
            header_join=header_join,
        )

    @Metadata.property
    def keys(self):
        return self.get("keys")

    @Metadata.property
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
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        keys=None,
        keyed=None,
        property=None,
        header=None,
        header_rows=None,
        header_join=None,
    ):
        self.setinitial("keys", keys)
        self.setinitial("keyed", keyed)
        self.setinitial("property", property)
        super().__init__(
            descriptor=descriptor,
            header=header,
            header_rows=header_rows,
            header_join=header_join,
        )

    @Metadata.property
    def keys(self):
        return self.get("keys")

    @Metadata.property
    def keyed(self):
        return self.get("keyed", False)

    @Metadata.property
    def property(self):
        return self.get("property")

    # Expand

    def expand(self):
        super().expand()
        self.setdefault("keyed", self.keyed)
