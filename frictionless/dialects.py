from .metadata import Metadata


class Dialect(Metadata):
    """Dialect representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    def __init__(self, descriptor=None):
        super().__init__(descriptor)

    # Expand

    def expand(self):
        pass


# TODO: move to plugins


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
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'delimiter': {'type': 'string'},
            'lineTerminator': {'type': 'string'},
            'quoteChar': {'type': 'string'},
            'doubleQuote': {'type': 'boolean'},
            'escapeChar': {'type': 'string'},
            'nullSequence': {'type': 'string'},
            'skipInitialSpace': {'type': 'boolean'},
            'header': {'type': 'boolean'},
            'commentChar': {'type': 'string'},
            'caseSensitiveHeader': {'type': 'boolean'},
        },
    }

    def __init__(
        self,
        descriptor=None,
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
    ):
        super().__init(
            descriptor,
            delimiter=delimiter,
            line_terminator=line_terminator,
            quote_char=quote_char,
            double_quote=double_quote,
            escape_char=escape_char,
            null_sequence=null_sequence,
            skip_initial_space=skip_initial_space,
            header=header,
            comment_char=comment_char,
            case_sensitive_header=case_sensitive_header,
        )

    # Expand

    def expand(self):
        self.setdetault('delimiter', ',')
        self.setdetault('lineTerminator', '\r\n')
        self.setdetault('quoteChar', '""')
        self.setdetault('doubleQuote', True)
        self.setdetault('skipInitialSpace', True)
        self.setdetault('header', True)
        self.setdetault('caseSensitiveHeader', False)


class ExcelDialect(Dialect):
    """Excel dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        sheet? (int|str): sheet
        fill_merged_cells? (bool): fill_merged_cells
        preserve_formatting? (bool): preserve_formatting
        adjust_floating_point_error? (bool): adjust_floating_point_error

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'sheet': {'type': ['number', 'string']},
            'fillMergedCells': {'type': 'boolean'},
            'preserveFormatting': {'type': 'boolean'},
            'adjustFloatingPointError': {'type': 'boolean'},
        },
    }

    def __init__(
        self,
        descriptor=None,
        sheet=None,
        fill_merged_cells=None,
        preserve_formatting=None,
        adjust_floating_point_error=None,
    ):
        super().__init(
            descriptor,
            sheet=sheet,
            fill_merged_cells=fill_merged_cells,
            preserve_formatting=preserve_formatting,
            adjust_floating_point_error=adjust_floating_point_error,
        )

    # Expand

    def expand(self):
        self.setdetault('sheet', 1)
        self.setdetault('fillMergedCells', False)
        self.setdetault('preserveFormatting', False)
        self.setdetault('adjustFloatingPointError', False)


class JsonDialect(Dialect):
    """Excel dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        keyes? (bool): keyes
        lined? (bool): lined
        property? (str): property

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'keyed': {'type': 'boolean'},
            'lined': {'type': 'boolean'},
            'property': {'type': 'string'},
        },
    }

    def __init__(
        self, descriptor=None, keyed=None, lined=None, property=None,
    ):
        super().__init(
            descriptor, keyed=keyed, lined=lined, property=property,
        )

    # Expand

    def expand(self):
        self.setdetault('keyed', False)
        self.setdetault('lined', False)
