import hashlib
import statistics
from .. import errors
from ..check import Check
from ..plugin import Plugin


# Plugin


class HintPlugin(Plugin):
    def create_check(self, name, *, descriptor=None):
        if name == 'hint/duplicate-row':
            return DuplicateRowCheck(descriptor)
        if name == 'hint/deviated-value':
            return DeviatedValueCheck(descriptor)
        if name == 'hint/truncated-value':
            return TruncatedValueCheck(descriptor)


# Errors


class DuplicateRowError(errors.RowError):
    code = 'hint/duplicate-row'
    name = 'Duplicate Row'
    tags = ['#body', '#hint']
    template = 'Row at position {rowPosition} is duplicated: {details}'
    description = 'The row is duplicated.'


class DeviatedValueError(errors.Error):
    code = 'hint/deviated-value'
    name = 'Deviated Value'
    tags = ['#body', '#hint']
    template = 'There is a possible error because the value is deviated: {details}'
    description = 'The value is deviated.'


class TruncatedValueError(errors.CellError):
    code = 'hint/truncated-value'
    name = 'Truncated Value'
    tags = ['#body', '#hint']
    template = 'The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {details}'
    description = 'The value is possible truncated.'


# Checks


class DuplicateRowCheck(Check):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {},
    }
    possible_Errors = [  # type: ignore
        DuplicateRowError
    ]

    def prepare(self):
        self.memory = {}

    def validate_row(self, row):
        text = ','.join(map(str, row.values()))
        hash = hashlib.sha256(text.encode('utf-8')).hexdigest()
        match = self.memory.get(hash)
        if match:
            details = 'the same as row at position "%s"' % match
            yield DuplicateRowError.from_row(row, details=details)
        self.memory[hash] = row.row_position


class DeviatedValueCheck(Check):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'requred': ['fieldName'],
        'properties': {
            'fieldName': {'type': 'string'},
            'average': {'type': ['string', 'null']},
            'interval': {'type': ['number', 'null']},
        },
    }
    possible_Errors = [  # type: ignore
        DeviatedValueError
    ]

    def prepare(self):
        self.exited = False
        self.cells = []
        self.row_positions = []
        self.field_name = self['fieldName']
        self.interval = self.get('interval', 3)
        self.average = self.get('average', 'mean')
        self.average_function = AVERAGE_FUNCTIONS.get(self.average)

    # Validate

    def validate_task(self):
        if self.field_name not in self.schema.field_names:
            details = 'deviated value check requires field "%s" to exist'
            yield errors.TaskError(details=details % self.field_name)
        elif self.schema.get_field(self.field_name).type not in ['integer', 'number']:
            details = 'deviated value check requires field "%s" to be numiric'
            yield errors.TaskError(details=details % self.field_name)
        if not self.average_function:
            details = 'deviated value check supports only average functions "%s"'
            details = details % ', '.join(AVERAGE_FUNCTIONS.keys())
            yield errors.TaskError(details=details)

    def validate_row(self, row):
        cell = row[self.field_name]
        if cell is not None:
            self.cells.append(cell)
            self.row_positions.append(row.row_position)
        yield from []

    def validate_table(self):
        if len(self.cells) < 2:
            return

        # Prepare interval
        try:
            stdev = statistics.stdev(self.cells)
            average = self.average_function(self.cells)
            minimum = average - stdev * self.interval
            maximum = average + stdev * self.interval
        except Exception as exception:
            details = 'calculation issue "%s"' % exception
            yield DeviatedValueError(details=details)

        # Check values
        for row_position, cell in zip(self.row_positions, self.cells):
            if not (minimum <= cell <= maximum):
                dtl = 'value "%s" in row at position "%s" and field "%s" is deviated "[%.2f, %.2f]"'
                dtl = dtl % (cell, row_position, self.field_name, minimum, maximum)
                yield DeviatedValueError(details=dtl)


class TruncatedValueCheck(Check):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {},
    }
    possible_Errors = [  # type: ignore
        TruncatedValueError
    ]

    def validate_row(self, row):
        for field_name, cell in row.items():
            truncated = False
            if cell is None:
                continue

            # Check string cutoff
            if isinstance(cell, str):
                if len(cell) in TRUNCATED_STRING_LENGTHS:
                    truncated = True

            # Check integer cutoff
            if isinstance(cell, int):
                if cell in TRUNCATED_INTEGER_VALUES:
                    truncated = True

            # Add error
            if truncated:
                details = 'value  is probably truncated'
                yield TruncatedValueError.from_row(
                    row, details=details, field_name=field_name
                )


# Internal


AVERAGE_FUNCTIONS = {
    'mean': statistics.mean,
    'median': statistics.median,
    'mode': statistics.mode,
}
TRUNCATED_STRING_LENGTHS = [
    255,
]
TRUNCATED_INTEGER_VALUES = [
    # BigInt
    18446744073709551616,
    9223372036854775807,
    # Int
    4294967295,
    2147483647,
    # SummedInt
    2097152,
    # SmallInt
    65535,
    32767,
]
