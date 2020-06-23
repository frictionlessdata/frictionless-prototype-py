import hashlib
import statistics
from .. import errors
from ..check import Check


class DuplicateRowCheck(Check):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {},
    }
    possible_Errors = [  # type: ignore
        errors.DuplicateRowError
    ]

    def prepare(self):
        self.memory = {}

    def validate_row(self, row):
        text = ','.join(map(str, row.values()))
        hash = hashlib.sha256(text.encode('utf-8')).hexdigest()
        match = self.memory.get(hash)
        if match:
            note = 'the same as row at position "%s"' % match
            yield errors.DuplicateRowError.from_row(row, note=note)
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
        errors.DeviatedValueError
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
            note = 'deviated value check requires field "%s" to exist'
            yield errors.TaskError(note=note % self.field_name)
        elif self.schema.get_field(self.field_name).type not in ['integer', 'number']:
            note = 'deviated value check requires field "%s" to be numiric'
            yield errors.TaskError(note=note % self.field_name)
        if not self.average_function:
            note = 'deviated value check supports only average functions "%s"'
            note = note % ', '.join(AVERAGE_FUNCTIONS.keys())
            yield errors.TaskError(note=note)

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
            note = 'calculation issue "%s"' % exception
            yield errors.DeviatedValueError(note=note)

        # Check values
        for row_position, cell in zip(self.row_positions, self.cells):
            if not (minimum <= cell <= maximum):
                dtl = 'value "%s" in row at position "%s" and field "%s" is deviated "[%.2f, %.2f]"'
                dtl = dtl % (cell, row_position, self.field_name, minimum, maximum)
                yield errors.DeviatedValueError(note=dtl)


class TruncatedValueCheck(Check):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {},
    }
    possible_Errors = [  # type: ignore
        errors.TruncatedValueError
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
                note = 'value  is probably truncated'
                yield errors.TruncatedValueError.from_row(
                    row, note=note, field_name=field_name
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
