from itertools import zip_longest
from collections import OrderedDict
from .helpers import memoprop
from . import errors


class Row(OrderedDict):
    """Row representation

    # Arguments
        cells
        fields
        field_positions
        row_position
        row_number

    """

    def __init__(self, cells, *, fields, field_positions, row_position, row_number):
        assert len(field_positions) in (len(cells), len(fields))

        # Set params
        self.__field_positions = field_positions
        self.__row_position = row_position
        self.__row_number = row_number
        self.__blank_cells = {}
        self.__error_cells = {}
        self.__errors = []

        # Extra cells
        if len(fields) < len(cells):
            iterator = cells[len(fields) :]
            start = max(field_positions[: len(fields)]) + 1
            del cells[len(fields) :]
            for field_position, cell in enumerate(iterator, start=start):
                self.__errors.append(
                    errors.ExtraCellError(
                        note='',
                        cells=list(map(str, cells)),
                        row_number=row_number,
                        row_position=row_position,
                        cell=str(cell),
                        field_name='',
                        field_number=len(fields) + field_position - start,
                        field_position=field_position,
                    )
                )

        # Missing cells
        if len(fields) > len(cells):
            start = len(cells) + 1
            iterator = zip_longest(field_positions[len(cells) :], fields[len(cells) :])
            for field_number, (field_position, field) in enumerate(iterator, start=start):
                if field is not None:
                    cells.append(None)
                    self.__errors.append(
                        errors.MissingCellError(
                            note='',
                            cells=list(map(str, cells)),
                            row_number=row_number,
                            row_position=row_position,
                            cell='',
                            field_name=field.name,
                            field_number=field_number,
                            field_position=field_position
                            or max(field_positions) + field_number - start + 1,
                        )
                    )

        # Iterate items
        is_blank = True
        field_number = 0
        for field_position, field, cell in zip(field_positions, fields, cells):
            field_number += 1

            # Read cell
            self[field.name], notes = field.read_cell(cell)
            type_note = notes.pop('type', None) if notes else None
            if self[field.name] is None and not type_note:
                self.__blank_cells[field.name] = cell
            else:
                is_blank = False

            # Type error
            if type_note:
                self.__error_cells[field.name] = cell
                self.__errors.append(
                    errors.TypeError(
                        note=type_note,
                        cells=list(map(str, cells)),
                        row_number=row_number,
                        row_position=row_position,
                        cell=str(cell),
                        field_name=field.name,
                        field_number=field_number,
                        field_position=field_position,
                    )
                )

            # Constraint errors
            if notes:
                for note in notes.values():
                    self.__errors.append(
                        errors.ConstraintError(
                            note=note,
                            cells=list(map(str, cells)),
                            row_number=row_number,
                            row_position=row_position,
                            cell=str(cell),
                            field_name=field.name,
                            field_number=field_number,
                            field_position=field_position,
                        )
                    )

        # Blank row
        if is_blank:
            self.__errors = [
                errors.BlankRowError(
                    note='',
                    cells=list(map(str, cells)),
                    row_number=row_number,
                    row_position=row_position,
                )
            ]

    @memoprop
    def field_positions(self):
        return self.__field_positions

    @memoprop
    def row_position(self):
        return self.__row_position

    @memoprop
    def row_number(self):
        return self.__row_number

    @memoprop
    def blank_cells(self):
        return self.__blank_cells

    @memoprop
    def error_cells(self):
        return self.__error_cells

    @memoprop
    def errors(self):
        return self.__errors
