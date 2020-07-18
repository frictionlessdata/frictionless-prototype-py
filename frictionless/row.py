from itertools import zip_longest
from collections import OrderedDict
from .helpers import cached_property
from .parsers import JsonParser
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

    def __init__(self, cells, *, schema, field_positions, row_position, row_number):
        assert len(field_positions) in (len(cells), len(schema.fields))

        # Set attributes
        fields = schema.fields
        self.__schema = schema
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
                        note="",
                        cells=list(map(str, cells)),
                        row_number=row_number,
                        row_position=row_position,
                        cell=str(cell),
                        field_name="",
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
                            note="",
                            cells=list(map(str, cells)),
                            row_number=row_number,
                            row_position=row_position,
                            cell="",
                            field_name=field.name,
                            field_number=field_number,
                            field_position=field_position
                            or max(field_positions) + field_number - start + 1,
                        )
                    )

        # Iterate items
        field_number = 0
        for field_position, field, source in zip(field_positions, fields, cells):
            field_number += 1

            # Read cell
            target, notes = field.read_cell(source)
            type_note = notes.pop("type", None) if notes else None
            if target is None and not type_note:
                self.__blank_cells[field.name] = source
            self[field.name] = target

            # Type error
            if type_note:
                self.__error_cells[field.name] = source
                self.__errors.append(
                    errors.TypeError(
                        note=type_note,
                        cells=list(map(str, cells)),
                        row_number=row_number,
                        row_position=row_position,
                        cell=str(source),
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
                            cell=str(source),
                            field_name=field.name,
                            field_number=field_number,
                            field_position=field_position,
                        )
                    )

        # Blank row
        if len(self) == len(self.__blank_cells):
            self.__errors = [
                errors.BlankRowError(
                    note="",
                    cells=list(map(str, cells)),
                    row_number=row_number,
                    row_position=row_position,
                )
            ]

    @cached_property
    def schema(self):
        return self.__schema

    @cached_property
    def field_positions(self):
        return self.__field_positions

    @cached_property
    def row_position(self):
        return self.__row_position

    @cached_property
    def row_number(self):
        return self.__row_number

    @cached_property
    def blank_cells(self):
        return self.__blank_cells

    @cached_property
    def error_cells(self):
        return self.__error_cells

    @cached_property
    def errors(self):
        return self.__errors

    @cached_property
    def valid(self):
        return not self.__errors

    # Import/Export

    def to_dict(self, *, json=False):
        if json:
            result = {}
            for field in self.__schema.fields:
                cell = self[field.name]
                if field.type not in JsonParser.native_types:
                    cell, notes = field.write_cell(cell)
                result[field.name] = cell
            return result
        return dict(self)

    def to_list(self, *, json=False):
        if json:
            result = []
            for field in self.__schema.fields:
                cell = self[field.name]
                if field.type not in JsonParser.native_types:
                    cell, notes = field.write_cell(cell)
                result.append(cell)
            return result
        return list(self.values())
