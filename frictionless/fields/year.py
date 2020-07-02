from ..field import Field


class YearField(Field):
    supported_constraints = [
        "required",
        "minimum",
        "maximum",
        "enum",
    ]

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, int):
            if not isinstance(cell, str):
                return None
            if len(cell) != 4:
                return None
            try:
                cell = int(cell)
            except Exception:
                return None
        if cell < 0 or cell > 9999:
            return None
        return cell

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)
