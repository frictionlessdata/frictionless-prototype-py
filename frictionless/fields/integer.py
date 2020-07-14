import re
from decimal import Decimal
from ..metadata import Metadata
from ..field import Field


class IntegerField(Field):
    supported_constraints = [
        "required",
        "minimum",
        "maximum",
        "enum",
    ]

    @Metadata.property
    def bare_number(self):
        return self.get("bareNumber", DEFAULT_BARE_NUMBER)

    # Read

    def read_cell_cast(self, cell):
        if isinstance(cell, str):
            if self.read_cell_cast_pattern:
                cell = self.read_cell_cast_pattern.sub("", cell)
            try:
                return int(cell)
            except Exception:
                return None
        elif cell is True or cell is False:
            return None
        elif isinstance(cell, int):
            return cell
        elif isinstance(cell, float) and cell.is_integer():
            return int(cell)
        elif isinstance(cell, Decimal) and cell % 1 == 0:
            return int(cell)
        return None

    @Metadata.property(write=False)
    def read_cell_cast_pattern(self):
        if not self.bare_number:
            return re.compile(r"((^\D*)|(\D*$))")

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)


# Internal

DEFAULT_BARE_NUMBER = True
