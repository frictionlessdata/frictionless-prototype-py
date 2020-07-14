import re
from decimal import Decimal
from ..metadata import Metadata
from ..field import Field


class NumberField(Field):
    supported_constraints = [
        "required",
        "minimum",
        "maximum",
        "enum",
    ]

    @Metadata.property
    def decimal_char(self):
        return self.get("decimalChar", DEFAULT_DECIMAL_CHAR)

    @Metadata.property
    def group_char(self):
        return self.get("groupChar", DEFAULT_GROUP_CHAR)

    @Metadata.property
    def bare_number(self):
        return self.get("bareNumber", DEFAULT_BARE_NUMBER)

    # Read

    def read_cell_cast(self, cell):
        if isinstance(cell, str):
            if self.read_cell_cast_processor:
                cell = self.read_cell_cast_processor(cell)
            try:
                return Decimal(cell)
            except Exception:
                return None
        elif isinstance(cell, Decimal):
            return cell
        elif cell is True or cell is False:
            return None
        elif isinstance(cell, int):
            return cell
        elif isinstance(cell, float):
            return Decimal(str(cell))
        return None

    @Metadata.property(write=False)
    def read_cell_cast_processor(self):
        if set(["groupChar", "decimalChar", "bareNumber"]).intersection(self.keys()):

            def processor(cell):
                cell = cell.replace(self.group_char, "")
                cell = cell.replace(self.decimal_char, ".")
                if self.read_cell_cast_pattern:
                    cell = self.read_cell_cast_pattern.sub("", cell)
                return cell

            return processor

    @Metadata.property(write=False)
    def read_cell_cast_pattern(self):
        if not self.bare_number:
            return re.compile(r"((^\D*)|(\D*$))")

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)


# Internal

DEFAULT_GROUP_CHAR = ""
DEFAULT_DECIMAL_CHAR = "."
DEFAULT_BARE_NUMBER = True
