from datetime import datetime
from dateutil.parser import parse
from ..field import Field


class DatetimeField(Field):
    supported_constraints = [
        "required",
        "minimum",
        "maximum",
        "enum",
    ]

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, datetime):
            if not isinstance(cell, str):
                return None
            try:
                if self.format == "default":
                    cell = datetime.strptime(cell, DEFAULT_PATTERN)
                elif self.format == "any":
                    cell = parse(cell)
                else:
                    cell = datetime.strptime(cell, self.format)
            except Exception:
                return None
        return cell

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)


# Internal

DEFAULT_PATTERN = "%Y-%m-%dT%H:%M:%SZ"
