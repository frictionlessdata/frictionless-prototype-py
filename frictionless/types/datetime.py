from datetime import datetime
from dateutil.parser import parse
from ..type import Type
from .. import config


class DatetimeType(Type):
    supported_constraints = [
        "required",
        "minimum",
        "maximum",
        "enum",
    ]

    # Read

    def read_cell(self, cell):
        if not isinstance(cell, datetime):
            if not isinstance(cell, str):
                return None
            try:
                if self.field.format == "default":
                    cell = datetime.strptime(cell, config.DEFAULT_DATETIME_PATTERN)
                elif self.field.format == "any":
                    cell = parse(cell)
                else:
                    cell = datetime.strptime(cell, self.field.format)
            except Exception:
                return None
        return cell

    # Write

    def write_cell(self, cell):
        format = self.field.format or config.DEFAULT_DATETIME_PATTERN
        return datetime.strftime(cell, format)
