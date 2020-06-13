from datetime import datetime, time
from dateutil.parser import parse
from ..field import Field


class TimeField(Field):

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, time):
            if not isinstance(cell, str):
                return None
            try:
                if format == 'default':
                    cell = datetime.strptime(cell, DEFAULT_PATTERN).time()
                elif format == 'any':
                    cell = parse(cell).time()
                else:
                    cell = datetime.strptime(cell, format).time()
            except Exception:
                return None
        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)


# Internal

DEFAULT_PATTERN = '%H:%M:%S'
