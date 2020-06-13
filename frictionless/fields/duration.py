import isodate
import datetime
from ..field import Field


class DurationField(Field):

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, (isodate.Duration, datetime.timedelta)):
            if not isinstance(cell, str):
                return None
            try:
                cell = isodate.parse_duration(cell)
            except Exception:
                return None
        return cell

    # Write

    def write_cell_cast(self, cell):
        return str(cell)
