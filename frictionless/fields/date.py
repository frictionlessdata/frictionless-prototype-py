from datetime import datetime, date
from dateutil.parser import parse
from ..field import Field


class DateField(Field):

    # Read

    def read_cell_cast(self, cell):
        if isinstance(cell, datetime):
            value_time = cell.time()
            if value_time.hour == 0 and value_time.minute == 0 and value_time.second == 0:
                return datetime(cell.year, cell.month, cell.day).date()
            else:
                return None

        if isinstance(cell, date):
            return cell

        if not isinstance(cell, str):
            return None

        # Parse string date
        try:
            if format == 'default':
                cell = datetime.strptime(cell, DEFAULT_PATTERN).date()
            elif format == 'any':
                cell = parse(cell).date()
            else:
                cell = datetime.strptime(cell, format).date()
        except Exception:
            return None

        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)


# Internal

DEFAULT_PATTERN = '%Y-%m-%d'
