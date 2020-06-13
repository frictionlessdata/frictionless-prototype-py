import re
from decimal import Decimal
from ..field import Field


class IntegerField(Field):

    # Read

    def read_cell(self, cell):
        if isinstance(cell, int):
            if cell is True or cell is False:
                return None
            pass

        elif isinstance(cell, str):
            if not self.get('bareNumber', DEFAULT_BARE_NUMBER):
                cell = re.sub(r'((^\D*)|(\D*$))', '', cell)

            try:
                cell = int(cell)
            except Exception:
                return None

        elif isinstance(cell, float) and cell.is_integer():
            cell = int(cell)

        elif isinstance(cell, Decimal) and cell % 1 == 0:
            cell = int(cell)

        else:
            return None

        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)


# Internal

DEFAULT_BARE_NUMBER = True
