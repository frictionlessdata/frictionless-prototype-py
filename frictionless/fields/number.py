import re
from decimal import Decimal
from ..field import Field


class NumberField(Field):
    supported_constraints = [
        'required',
        'minimum',
        'maximum',
        'enum',
    ]

    # Read

    def read_cell_cast(self, cell):
        group_char = self.get('groupChar', DEFAULT_GROUP_CHAR)
        decimal_char = self.get('decimalChar', DEFAULT_DECIMAL_CHAR)
        if not isinstance(cell, Decimal):
            if isinstance(cell, str):
                cell = re.sub(r'\s', '', cell)
                cell = cell.replace(decimal_char, '__decimal_char__')
                cell = cell.replace(group_char, '')
                cell = cell.replace('__decimal_char__', '.')
                if not self.get('bareNumber', DEFAULT_BARE_NUMBER):
                    cell = re.sub(r'((^\D*)|(\D*$))', '', cell)
            elif not isinstance(cell, (int,) + (float,)):
                return None
            elif cell is True or cell is False:
                return None
            try:
                if isinstance(cell, float):
                    cell = str(cell)
                cell = Decimal(cell)
            except Exception:
                return None
        return cell

    # Write

    def write_cell_cast(self, cell):
        return str(cell)


# Internal

DEFAULT_GROUP_CHAR = ''
DEFAULT_DECIMAL_CHAR = '.'
DEFAULT_BARE_NUMBER = True
