import json
from ..field import Field


class ObjectField(Field):

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, dict):
            if not isinstance(cell, str):
                return None
            try:
                cell = json.loads(cell)
            except Exception:
                return None
            if not isinstance(cell, dict):
                return None
        return cell

    # Write

    def write_cell_cast(self, cell):
        return str(cell)
