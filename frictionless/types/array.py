import json
from ..type import Type


class ArrayType(Type):
    supported_constraints = [
        "required",
        "minLength",
        "maxLength",
        "enum",
    ]

    # Read

    def read_cell(self, cell):
        if not isinstance(cell, list):
            if isinstance(cell, tuple):
                return list(cell)
            if not isinstance(cell, str):
                return None
            try:
                cell = json.loads(cell)
            except Exception:
                return None
            if not isinstance(cell, list):
                return None
        return cell

    # Write

    def write_cell(self, cell):
        return json.dumps(cell)
