import json
from ..type import Type


class ObjectType(Type):
    supported_constraints = [
        "required",
        "minLength",
        "maxLength",
        "enum",
    ]

    # Read

    def read_cell(self, cell):
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

    # TODO: implement proper casting
    def write_cell(self, cell):
        return str(cell)
