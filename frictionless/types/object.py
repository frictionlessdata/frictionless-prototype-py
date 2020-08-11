import json
from ..type import Type


class ObjectType(Type):
    """Object type implementation.

    API      | Usage
    -------- | --------
    Public   | `from frictionless import types`

    """

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

    def write_cell(self, cell):
        return json.dumps(cell)
