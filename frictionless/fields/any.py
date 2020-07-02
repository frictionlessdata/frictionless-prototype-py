from ..field import Field


class AnyField(Field):
    supported_constraints = [
        "required",
        "enum",
    ]

    # Read

    def read_cell_cast(self, cell):
        return cell

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)
