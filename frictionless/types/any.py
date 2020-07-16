from ..type import Type


class AnyType(Type):
    supported_constraints = [
        "required",
        "enum",
    ]

    # Read

    def read_cell(self, cell):
        return cell

    # Write

    # TODO: implement proper casting
    def write_cell(self, cell):
        return str(cell)
