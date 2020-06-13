from ..field import Field


class AnyField(Field):

    # Read

    def read_cell_cast(self, cell):
        return cell, None

    # Write

    def write_cell(self, cell):
        return str(cell), None
