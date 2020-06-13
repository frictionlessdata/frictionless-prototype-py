from ..field import Field


class AnyField(Field):

    # Read

    def read_cell_cast(self, cell):
        return cell

    # Write

    def write_cell_cast(self, cell):
        return str(cell)
