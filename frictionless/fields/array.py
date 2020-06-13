from ..field import Field


class ArrayField(Field):

    # Read

    def read_cell_cast(self, cell):
        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)
