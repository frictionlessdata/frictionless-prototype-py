from ..field import Field


class IntegerField(Field):

    # Read

    def read_cell(self, cell):
        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)
