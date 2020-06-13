from ..field import Field


class StringField(Field):

    # Read

    def read_cell(self, cell):
        return cell

    # Write

    def write_cell(self, cell):
        return cell
