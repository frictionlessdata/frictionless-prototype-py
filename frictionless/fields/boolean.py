from ..field import Field


class BooleanField(Field):

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, bool):
            if isinstance(cell, str):
                cell = cell.strip()
            if cell in self.get('trueValues', TRUE_VALUES):
                cell = True
            elif cell in self.get('falseValues', FALSE_VALUES):
                cell = False
            else:
                return None
        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)


# Internal

TRUE_VALUES = ['true', 'True', 'TRUE', '1']
FALSE_VALUES = ['false', 'False', 'FALSE', '0']
