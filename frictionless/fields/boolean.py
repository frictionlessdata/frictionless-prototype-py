from ..metadata import Metadata
from ..field import Field


class BooleanField(Field):
    supported_constraints = [
        "required",
        "enum",
    ]

    # Read

    def read_cell_cast(self, cell):
        if cell is True or cell is False:
            return cell
        return self.read_cell_cast_mapping.get(cell)

    @Metadata.property(write=False)
    def read_cell_cast_mapping(self):
        mapping = {}
        for value in self.get("trueValues", TRUE_VALUES):
            mapping[value] = True
        for value in self.get("falseValues", FALSE_VALUES):
            mapping[value] = False
        return mapping

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)


# Internal

TRUE_VALUES = ["true", "True", "TRUE", "1"]
FALSE_VALUES = ["false", "False", "FALSE", "0"]
