from ..metadata import Metadata
from ..field import Field


class BooleanField(Field):
    supported_constraints = [
        "required",
        "enum",
    ]

    @Metadata.property
    def true_values(self):
        true_values = self.get("trueValues", DEFAULT_TRUE_VALUES)
        return self.metadata_attach("trueValues", true_values)

    @Metadata.property
    def false_values(self):
        false_values = self.get("falseValues", DEFAULT_FALSE_VALUES)
        return self.metadata_attach("falseValues", false_values)

    # Expand

    def expand(self):
        super().expand()
        self.setdefault("trueValues", self.true_values)
        self.setdefault("falseValues", self.false_values)

    # Read

    def read_cell_cast(self, cell):
        if cell is True or cell is False:
            return cell
        return self.read_cell_cast_mapping.get(cell)

    @Metadata.property(write=False)
    def read_cell_cast_mapping(self):
        mapping = {}
        for value in self.true_values:
            mapping[value] = True
        for value in self.false_values:
            mapping[value] = False
        return mapping

    # Write

    # TODO: implement proper casting
    def write_cell_cast(self, cell):
        return str(cell)


# Internal

DEFAULT_TRUE_VALUES = ["true", "True", "TRUE", "1"]
DEFAULT_FALSE_VALUES = ["false", "False", "FALSE", "0"]
