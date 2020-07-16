from .helpers import cached_property


class Type:
    supported_constraints = []

    def __init__(self, field):
        self.__field = field

    @cached_property
    def field(self):
        return self.__field

    # Read

    def read_cell(self, cell):
        raise NotImplementedError()

    # Write

    def write_cell(self, cell):
        raise NotImplementedError()
