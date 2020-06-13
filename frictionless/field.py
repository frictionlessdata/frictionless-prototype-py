from cached_property import cached_property
from .metadata import Metadata
from . import config


class Field(Metadata):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'required': ['name'],
        'properties': {'name': {'type': 'string'}},
    }

    @cached_property
    def name(self):
        """Field name

        # Returns
            str: field name

        """
        return self.get('name')

    @cached_property
    def type(self):
        """Field type

        # Returns
            str: field type

        """
        return self.get('type')

    @cached_property
    def format(self):
        """Field format

        # Returns
            str: field format

        """
        return self.get('format', 'default')

    @cached_property
    def missing_values(self):
        """Field's missing values

        # Returns
            str[]: missing values

        """
        return self.get('missingValues', config.MISSING_VALUES)

    @cached_property
    def required(self):
        """Whether field is required

        # Returns
            bool: true if required

        """
        return self.constraints.get('required', False)

    @cached_property
    def constraints(self):
        """Field constraints

        # Returns
            dict: dict of field constraints

        """
        return self.get('constraints', {})

    # Read

    def read_cell(self, cell):
        errors = None
        if cell in self.missing_values:
            cell = None
        if cell is not None:
            cell, errors = self.read_cell_cast(cell)
        if not errors:
            cell, errors = self.read_cell_check(cell)
        return cell, errors

    def read_cell_cast(self, cell):
        raise NotImplementedError()

    def read_cell_check(self, cell):
        raise NotImplementedError()

    # Write

    def write_cell(self, cell):
        errors = None
        if cell is None:
            cell = ''
        if cell is not None:
            cell, errors = self.write_cell_cast(cell)
        return cell, errors

    def write_cell_cast(self, cell):
        raise NotImplementedError()
