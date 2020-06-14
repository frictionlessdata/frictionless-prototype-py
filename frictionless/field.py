import warnings
import importlib
from cached_property import cached_property
from .metadata import Metadata
from . import config


# TODO: reset cached properties on transform
class Field(Metadata):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'required': ['name'],
        'properties': {'name': {'type': 'string'}},
    }
    supported_constraints = []  # type: ignore

    def __init__(self, descriptor):
        super().__init__(descriptor)
        self.__proxy = None
        if type(self) is Field:
            pref = descriptor.get('type', '')
            name = f'{pref.capitalize()}Field'
            module = importlib.import_module('frictionless.fields')
            ProxyField = getattr(module, name, getattr(module, 'AnyField'))
            self.__proxy = ProxyField(descriptor)

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
        format = self.get('format', 'default')
        if format.startswith('fmt:'):
            warnings.warn(
                'Format "fmt:<PATTERN>" is deprecated. '
                'Please use "<PATTERN>" without "fmt:" prefix.',
                UserWarning,
            )
            format = format.replace('fmt:', '')
        return format

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
        note = None
        if cell in self.missing_values:
            cell = None
        if cell is not None:
            cell = self.read_cell_cast(cell)
            if cell is None:
                note = f'expected type is "{self.type}" and format is "{self.format}"'
        return cell, note

    def read_cell_cast(self, cell):
        return self.__proxy.read_cell_cast(cell)

    # Test

    def test_cell(self, cell):
        items = []
        # TODO: implement
        return items

    # Write

    def write_cell(self, cell):
        notes = None
        if cell is None:
            cell = ''
        if cell is not None:
            cell = self.write_cell_cast(cell)
        if cell is None:
            notes = [self.type_error_note]
        return cell, notes

    def write_cell_cast(self, cell):
        return self.__proxy.write_cell_cast(cell)
