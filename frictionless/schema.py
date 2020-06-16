import io
import json
from .metadata import ControlledMetadata
from .field import Field
from . import exceptions
from . import helpers
from . import errors
from . import config


class Schema(ControlledMetadata):
    """Schema representation

    # Arguments
        descriptor (str|dict): schema descriptor
        strict (bool): if True it will raise a first validation error

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = errors.SchemaError  # type: ignore
    metadata_profile = config.SCHEMA_PROFILE

    @property
    def missing_values(self):
        """Schema's missing values

        # Returns
            str[]: missing values

        """
        return self.get('missingValues', config.MISSING_VALUES)

    @property
    def primary_key(self):
        """Schema's primary keys

        # Returns
            str[]: primary keys

        """
        primary_key = self.get('primaryKey', [])
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        return primary_key

    @property
    def foreign_keys(self):
        """Schema's foreign keys

        # Returns
            dict[]: foreign keys

        """
        foreign_keys = self.get('foreignKeys', [])
        for key in foreign_keys:
            key.setdefault('fields', [])
            key.setdefault('reference', {})
            key['reference'].setdefault('resource', '')
            key['reference'].setdefault('fields', [])
            if not isinstance(key['fields'], list):
                key['fields'] = [key['fields']]
            if not isinstance(key['reference']['fields'], list):
                key['reference']['fields'] = [key['reference']['fields']]
        return foreign_keys

    # Fields

    @property
    def fields(self):
        """Schema's fields

        # Returns
            Field[]: an array of field instances

        """
        return self.get('fields', [])

    @property
    def field_names(self):
        """Schema's field names

        # Returns
            str[]: an array of field names

        """
        return [field.name for field in self.fields]

    def add_field(self, descriptor):
        """ Add new field to schema.

        The schema descriptor will be validated with newly added field descriptor.

        # Arguments
            descriptor (dict): field descriptor

        # Returns
            Field/None: added `Field` instance or `None` if not added

        """
        self.setdefault('fields', [])
        self['fields'].append(descriptor)
        return self.fields[-1]

    def del_field(self, name):
        """Remove field by name.

        The schema descriptor will be validated after field descriptor removal.

        # Arguments
            name (str): schema field name

        # Returns
            Field/None: removed `Field` instances or `None` if not found

        """
        field = self.get_field(name)
        if field:
            predicat = lambda field: field.name != name
            self['fields'] = list(filter(predicat, self.fields))
        return field

    def get_field(self, name):
        """Get schema's field by name.

        # Arguments
            name (str): schema field name

        # Returns
           Field/None: `Field` instance or `None` if not found

        """
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def has_field(self, name):
        """Check if a field is present

        # Arguments
            name (str): schema field name

        # Returns
           bool: whether there is the field

        """
        for field in self.fields:
            if field.name == name:
                return True
        return False

    # Expand

    def expand(self):
        """Expand the schema

        It will add default values to the schema.

        """
        self.setdefault('fields', [])
        self.setdefault('missingValues', config.MISSING_VALUES)
        for field in self.fields:
            field.expand()

    # Infer

    def infer(self, sample):
        pass

    # Save

    def save(self, target, ensure_ascii=True):
        """Save schema descriptor to target destination.

        # Arguments
            target (str): path where to save a descriptor
            ensure_ascii (bool): the same as `json.dump` provides

        # Raises
            FrictionlessException: raises any error that occurs during the process

        """
        try:
            helpers.ensure_dir(target)
            with io.open(target, mode='w', encoding='utf-8') as file:
                json.dump(self, file, indent=4, ensure_ascii=ensure_ascii)
        except Exception as exception:
            raise exceptions.FrictionlessException(str(exception)) from exception

    # Read

    def read_cells(self, cells):
        """Read a list of cells (normalize/cast)

        # Arguments
            cells (any[]): list of cells

        # Returns
            any[]: list of processed cells

        """
        result_cells = []
        result_notes = []
        for index, field in enumerate(self.fields):
            cell = cells[index] if len(cells) > index else None
            cell, notes = field.read_cell(cell)
            result_cells.append(cell)
            result_notes.append(notes)
        return result_cells, result_notes

    # Write

    def write_cells(self, cells):
        """Write a list of cells (normalize/uncast)

        # Arguments
            cells (any[]): list of cells

        # Returns
            any[]: list of processed cells

        """
        result_cells = []
        result_notes = []
        for index, field in enumerate(self.fields):
            cell = cells[index] if len(cells) > index else None
            cell, notes = field.write_cell(cell)
            result_cells.append(cell)
            result_notes.append(notes)
        return result_cells, result_notes

    # Metadata

    def metadata_process(self):
        for index, field in enumerate(self.fields):
            if not isinstance(field, Field):
                if not isinstance(field, dict):
                    field = {'name': f'field{index+1}', 'type': 'any'}
                field = Field(field, strict=self.metadata_strict, schema=self)
                list.__setitem__(self.fields, index, field)
        super().metadata_process()

    def metadata_validate(self):
        super().metadata_validate()
