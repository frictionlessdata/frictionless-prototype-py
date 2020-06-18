from copy import deepcopy
from .metadata import ControlledMetadata
from .helpers import syncprop
from .field import Field
from . import errors
from . import config


class Schema(ControlledMetadata):
    """Schema representation

    # Arguments
        descriptor? (str|dict): schema descriptor

        fields? (dict[]): list of field descriptors
        missing_values? (str[]): missing_values
        primary_key? (str[]): primary_key
        foreign_keys? (dict[]): foreign_keys

        metadata_root? (Metadata): root metadata object
        metadata_raise? (bool): if True it will fail on the first metadata error
        metadata_resource? (Resource): parent resource

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = errors.SchemaError  # type: ignore
    metadata_profile = config.SCHEMA_PROFILE

    def __init__(
        self,
        descriptor=None,
        *,
        fields=None,
        missing_values=None,
        primary_key=None,
        foreign_keys=None,
        metadata_root=None,
        metadata_raise=False,
        metadata_resource=None,
    ):
        self.setdefined('fields', fields)
        self.setdefined('missingValues', missing_values)
        self.setdefined('primaryKey', primary_key)
        self.setdefined('foreignKeys', foreign_keys)
        self.__metadata_resource = metadata_resource
        super().__init__(
            descriptor=descriptor,
            metadata_root=metadata_root,
            metadata_raise=metadata_raise,
        )

    @syncprop('missingValues')
    def missing_values(self):
        missing_values = self.get('missingValues', config.MISSING_VALUES)
        return self.metadata_transorm_bind('missingValues', missing_values)

    @syncprop('primaryKey')
    def primary_key(self):
        primary_key = self.get('primaryKey', [])
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        return self.metadata_transorm_bind('primaryKey', primary_key)

    @syncprop('foreignKeys')
    def foreign_keys(self):
        foreign_keys = deepcopy(self.get('foreignKeys', []))
        for index, fk in enumerate(foreign_keys):
            if not isinstance(fk, dict):
                continue
            fk.setdefault('fields', [])
            fk.setdefault('reference', {})
            fk['reference'].setdefault('resource', '')
            fk['reference'].setdefault('fields', [])
            if not isinstance(fk['fields'], list):
                fk['fields'] = [fk['fields']]
            if not isinstance(fk['reference']['fields'], list):
                fk['reference']['fields'] = [fk['reference']['fields']]
        return self.metadata_transorm_bind('foreignKeys', foreign_keys)

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
        """Infer schema
        """
        pass

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
                field = Field(
                    field,
                    metadata_root=self.metadata_root,
                    metadata_raise=self.metadata_raise,
                    metadata_schema=self,
                )
                list.__setitem__(self.fields, index, field)
        super().metadata_process()

    def metadata_validate(self):
        super().metadata_validate()

        # Primary Key
        for name in self.primary_key:
            if name not in self.field_names:
                note = 'primary key "%s" does not match the fields "%s"'
                note = note % (self.primary_key, self.field_names)
                self.metadata_errors.append(errors.SchemaError(note=note))

        # Foreign Keys
        for fk in self.foreign_keys:
            for name in fk['fields']:
                if name not in self.field_names:
                    note = 'foreign key "%s" does not match the fields "%s"'
                    note = note % (fk, self.field_names)
                    self.metadata_errors.append(errors.SchemaError(note=note))
            if len(fk['fields']) != len(fk['reference']['fields']):
                note = 'foreign key fields "%s" does not match the reference fields "%s"'
                note = note % (fk['fields'], fk['reference']['fields'])
                self.metadata_errors.append(errors.SchemaError(note=note))
