import re
import decimal
import warnings
import importlib
from functools import partial
from collections import OrderedDict
from .metadata import ControlledMetadata
from . import helpers
from . import errors
from . import config


class Field(ControlledMetadata):
    """Field representation

    # Arguments
        descriptor? (str|dict): field descriptor

        name? (str): name
        type? (str): type
        format? (str): format
        missing_values? (str[]): missing_values
        constraints? (dict): constraints

        metadata_root? (Metadata): root metadata object
        metadata_raise? (bool): if True it will fail on the first metadata error
        metadata_schema? (Schema): parent schema object

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = errors.SchemaError  # type: ignore
    metadata_profile = {  # type: ignore
        'type': 'object',
        'required': ['name'],
        'properties': {'name': {'type': 'string'}},
    }
    supported_constraints = []  # type: ignore

    def __init__(
        self,
        descriptor=None,
        *,
        name=None,
        type=None,
        format=None,
        missing_values=None,
        constraints=None,
        metadata_root=None,
        metadata_raise=False,
        metadata_schema=None,
    ):
        self.setdefined('name', name)
        self.setdefined('type', type)
        self.setdefined('format', format)
        self.setdefined('missingValues', missing_values)
        self.setdefined('constraints', constraints)
        self.__metadata_schema = metadata_schema
        super().__init__(
            descriptor=descriptor,
            metadata_root=metadata_root,
            metadata_raise=metadata_raise,
        )

    @helpers.cached
    def name(self):
        return self.get('name', 'field')

    @name.setter  # type: ignore
    def name(self, value):
        self['name'] = value

    @helpers.cached
    def type(self):
        return self.get('type', 'any')

    @type.setter  # type: ignore
    def type(self, value):
        self['type'] = value

    @helpers.cached
    def format(self):
        format = self.get('format', 'default')
        if format.startswith('fmt:'):
            warnings.warn(
                'Format "fmt:<PATTERN>" is deprecated. '
                'Please use "<PATTERN>" without "fmt:" prefix.',
                UserWarning,
            )
            format = format.replace('fmt:', '')
        return format

    @format.setter  # type: ignore
    def format(self, value):
        self['format'] = value

    @helpers.cached
    def missing_values(self):
        schema = self.__metadata_schema
        default = schema.missing_values if schema else config.MISSING_VALUES
        missing_values = self.get('missingValues', default)
        return self.metadata_transorm_bind('missingValues', missing_values)

    @missing_values.setter  # type: ignore
    def missing_values(self, value):
        self['missingValues'] = value

    @helpers.cached
    def constraints(self):
        constraints = self.get('constraints', {})
        return self.metadata_transorm_bind('constraints', constraints)

    @constraints.setter  # type: ignore
    def constraints(self, value):
        self['constraints'] = value

    @helpers.cached
    def required(self):
        return self.constraints.get('required', False)

    @required.setter  # type: ignore
    def required(self, value):
        self.constraints['required'] = value

    # Expand

    def expand(self):
        self.setdefault('name', 'field')
        self.setdefault('type', 'any')
        self.setdefault('format', 'default')

    # Read

    def read_cell(self, cell):
        """Read cell (cast)

        # Arguments
            cell (any): cell

        # Returns
            (any, OrderedDict): processed cell and dict of notes

        """
        notes = None
        if cell in self.missing_values:
            cell = None
        if cell is not None:
            cell = self.read_cell_cast(cell)
            if cell is None:
                notes = notes or OrderedDict()
                notes['type'] = f'type is "{self.type}/{self.format}"'
        if not notes:
            for name, check in self.read_cell_checks.items():
                if not check(cell):
                    notes = notes or OrderedDict()
                    notes[name] = f'constraint "{name}" is "{self.constraints[name]}"'
        return cell, notes

    def read_cell_cast(self, cell):
        """Read cell low-level (cast)

        # Arguments
            cell (any): cell

        # Returns
            any/None: processed cell or None if an error

        """
        return self.__proxy.read_cell_cast(cell)

    @helpers.cached
    def read_cell_checks(self):
        """Read cell low-level (cast)

        # Returns
            OrderedDict: dictionlary of check function by a constraint name

        """
        checks = OrderedDict()
        for name in self.__proxy.supported_constraints:
            constraint = self.constraints.get(name)
            if constraint is not None:
                if name in ['minimum', 'maximum']:
                    constraint = self.read_cell_cast(constraint)
                if name == 'enum':
                    constraint = list(map(self.read_cell_cast, constraint))
                checks[name] = partial(globals().get(f'check_{name}'), constraint)
        return checks

    # Write

    def write_cell(self, cell):
        """Write cell (cast)

        # Arguments
            cell (any): cell

        # Returns
            (any, OrderedDict): processed cell and dict of notes

        """
        notes = None
        if cell is None:
            cell = ''
        if cell is not None:
            cell = self.write_cell_cast(cell)
        if cell is None:
            notes = [self.type_error_note]
        return cell, notes

    def write_cell_cast(self, cell):
        """Write cell low-level (cast)

        # Arguments
            cell (any): cell

        # Returns
            any/None: processed cell or None if an error

        """
        return self.__proxy.write_cell_cast(cell)

    # Metadata

    def metadata_process(self):
        self.__proxy = None
        if type(self) is Field:
            pref = self.get('type', 'any')
            name = f'{pref.capitalize()}Field'
            module = importlib.import_module('frictionless.fields')
            self.__proxy = getattr(module, name, getattr(module, 'AnyField'))(self)
        super().metadata_process()

    def metadata_validate(self):
        if type(self) is Field:
            self.metadata_errors.clear()
            return self.metadata_errors.extend(self.__proxy.metadata_errors)
        super().metadata_validate()

        # Constraints
        for name in self.constraints.keys():
            if name not in self.supported_constraints:
                note = f'constraint "{name}" is not supported by type "{self.type}"'
                self.metadata_errors.append(errors.SchemaError(note=note))


# Internal


def check_required(constraint, cell):
    if not (constraint and cell is None):
        return True
    return False


def check_minLength(constraint, cell):
    if cell is None:
        return True
    if len(cell) >= constraint:
        return True
    return False


def check_maxLength(constraint, cell):
    if cell is None:
        return True
    if len(cell) <= constraint:
        return True
    return False


def check_minimum(constraint, cell):
    if cell is None:
        return True
    try:
        if cell >= constraint:
            return True
    except decimal.InvalidOperation:
        # For non-finite numbers NaN, INF and -INF
        # the constraint always is not satisfied
        return False
    return False


def check_maximum(constraint, cell):
    if cell is None:
        return True
    try:
        if cell <= constraint:
            return True
    except decimal.InvalidOperation:
        # For non-finite numbers NaN, INF and -INF
        # the constraint always is not satisfied
        return False
    return False


def check_pattern(constraint, cell):
    if cell is None:
        return True
    if not isinstance(constraint, COMPILED_RE):
        regex = re.compile('^{0}$'.format(constraint))
    else:
        regex = constraint
    match = regex.match(cell)
    if match:
        return True
    return False


def check_enum(constraint, cell):
    if cell is None:
        return True
    if cell in constraint:
        return True
    return False


COMPILED_RE = type(re.compile(""))
