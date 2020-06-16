import re
import decimal
import warnings
import importlib
from functools import partial
from collections import OrderedDict
from cached_property import cached_property
from .metadata import ControlledMetadata
from . import helpers
from . import errors
from . import config


# TODO: reset cached properties on transform
class Field(ControlledMetadata):
    """Field representation

    # Arguments
        descriptor (str|dict): field descriptor
        strict (bool): if True it will raise a first validation error

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

    def __init__(self, descriptor, strict=False, schema=None):
        super().__init__(descriptor, strict=strict)
        self.__schema = schema

    @cached_property
    def schema(self):
        """Field schema

        # Returns
            str: field schema

        """
        return self.__schema

    @cached_property
    def name(self):
        """Field name

        # Returns
            str: field name

        """
        return self.get('name', 'field')

    @cached_property
    def type(self):
        """Field type

        # Returns
            str: field type

        """
        return self.get('type', 'any')

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
        default = self.__schema.missing_values if self.__schema else config.MISSING_VALUES
        return self.get('missingValues', default)

    @cached_property
    def constraints(self):
        """Field constraints

        # Returns
            dict: dict of field constraints

        """
        return self.get('constraints', {})

    @cached_property
    def required(self):
        """Whether field is required

        # Returns
            bool: true if required

        """
        return self.constraints.get('required', False)

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

    @cached_property
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
        helpers.reset_cached_properties(self)
        super().metadata_process()
        self.__proxy = None
        if type(self) is Field:
            pref = self.get('type', '')
            name = f'{pref.capitalize()}Field'
            module = importlib.import_module('frictionless.fields')
            self.__proxy = getattr(module, name, getattr(module, 'AnyField'))(self)


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
