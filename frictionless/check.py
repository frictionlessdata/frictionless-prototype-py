from .metadata import Metadata
from . import errors


class Check(Metadata):
    """Check representation.

    # Arguments
        descriptor? (str|dict): schema descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_strict = True
    metadata_Error = errors.CheckError
    possible_Errors = []  # type: ignore

    def __init__(self, descriptor=None):
        super().__init__(descriptor)

    @property
    def table(self):
        return self.__table

    # Validation

    def connect(self, table):
        self.__table = table

    def prepare(self):
        pass

    def validate_task(self):
        yield from []

    def validate_schema(self, schema):
        yield from []

    def validate_headers(self, headers):
        yield from []

    def validate_row(self, row):
        yield from []

    def validate_table(self):
        yield from []
