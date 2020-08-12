from .metadata import Metadata
from . import errors


class Check(Metadata):
    """Check representation.

    Parameters:
        descriptor? (str|dict): schema descriptor

    Raises:
        FrictionlessException: raise if metadata is invalid

    """

    def __init__(self, descriptor=None):
        super().__init__(descriptor)

    @property
    def table(self):
        """
        Returns:
            Table?: table object available after the `check.connect` call
        """
        return self.__table

    # Validation

    def connect(self, table):
        """Connect to the given table

        Parameters:
            table (Table): data table
        """
        self.__table = table

    def prepare(self):
        """Called before validation
        """
        pass

    def validate_task(self):
        """Called to validate the check itself

        Yields:
            Error: found errors
        """
        yield from []

    def validate_schema(self, schema):
        """Called to validate the given schema

        Parameters:
            schema (Schema): table schema

        Yields:
            Error: found errors
        """
        yield from []

    def validate_headers(self, headers):
        """Called to validate the given headers

        Parameters:
            headers (Headers): table headers

        Yields:
            Error: found errors
        """
        yield from []

    def validate_row(self, row):
        """Called to validate the given row (on every row)

        Parameters:
            row (Row): table row

        Yields:
            Error: found errors
        """
        yield from []

    def validate_table(self):
        """Called to validate the table (after no rows left)

        Yields:
            Error: found errors
        """
        yield from []

    # Metadata

    metadata_strict = True
    metadata_Error = errors.CheckError
    possible_Errors = []  # type: ignore
