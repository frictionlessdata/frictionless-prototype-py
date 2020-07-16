import functools
from copy import deepcopy
from . import config
from . import helpers
from . import exceptions
from .metadata import Metadata
from .errors import Error, TaskError, ReportError


class Report(Metadata):
    """Report representation.

    # Arguments
        descriptor? (str|dict): schema descriptor
        time
        errors
        tables

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_strict = True
    metadata_Error = ReportError
    metadata_profile = deepcopy(config.REPORT_PROFILE)
    metadata_profile["properties"]["tables"] = {
        "type": "array",
        "items": {"type": "object"},
    }

    def __init__(self, descriptor=None, *, time, errors, tables):
        self["version"] = config.VERSION
        self["time"] = time
        self["valid"] = not errors and all(tab["valid"] for tab in tables)
        self["stats"] = {
            "errors": len(errors) + sum(tab["stats"]["errors"] for tab in tables),
            "tables": len(tables),
        }
        self["errors"] = errors
        self["tables"] = tables
        super().__init__(descriptor)

    @property
    def version(self):
        return self["version"]

    @property
    def time(self):
        return self["time"]

    @property
    def valid(self):
        return self["valid"]

    @property
    def stats(self):
        return self["stats"]

    @property
    def errors(self):
        return self["errors"]

    @property
    def tables(self):
        return self["tables"]

    @property
    def table(self):
        if len(self.tables) != 1:
            error = Error(note='The "report.table" is available for single table reports')
            raise exceptions.FrictionlessException(error)
        return self.tables[0]

    # Expand

    def expand(self):
        for table in self.tables:
            table.expand()

    # Flatten

    def flatten(self, spec):
        """Flatten the report

        # Arguments
            spec

        """

        result = []
        for error in self.errors:
            context = {}
            context.update(error)
            result.append([context.get(prop) for prop in spec])
        for count, table in enumerate(self.tables, start=1):
            for error in table.errors:
                context = {"tableNumber": count, "tablePosition": count}
                context.update(error)
                result.append([context.get(prop) for prop in spec])
        return result

    # Import/Export

    @staticmethod
    def from_validate(validate):
        @functools.wraps(validate)
        def wrapper(*args, **kwargs):
            timer = helpers.Timer()
            try:
                return validate(*args, **kwargs)
            except Exception as exception:
                error = TaskError(note=str(exception))
                if isinstance(exception, exceptions.FrictionlessException):
                    error = exception.error
                return Report(time=timer.time, errors=[error], tables=[])

        return wrapper

    def to_dict(self, expand=False):
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result

    # Metadata

    def metadata_validate(self):
        yield from super().metadata_validate()

        # Tables
        for table in self.tables:
            yield from table.metadata_errors


class ReportTable(Metadata):
    """Report table representation.

    # Arguments
        descriptor? (str|dict): schema descriptor
        time
        scope
        partial
        row_count
        path
        scheme
        format
        encoding
        compression
        headers
        headers_row
        headers_joiner
        pick_fields
        skip_fields
        limit_fields
        offset_fields
        pick_rows
        skip_rows
        limit_rows
        offset_rows
        schema
        dialect
        errors

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_strict = True
    metadata_Error = ReportError
    metadata_profile = config.REPORT_PROFILE["properties"]["tables"]["items"]

    def __init__(self, descriptor=None, *, time, scope, partial, errors, table):
        # File
        self["path"] = table.path
        self["scheme"] = table.scheme
        self["format"] = table.format
        self["hashing"] = table.hashing
        self["encoding"] = table.encoding
        self["compression"] = table.compression
        self["compressionPath"] = table.compression_path
        self["dialect"] = table.dialect
        self["query"] = table.query
        # Table
        self.setinitial("schema", table.schema)
        self.setinitial("headers", table.headers)
        # Validation
        self["time"] = time
        self["valid"] = not errors
        self["scope"] = scope
        self["stats"] = helpers.copy_merge(table.stats, {"errors": len(errors)})
        self["partial"] = partial
        self["errors"] = errors
        super().__init__(descriptor)

    @property
    def path(self):
        return self["path"]

    @property
    def scheme(self):
        return self["scheme"]

    @property
    def format(self):
        return self["format"]

    @property
    def hashing(self):
        return self["hashing"]

    @property
    def encoding(self):
        return self["encoding"]

    @property
    def compression(self):
        return self["compression"]

    @property
    def compression_path(self):
        return self["compressionPath"]

    @property
    def dialect(self):
        return self["dialect"]

    @property
    def query(self):
        return self["query"]

    @property
    def schema(self):
        return self["schema"]

    @property
    def headers(self):
        return self["headers"]

    @property
    def time(self):
        return self["time"]

    @property
    def valid(self):
        return self["valid"]

    @property
    def scope(self):
        return self["scope"]

    @property
    def stats(self):
        return self["stats"]

    @property
    def partial(self):
        return self["partial"]

    @property
    def errors(self):
        return self["errors"]

    @property
    def error(self):
        if len(self.errors) != 1:
            error = Error(note='The "table.error" is available for single error tables')
            raise exceptions.FrictionlessException(error)
        return self.errors[0]

    # Expand

    def expand(self):
        self.dialect.expand()
        if self.schema is not None:
            self.schema.expand()

    # Flatten

    def flatten(self, spec):
        """Flatten the report table

        # Arguments
            spec

        """

        result = []
        for error in self.errors:
            context = {}
            context.update(error)
            result.append([context.get(prop) for prop in spec])
        return result

    # Import/Export

    def to_dict(self, expand=False):
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result
