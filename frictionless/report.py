import functools
from . import config
from . import helpers
from . import exceptions
from .metadata import Metadata
from .errors import Error, TaskError


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
    metadata_profile = config.REPORT_PROFILE

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

    # Save

    def save(self, target):
        self.metadata_save(target)

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

    def to_dict(self):
        return self.copy()


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
    metadata_profile = config.REPORT_PROFILE["properties"]["tables"]["items"]

    def __init__(
        self,
        descriptor=None,
        *,
        # File
        path,
        scheme,
        format,
        hashing,
        encoding,
        compression,
        compression_path,
        # Table
        dialect,
        headers,
        schema,
        # Discovery
        pick_fields,
        skip_fields,
        limit_fields,
        offset_fields,
        pick_rows,
        skip_rows,
        limit_rows,
        offset_rows,
        # Validation
        time,
        scope,
        stats,
        partial,
        errors,
    ):
        # File
        self["path"] = path
        self["scheme"] = scheme
        self["format"] = format
        self["hashing"] = hashing
        self["encoding"] = encoding
        self["compression"] = compression
        self["compressionPath"] = compression_path
        # Table
        self.setinitial("dialect", dialect)
        self.setinitial("headers", headers)
        self.setinitial("schema", schema)
        # Discovery
        self.setinitial("pickFields", pick_fields)
        self.setinitial("skipFields", skip_fields)
        self.setinitial("limitFields", limit_fields)
        self.setinitial("offsetFields", offset_fields)
        self.setinitial("pickRows", pick_rows)
        self.setinitial("skipRows", skip_rows)
        self.setinitial("limitRows", limit_rows)
        self.setinitial("offsetRows", offset_rows)
        # Validation
        self["time"] = time
        self["valid"] = not errors
        self["scope"] = scope
        self["stats"] = helpers.copy_merge(stats, {"errors": len(errors)})
        self["partial"] = partial
        self["errors"] = errors
        super().__init__(descriptor)

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

    # Save

    def save(self, target):
        self.metadata_save(target)

    # Import/Export

    def to_dict(self):
        return self.copy()
