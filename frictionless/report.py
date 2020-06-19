import functools
from . import config
from . import helpers
from . import exceptions
from .errors import TaskError
from .metadata import Metadata


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

    metadata_profile = config.REPORT_PROFILE

    def __init__(self, descriptor=None, *, time, errors, tables):
        self['time'] = time
        self['valid'] = not errors and all(tab['valid'] for tab in tables)
        self['version'] = config.VERSION
        self['tableCount'] = len(tables)
        self['errorCount'] = len(errors) + sum(tab['errorCount'] for tab in tables)
        self['errors'] = errors
        self['tables'] = tables
        super().__init__(descriptor)

    @property
    def time(self):
        return self['time']

    @property
    def valid(self):
        return self['valid']

    @property
    def version(self):
        return self['version']

    @property
    def table_count(self):
        return self['tableCount']

    @property
    def error_count(self):
        return self['errorCount']

    @property
    def errors(self):
        return self['errors']

    @property
    def tables(self):
        return self['tables']

    @property
    def table(self):
        if len(self.tables) != 1:
            message = 'The "report.table" is only available for a single table reports'
            raise exceptions.FrictionlessException(message)
        return self.tables[0]

    # Create

    @staticmethod
    def from_validate(validate):
        @functools.wraps(validate)
        def wrapper(*args, **kwargs):
            timer = helpers.Timer()
            try:
                return validate(*args, **kwargs)
            except Exception as exception:
                time = timer.get_time()
                error = TaskError(note=str(exception))
                return Report(time=time, errors=[error], tables=[])

        return wrapper

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
                context = {'tableNumber': count, 'tablePosition': count}
                context.update(error)
                result.append([context.get(prop) for prop in spec])
        return result


class ReportTable(Metadata):
    """Report table representation.

    # Arguments
        descriptor? (str|dict): schema descriptor
        time
        scope
        partial
        row_count
        source
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

    def __init__(
        self,
        descriptor=None,
        *,
        time,
        scope,
        partial,
        row_count,
        source,
        scheme,
        format,
        encoding,
        compression,
        headers,
        headers_row,
        headers_joiner,
        pick_fields,
        skip_fields,
        limit_fields,
        offset_fields,
        pick_rows,
        skip_rows,
        limit_rows,
        offset_rows,
        schema,
        dialect,
        errors,
    ):
        self['time'] = time
        self['valid'] = not errors
        self['scope'] = scope
        self['partial'] = partial
        self['rowCount'] = row_count
        self['errorCount'] = len(errors)
        self['source'] = source
        self['scheme'] = scheme
        self['format'] = format
        self['encoding'] = encoding
        self['compression'] = compression
        self['headers'] = headers
        self['headersRow'] = headers_row
        self['headersJoiner'] = headers_joiner
        self['pickFields'] = pick_fields
        self['skipFields'] = skip_fields
        self['limitFields'] = limit_fields
        self['offsetFields'] = offset_fields
        self['pickRows'] = pick_rows
        self['skipRows'] = skip_rows
        self['limitRows'] = limit_rows
        self['offsetRows'] = offset_rows
        self['schema'] = schema
        self['dialect'] = dialect
        self['errors'] = errors
        super().__init__(descriptor)

    @property
    def time(self):
        return self['time']

    @property
    def valid(self):
        return self['valid']

    @property
    def scope(self):
        return self['scope']

    @property
    def partial(self):
        return self['partial']

    @property
    def row_count(self):
        return self['rowCount']

    @property
    def error_count(self):
        return self['errorCount']

    @property
    def errors(self):
        return self['errors']

    @property
    def error(self):
        if len(self.errors) != 1:
            message = 'The "table.error" is only available for a single error tables'
            raise exceptions.FrictionlessException(message)
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
