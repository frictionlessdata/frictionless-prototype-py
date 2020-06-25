from copy import copy
from .. import config
from .. import helpers
from .. import exceptions
from ..row import Row
from ..table import Table
from ..system import system
from ..schema import Schema
from ..headers import Headers
from ..errors import Error, SchemaError
from ..report import Report, ReportTable


@Report.from_validate
def validate_table(
    source,
    *,
    # File
    scheme=None,
    format=None,
    hashing=None,
    encoding=None,
    compression=None,
    compression_path=None,
    control=None,
    dialect=None,
    # Table
    headers_row=config.HEADERS_ROW,
    headers_joiner=config.HEADERS_JOINER,
    pick_fields=None,
    skip_fields=None,
    limit_fields=None,
    offset_fields=None,
    pick_rows=None,
    skip_rows=None,
    limit_rows=None,
    offset_rows=None,
    # Schema
    schema=None,
    sync_schema=False,
    patch_schema=False,
    infer_type=None,
    infer_names=None,
    infer_volume=config.INFER_VOLUME,
    infer_confidence=config.INFER_CONFIDENCE,
    # Integrity
    size=None,
    hash=None,
    lookup=None,
    # Validation
    pick_errors=None,
    skip_errors=None,
    limit_errors=None,
    limit_memory=config.LIMIT_MEMORY,
    extra_checks=None,
):
    """Validate table

    # Arguments
        source (any)

        scheme? (str)
        format? (str)
        encoding? (str)
        compression? (str)

        headers_row? (int | int[])
        headers_joiner? (str)

        pick_fields? ((int | str)[])
        skip_fields? ((int | str)[])
        limit_fields? (int)
        offset_fields? (int)

        pick_rows? ((int | str)[])
        skip_rows? ((int | str)[])
        limit_rows? (int)
        offset_rows? (int)

        schema? (str | dict)
        sync_schema? (bool)
        patch_schema? (dict)
        infer_type? (str)
        infer_names? (str[])
        infer_volume? (int)
        infer_confidence? (float)

        dialect? (dict)

        control? (dict)

        size? (int)
        hash? (str)
        lookup? (dict)

        pick_errors? (str[])
        skip_errors? (str[])
        limit_errors? (int)
        extra_checks? (list)

    # Returns
        Report

    """

    # Prepare state
    checks = []
    exited = False
    partial = False
    row_number = 0
    task_errors = []
    timer = helpers.Timer()
    errors = Errors(
        pick_errors=pick_errors, skip_errors=skip_errors, limit_errors=limit_errors
    )

    # Create stream
    table = Table(
        source,
        scheme=scheme,
        format=format,
        encoding=encoding,
        compression=compression,
        headers=helpers.translate_headers(headers_row),
        multiline_headers_joiner=headers_joiner,
        pick_fields=helpers.translate_pick_fields(pick_fields),
        skip_fields=helpers.translate_skip_fields(skip_fields),
        limit_fields=limit_fields,
        offset_fields=offset_fields,
        pick_rows=helpers.translate_pick_rows(pick_rows),
        skip_rows=helpers.translate_skip_rows(skip_rows),
        limit_rows=limit_rows,
        offset_rows=offset_rows,
        sample_size=infer_volume,
        hashing_algorithm=helpers.parse_hashing_algorithm(hash),
        **helpers.translate_dialect(dialect or {}),
        **helpers.translate_control(control or {}),
    )

    # Open table
    try:
        table.open()
        if not table.sample:
            message = 'There are no rows available'
            raise exceptions.SourceError(message)
    except Exception as exception:
        errors.add(Error.from_exception(exception), force=True)
        exited = True

    # Create schema
    try:
        schema = Schema(schema)
    except exceptions.FrictionlessException as exception:
        errors.add(exception.error, force=True)
        schema = None
        exited = True

    # Prepare schema
    if not exited:

        # Infer schema
        if not schema.fields:
            schema.infer(
                table.sample,
                type=infer_type,
                names=infer_names or table.headers,
                confidence=infer_confidence,
            )

        # Sync schema
        if sync_schema:
            fields = []
            mapping = {field.get('name'): field for field in schema.fields}
            for name in table.headers:
                fields.append(mapping.get(name, {'name': name, 'type': 'any'}))
            schema.fields = fields

        # Patch schema
        if patch_schema:
            fields = patch_schema.pop('fields', {})
            schema.update(patch_schema)
            for field in schema.fields:
                field.update((fields.get(field.get('name'), {})))

        # Validate schema
        if schema.metadata_errors:
            for error in schema.metadata_errors:
                errors.add(error, force=True)
            schema = None
            exited = True

        # Confirm schema
        if schema and len(schema.field_names) != len(set(schema.field_names)):
            note = 'Schemas with duplicate field names are not supported'
            error = SchemaError(note=note)
            errors.add(error, force=True)
            schema = None
            exited = True

    # Create checks
    if not exited:
        items = []
        items.append('baseline')
        items.append(('integrity', {'size': size, 'hash': hash, 'lookup': lookup}))
        items.extend(extra_checks or [])
        create = system.create_check
        for item in items:
            p1, p2 = item if isinstance(item, (tuple, list)) else (item, None)
            check = p1(p2) if isinstance(p1, type) else create(p1, descriptor=p2)
            check.connect(stream=table, schema=schema)
            check.prepare()
            checks.append(check)
            errors.register(check)

    # Validate task
    if not exited:
        for check in checks.copy():
            for error in check.validate_task():
                task_errors.append(error)
                if check in checks:
                    checks.remove(check)

    # Validate headers
    if not exited:
        if table.headers:

            # Get headers
            headers = Headers(
                table.headers,
                fields=schema.fields,
                field_positions=table.field_positions,
            )

            # Validate headers
            for check in checks:
                for error in check.validate_headers(headers):
                    errors.add(error)

    # Validate rows
    if not exited:
        fields = schema.fields
        iterator = table.iter(extended=True)
        field_positions = table.field_positions
        if not field_positions:
            field_positions = list(range(1, len(schema.fields) + 1))
        while True:

            # Read cells
            try:
                row_position, _, cells = next(iterator)
            except StopIteration:
                break
            except Exception as exception:
                errors.add(Error.from_exception(exception), force=True)
                exited = True
                break

            # Create row
            row_number += 1
            row = Row(
                cells,
                fields=fields,
                field_positions=field_positions,
                row_position=row_position,
                row_number=row_number,
            )

            # Validate row
            for check in checks:
                for error in check.validate_row(row):
                    errors.add(error)

            # Limit errors
            if limit_errors and len(errors) >= limit_errors:
                partial = True
                break

            # Limit memory
            if limit_memory and not row_number % 100000:
                memory = helpers.get_current_memory_usage()
                if memory and memory > limit_memory:
                    error = Error(note=f'exceeded memory limit "{limit_memory}MB"')
                    raise exceptions.FrictionlessException(error)

    # Validate table
    if not exited:
        for check in checks:
            for error in check.validate_table():
                errors.add(error)

    # Return report
    time = timer.get_time()
    source = table.source if isinstance(table.source, str) else 'inline'
    return Report(
        time=time,
        errors=task_errors,
        tables=[
            ReportTable(
                time=time,
                scope=errors.scope,
                partial=partial,
                row_count=row_number,
                source=source,
                scheme=table.scheme,
                format=table.format,
                encoding=table.encoding,
                compression=table.compression,
                headers=table.headers,
                headers_row=headers_row,
                headers_joiner=headers_joiner,
                pick_fields=pick_fields,
                skip_fields=skip_fields,
                limit_fields=limit_fields,
                offset_fields=offset_fields,
                pick_rows=pick_rows,
                skip_rows=skip_rows,
                limit_rows=limit_rows,
                offset_rows=offset_rows,
                schema=copy(schema),
                dialect=table.dialect,
                errors=errors,
            )
        ],
    )


# Internal


# TODO: refactor add/force (make more explicit)
class Errors(list):
    def __init__(self, *, pick_errors=None, skip_errors=None, limit_errors=None):
        self.__pick_errors = set(pick_errors or [])
        self.__skip_errors = set(skip_errors or [])
        self.__limit_errors = limit_errors
        self.__scope = []

    @property
    def scope(self):
        return self.__scope

    def add(self, error, *, force=False):
        if not force:
            if self.__limit_errors:
                if len(self) >= self.__limit_errors:
                    return
            if not self.match(error):
                return
        self.append(error)

    def match(self, error):
        match = True
        if self.__pick_errors:
            match = False
            if error.code in self.__pick_errors:
                match = True
            if self.__pick_errors.intersection(error.tags):
                match = True
        if self.__skip_errors:
            match = True
            if error.code in self.__skip_errors:
                match = False
            if self.__skip_errors.intersection(error.tags):
                match = False
        return match

    def register(self, check):
        for error in check.possible_Errors:
            if not self.match(error):
                continue
            if error.code in self.__scope:
                continue
            self.__scope.append(error.code)
