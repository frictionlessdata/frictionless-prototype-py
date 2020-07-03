from .. import config
from .. import helpers
from .. import exceptions
from ..table import Table
from ..errors import Error
from ..system import system
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
    headers=config.DEFAULT_HEADERS_ROW,
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
    infer_volume=config.DEFAULT_INFER_VOLUME,
    infer_confidence=config.DEFAULT_INFER_CONFIDENCE,
    # Validation
    stats=None,
    lookup=None,
    pick_errors=None,
    skip_errors=None,
    limit_errors=None,
    limit_memory=config.DEFAULT_LIMIT_MEMORY,
    extra_checks=None,
):
    """Validate table

    # Arguments
        source (any)

        scheme? (str)
        format? (str)
        hashing? (str)
        encoding? (str)
        compression? (str)
        compression_path? (str)
        dialect? (dict)
        control? (dict)

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

        stats? (dict)
        lookup? (dict)

        pick_errors? (str[])
        skip_errors? (str[])
        limit_errors? (int)
        extra_checks? (list)

    # Returns
        Report

    """

    # Create state
    checks = []
    partial = False
    task_errors = []
    table_errors = TableErrors(pick_errors, skip_errors, limit_errors)
    timer = helpers.Timer()

    # Create table
    table = Table(
        source,
        # File
        scheme=scheme,
        format=format,
        hashing=hashing,
        encoding=encoding,
        compression=compression,
        compression_path=compression_path,
        control=control,
        dialect=dialect,
        # Table
        headers=headers,
        pick_fields=pick_fields,
        skip_fields=skip_fields,
        limit_fields=limit_fields,
        offset_fields=offset_fields,
        pick_rows=pick_rows,
        skip_rows=skip_rows,
        limit_rows=limit_rows,
        offset_rows=offset_rows,
        # Schema
        schema=schema,
        sync_schema=sync_schema,
        patch_schema=patch_schema,
        infer_type=infer_type,
        infer_names=infer_names,
        infer_volume=infer_volume,
        infer_confidence=infer_confidence,
    )

    # Create checks
    items = []
    items.append("baseline")
    items.append(("integrity", {"stats": stats, "lookup": lookup}))
    items.extend(extra_checks or [])
    create = system.create_check
    for item in items:
        p1, p2 = item if isinstance(item, (tuple, list)) else (item, None)
        check = p1(p2) if isinstance(p1, type) else create(p1, descriptor=p2)
        checks.append(check)

    # Open table
    with table:

        # Prepare checks
        for check in checks:
            table_errors.register(check)
            check.connect(table)
            check.prepare()

        # Validate task
        for check in checks.copy():
            for error in check.validate_task():
                task_errors.append(error)
                if check in checks:
                    checks.remove(check)

        # Validate headers
        if table.headers:
            for check in checks:
                for error in check.validate_headers(table.headers):
                    table_errors.append(error)

        # Iterate rows
        for row in table.row_stream:

            # Validate row
            for check in checks:
                for error in check.validate_row(row):
                    table_errors.append(error)

            # Limit errors
            if limit_errors and len(table_errors) >= limit_errors:
                partial = True
                break

            # Limit memory
            if limit_memory and not table.stats["rows"] % 100000:
                memory = helpers.get_current_memory_usage()
                if memory and memory > limit_memory:
                    error = Error(note=f'exceeded memory limit "{limit_memory}MB"')
                    raise exceptions.FrictionlessException(error)

        # Validate table
        for check in checks:
            for error in check.validate_table():
                table_errors.append(error)

    # Return report
    # TODO: copy metadata here or in ReportTable
    return Report(
        time=timer.time,
        errors=task_errors,
        tables=[
            ReportTable(
                # File
                path=table.path,
                scheme=table.scheme,
                format=table.format,
                hashing=table.hashing,
                encoding=table.encoding,
                compression=table.compression,
                compression_path=table.compression_path,
                dialect=table.dialect,
                # Table
                headers=table.headers,
                pick_fields=pick_fields,
                skip_fields=skip_fields,
                limit_fields=limit_fields,
                offset_fields=offset_fields,
                pick_rows=pick_rows,
                skip_rows=skip_rows,
                limit_rows=limit_rows,
                offset_rows=offset_rows,
                # Schema
                schema=table.schema,
                # Validation
                time=timer.time,
                scope=table_errors.scope,
                stats=table.stats,
                partial=partial,
                errors=table_errors,
            )
        ],
    )


# Internal


class TableErrors(list):
    def __init__(self, pick_errors, skip_errors, limit_errors):
        self.__pick_errors = set(pick_errors or [])
        self.__skip_errors = set(skip_errors or [])
        self.__limit_errors = limit_errors
        self.__scope = []

    @property
    def scope(self):
        return self.__scope

    def append(self, error, *, force=False):
        if not force:
            if self.__limit_errors:
                if len(self) >= self.__limit_errors:
                    return
            if not self.match(error):
                return
        super().append(error)

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
