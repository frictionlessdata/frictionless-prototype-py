import datapackage
from .. import helpers
from ..report import Report
from ..errors import ResourceError
from .table import validate_table


@Report.from_validate
def validate_resource(source, base_path=None, exact=False, lookup=None, **options):
    """Validate resource
    """

    # Prepare state
    timer = helpers.Timer()

    # Create resource
    try:
        resource = datapackage.Resource(source, base_path=base_path)
    except datapackage.exceptions.DataPackageException as exception:
        time = timer.get_time()
        error = ResourceError(note=str(exception))
        return Report(time=time, errors=[error], tables=[])

    # Prepare resource
    for stage in [1, 2]:
        errors = []
        if stage == 1:
            if not exact:
                continue
        if stage == 2:
            try:
                resource.infer()
            except Exception as exception:
                errors.append(ResourceError(note=str(exception)))
        if not resource.tabular:
            errors.append(ResourceError(note='resource is not tabular'))
        for error in resource.errors:
            errors.append(ResourceError(note=str(error)))
        if errors:
            time = timer.get_time()
            return Report(time=time, errors=errors, tables=[])

    # Prepare table
    source = resource.source
    if resource.multipart:
        source = resource.raw_iter(stream=True)
    headers_row = 1
    dialect = resource.descriptor.get('dialect', {})
    if dialect.get('header') is False:
        headers_row = None
    if lookup is None:
        lookup = helpers.create_lookup(resource)
    stats = None
    hashing = None
    stats_hash = resource.descriptor.get('hash')
    stats_bytes = resource.descriptor.get('bytes')
    if stats_hash:
        stats = stats or {}
        stats['hash'] = helpers.parse_hashing_digest(stats_hash)
        hashing = helpers.parse_hashing_algorithm(stats_hash)
    if stats_bytes:
        stats = stats or {}
        stats['bytes'] = stats_bytes

    # Validate table
    report = validate_table(
        source,
        scheme=resource.descriptor.get('scheme'),
        format=resource.descriptor.get('format'),
        hashing=hashing,
        encoding=resource.descriptor.get('encoding'),
        compression=resource.descriptor.get('compression'),
        dialect=dialect,
        headers_row=headers_row,
        pick_fields=resource.descriptor.get('pickFields'),
        skip_fields=resource.descriptor.get('skipFields'),
        limit_fields=resource.descriptor.get('limitFields'),
        offset_fields=resource.descriptor.get('offsetFields'),
        pick_rows=resource.descriptor.get('pickRows'),
        skip_rows=resource.descriptor.get('skipRows'),
        limit_rows=resource.descriptor.get('limitRows'),
        offset_rows=resource.descriptor.get('offsetRows'),
        schema=resource.descriptor.get('schema'),
        stats=stats,
        lookup=lookup,
        **options,
    )

    # Return report
    time = timer.get_time()
    return Report(time=time, errors=report['errors'], tables=report['tables'])
