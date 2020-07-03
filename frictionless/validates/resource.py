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
        error = ResourceError(note=str(exception))
        return Report(time=timer.time, errors=[error], tables=[])

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
            errors.append(ResourceError(note="resource is not tabular"))
        for error in resource.errors:
            errors.append(ResourceError(note=str(error)))
        if errors:
            return Report(time=timer.time, errors=errors, tables=[])

    # Prepare table
    # TODO: rework
    source = resource.source
    if resource.multipart:
        source = resource.raw_iter(stream=True)
    headers = 1
    dialect = resource.descriptor.get("dialect", {})
    if dialect.get("header") is False:
        headers = None
    if lookup is None:
        lookup = helpers.create_lookup(resource)
    stats = None
    hashing = None
    stats_hash = resource.descriptor.get("hash")
    stats_bytes = resource.descriptor.get("bytes")
    if stats_hash:
        stats = stats or {}
        stats["hash"] = helpers.parse_hashing_digest(stats_hash)
        hashing = helpers.parse_hashing_algorithm(stats_hash)
    if stats_bytes:
        stats = stats or {}
        stats["bytes"] = stats_bytes

    # Validate table
    report = validate_table(
        source,
        scheme=resource.descriptor.get("scheme"),
        format=resource.descriptor.get("format"),
        hashing=hashing,
        encoding=resource.descriptor.get("encoding"),
        compression=resource.descriptor.get("compression"),
        compression_path=resource.descriptor.get("compressionPath"),
        dialect=dialect,
        headers=headers,
        schema=resource.descriptor.get("schema"),
        stats=stats,
        lookup=lookup,
        **options,
    )

    # Return report
    return Report(time=timer.time, errors=report["errors"], tables=report["tables"])
