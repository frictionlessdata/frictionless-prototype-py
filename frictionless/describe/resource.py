from ..resource import Resource
from ..table import Table
from .. import helpers
from .. import config


def describe_resource(
    source,
    *,
    headers=None,
    # File
    scheme=None,
    format=None,
    hashing=None,
    encoding=None,
    compression=None,
    compression_path=None,
    control=None,
    dialect=None,
    query=None,
    # Schema
    infer_type=None,
    infer_names=None,
    infer_volume=config.DEFAULT_INFER_VOLUME,
    infer_confidence=config.DEFAULT_INFER_CONFIDENCE,
    infer_missing_values=config.DEFAULT_MISSING_VALUES,
    # Description
    expand=False,
):

    # Create table
    table = Table(
        source,
        headers=headers,
        # File
        scheme=scheme,
        format=format,
        hashing=hashing,
        encoding=encoding,
        compression=compression,
        compression_path=compression_path,
        control=control,
        dialect=dialect,
        query=query,
        # Schema
        infer_type=infer_type,
        infer_names=infer_names,
        infer_volume=infer_volume,
        infer_confidence=infer_confidence,
        infer_missing_values=infer_missing_values,
    )

    # Create resource
    with table as table:
        helpers.pass_through(table.data_stream)
        resource = Resource(
            name=helpers.detect_name(table.path),
            path=table.path,
            scheme=table.scheme,
            format=table.format,
            hashing=table.hashing,
            encoding=table.encoding,
            compression=table.compression,
            compression_path=table.compression_path,
            dialect=table.dialect,
            schema=table.schema,
            profile="tabular-data-resource",
        )

    # Inline resource
    if not isinstance(table.source, str):
        resource.data = table.source

    # Stats resource
    resource.update(table.stats)
    if resource["hashing"] != config.DEFAULT_HASHING:
        resource["hash"] = ":".join([resource["hashing"], resource["hash"]])

    # Expand resource
    if expand:
        resource.expand()

    return resource
