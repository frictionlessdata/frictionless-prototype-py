from ..table import Table
from .. import config


def extract_table(
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
    schema=None,
    sync_schema=False,
    patch_schema=False,
    infer_type=None,
    infer_names=None,
    infer_volume=config.DEFAULT_INFER_VOLUME,
    infer_confidence=config.DEFAULT_INFER_CONFIDENCE,
    infer_missing_values=config.DEFAULT_MISSING_VALUES,
    lookup=None,
    # Extraction
    stream=False,
    json=False,
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
        schema=schema,
        sync_schema=sync_schema,
        patch_schema=patch_schema,
        infer_type=infer_type,
        infer_names=infer_names,
        infer_volume=infer_volume,
        infer_confidence=infer_confidence,
        lookup=lookup,
    )

    # Extract table
    with table as table:

        # Stream
        if stream:
            return table.row_stream

        # Json
        elif json:
            result = []
            for row in table.row_stream:
                result.append(row.to_dict(json=True))
            return result

        # Default
        return table.read_rows()
