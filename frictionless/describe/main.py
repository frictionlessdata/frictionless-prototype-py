from ..schema import Schema
from ..table import Table
from .. import config


# TODO: support source_type (table/resource/package)
# TODO: support detect_type
def describe(
    source,
    *,
    source_type=None,
    headers=None,
    expand=False,
    missing_values=None,
    infer_type=None,
    infer_names=None,
    infer_volume=config.DEFAULT_INFER_VOLUME,
    infer_confidence=config.DEFAULT_INFER_CONFIDENCE
):
    """Describe data source

    # Arguments
        source
        expand
        missing_values
        infer_type
        infer_names
        infer_volume
        infer_confidence

    """
    with Table(source, headers=headers, infer_volume=infer_volume) as table:
        schema = Schema.from_sample(
            table.sample,
            names=infer_names or table.headers,
            confidence=infer_confidence,
            missing_values=missing_values,
        )
        if expand:
            schema.expand()
        return schema
