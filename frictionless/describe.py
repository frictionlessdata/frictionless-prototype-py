from .schema import Schema
from .table import Table
from . import config


# TODO: support source_type (table/resource/package)
# TODO: support infer_type
def describe(
    source,
    *,
    source_type=None,
    headers_row=config.HEADERS_ROW,
    expand=False,
    missing_values=None,
    infer_type=None,
    infer_names=None,
    infer_sample=config.INFER_SAMPLE,
    infer_confidence=config.INFER_CONFIDENCE
):
    """Describe data source

    # Arguments
        source
        expand
        missing_values
        infer_type
        infer_names
        infer_sample
        infer_confidence

    """
    with Table(source, headers=headers_row, sample_size=infer_sample) as table:
        schema = Schema.from_sample(
            table.sample,
            names=infer_names or table.headers,
            confidence=infer_confidence,
            missing_values=missing_values,
        )
        if expand:
            schema.expand()
        return schema
