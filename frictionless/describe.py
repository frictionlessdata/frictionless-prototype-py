from tabulator import Stream
from .schema import Schema
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
    with Stream(source, headers=headers_row, sample_size=infer_sample) as stream:
        schema = Schema.from_sample(
            stream.sample,
            names=infer_names or stream.headers,
            confidence=infer_confidence,
            missing_values=missing_values,
        )
        if expand:
            schema.expand()
        return schema
