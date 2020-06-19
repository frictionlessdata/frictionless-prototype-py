from tabulator import Stream
from .schema import Schema
from . import config


# TODO: support source_type (table/resource/package)
def describe(
    source,
    *,
    expand=False,
    missing_values=None,
    infer_type=None,
    infer_names=None,
    infer_sample=config.INFER_SAMPLE,
    infer_confidence=config.INFER_CONFIDENCE
):
    """Describe data source
    """
    with Stream(source, headers=1, sample_size=infer_sample) as stream:
        schema = Schema.from_sample(
            stream.sample,
            names=infer_names or stream.headers,
            confidence=infer_confidence,
            missing_values=missing_values,
        )
        if expand:
            schema.expand()
        return schema
