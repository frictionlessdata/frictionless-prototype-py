from pathlib import Path
from importlib import import_module
from ..report import Report
from .. import helpers


@Report.from_validate
def validate(source, source_type=None, **options):
    module = import_module("frictionless.validate")

    # Normalize source
    if isinstance(source, Path):
        source = str(source)

    # Detect source type
    if not source_type:
        source_type = helpers.detect_source_type(source)

    # Validate source
    validate = getattr(module, "validate_%s" % source_type)
    return validate(source, **options)
