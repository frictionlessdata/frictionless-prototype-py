from pathlib import Path
from importlib import import_module
from .. import helpers


def extract(source, source_type=None, **options):
    module = import_module("frictionless.extract")

    # Normalize source
    if isinstance(source, Path):
        source = str(source)

    # Detect source type
    if not source_type:
        source_type = helpers.detect_source_type(source)

    # Validate source
    extract = getattr(module, "extract_%s" % source_type)
    return extract(source, **options)
