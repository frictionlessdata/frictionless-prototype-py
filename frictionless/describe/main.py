import glob
from pathlib import Path
from importlib import import_module


def describe(source, *, source_type=None, **options):
    module = import_module("frictionless.describe")

    # Normalize source
    if isinstance(source, Path):
        source = str(source)

    # Detect source type
    if not source_type:
        source_type = "resource"
        if isinstance(source, list):
            if source and isinstance(source[0], str):
                source_type = "package"
        elif glob.has_magic(source):
            source_type = "package"

    # Describe source
    describe = getattr(module, "describe_%s" % source_type)
    return describe(source, **options)
