import glob
from pathlib import Path
from importlib import import_module
from ..package import Package
from .. import helpers


def extract(source, *, source_type=None, process=None, **options):
    module = import_module("frictionless.extract")

    # Normalize source
    if isinstance(source, Path):
        source = str(source)

    # Detect source type
    if not source_type:
        if isinstance(source, list) or glob.has_magic(source):
            package = Package()
            package.infer(source)
            source = package
        source_type = helpers.detect_source_type(source)

    # Extract source
    extract = getattr(module, "extract_%s" % source_type)
    return extract(source, process=process, **options)
