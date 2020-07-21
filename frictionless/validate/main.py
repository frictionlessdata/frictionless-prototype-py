import os
import glob
from pathlib import Path
from importlib import import_module
from ..package import Package
from ..report import Report
from .. import helpers


@Report.from_validate
def validate(source, source_type=None, **options):
    module = import_module("frictionless.validate")

    # Normalize source
    # NOTE: move to lower-levels
    if isinstance(source, Path):
        source = str(source)

    # Detect source type
    # NOTE: move to helpers
    if not source_type:
        if source and isinstance(source, list) and isinstance(source[0], str):
            package = Package()
            package.infer(source)
            source = package
            source_type = "package"
        if isinstance(source, str):
            if glob.has_magic(source):
                package = Package()
                package.infer(source)
                source = package
                source_type = "package"
            elif os.path.isdir(source):
                package = Package()
                package.infer(f"{source}/*")
                source = package
                source_type = "package"
        if not source_type:
            source_type = helpers.detect_source_type(source)

    # Validate source
    validate = getattr(module, "validate_%s" % source_type)
    return validate(source, **options)
