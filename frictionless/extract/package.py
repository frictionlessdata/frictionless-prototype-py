from ..package import Package
from .. import helpers


def extract_package(source, *, process=None):

    # Create package
    package = Package(source)

    # Extract package
    result = {}
    for resource in package.resources:
        name = resource.name or helpers.detect_name(resource.path)
        if process:
            result[name] = []
            for row in resource.read_row_stream():
                result[name].append(process(row))
            continue
        result[name] = resource.read_rows()
    return result
