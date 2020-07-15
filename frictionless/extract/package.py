from ..package import Package
from .. import helpers


def extract_package(source, *, stream=False):

    # Create package
    package = Package(source)

    # Extract package
    result = {}
    for resource in package.resources:
        name = resource.name or helpers.detect_name(resource.path)
        result[name] = resource.read_row_stream() if stream else resource.read_rows()
    return result
