from ..package import Package
from .. import helpers


def extract_package(source, *, stream=False, json=False):

    # Create package
    package = Package(source)

    # Extract package
    result = {}
    for resource in package.resources:
        name = resource.name or helpers.detect_name(resource.path)

        # Stream
        if stream:
            result[name] = resource.read_row_stream()

        # Json
        elif json:
            result[name] = []
            for row in resource.read_row_stream():
                result[name].append(row.to_dict(json=True))

        # Default
        else:
            result[name] = resource.read_rows()

    return result
