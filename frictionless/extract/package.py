from collections import OrderedDict
from ..package import Package


def extract_package(source, *, process=None):

    # Create package
    package = Package(source)

    # Extract package
    result = OrderedDict()
    for number, resource in enumerate(package.resources, start=1):
        key = resource.fullpath if not resource.inline else f"memory{number}"
        if process:
            result[key] = []
            for row in resource.read_row_stream():
                result[key].append(process(row))
            continue
        result[key] = resource.read_rows()
    return result
