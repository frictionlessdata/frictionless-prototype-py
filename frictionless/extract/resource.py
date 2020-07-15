from ..resource import Resource


def extract_resource(source, *, stream=False):

    # Create resource
    resource = Resource(source)

    # Extract resource
    if stream:
        return resource.read_row_stream()
    return resource.read_rows()
