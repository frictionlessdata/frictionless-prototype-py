from ..resource import Resource


def extract_resource(source, *, process=None):

    # Create resource
    resource = Resource(source)

    # Extract resource
    if process:
        result = []
        for row in resource.read_row_stream():
            result.append(process(row))
        return result
    return resource.read_rows()
