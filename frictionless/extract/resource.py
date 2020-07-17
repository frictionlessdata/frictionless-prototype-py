from ..resource import Resource


def extract_resource(source, *, stream=False, json=False):

    # Create resource
    resource = Resource(source)

    # Stream
    if stream:
        return resource.read_row_stream()

    # Json
    elif json:
        result = []
        for row in resource.read_row_stream():
            result.append(row.to_dict(json=True))
        return result

    # Default
    return resource.read_rows()
