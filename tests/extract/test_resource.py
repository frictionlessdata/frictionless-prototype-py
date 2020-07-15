from frictionless import extract


# General


def test_extract_resource():
    assert extract("data/resource.json") == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_extract_resource_stream():
    row_stream = extract("data/resource.json", stream=True)
    assert isinstance(row_stream, list) is False
    assert list(row_stream) == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]
