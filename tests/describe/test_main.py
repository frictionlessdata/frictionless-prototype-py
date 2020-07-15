from frictionless import describe, Resource, Package


# General


def test_describe():
    resource = describe("data/table.csv")
    assert resource.metadata_valid
    assert resource == {
        "name": "table",
        "path": "data/table.csv",
        "scheme": "file",
        "format": "csv",
        "hashing": "md5",
        "encoding": "utf-8",
        "compression": "no",
        "compressionPath": "",
        "dialect": {},
        "schema": {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ]
        },
        "profile": "tabular-data-resource",
        "hash": "6c2c61dd9b0e9c6876139a449ed87933",
        "bytes": 30,
        "rows": 2,
    }


def test_describe_resource():
    resource = describe("data/table.csv")
    assert isinstance(resource, Resource)


def test_describe_package():
    resource = describe(["data/table.csv"])
    assert isinstance(resource, Package)


def test_describe_package_pattern():
    resource = describe("data/chunk*.csv")
    assert isinstance(resource, Package)


def test_describe_package_source_type():
    resource = describe("data/table.csv", source_type="package")
    assert isinstance(resource, Package)
