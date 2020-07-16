from frictionless import describe


# General


def test_describe_resource():
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


def test_describe_resource_schema():
    resource = describe("data/table-infer.csv")
    assert resource.schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_resource_schema_utf8():
    resource = describe("data/table-infer-utf8.csv")
    assert resource.schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_resource_schema_expand():
    resource = describe("data/table-infer.csv", expand=True)
    assert resource.schema == {
        "fields": [
            {"name": "id", "type": "integer", "format": "default", "bareNumber": True},
            {"name": "age", "type": "integer", "format": "default", "bareNumber": True},
            {"name": "name", "type": "string", "format": "default"},
        ],
        "missingValues": [""],
    }


def test_describe_resource_schema_infer_volume():
    resource = describe("data/table-infer-row-limit.csv", infer_volume=4)
    assert resource.schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_resource_schema_with_missing_values_default():
    resource = describe("data/table-infer-missing-values.csv")
    assert resource.schema == {
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_resource_schema_with_missing_values_using_the_argument():
    resource = describe("data/table-infer-missing-values.csv", infer_missing_values=["-"])
    assert resource.schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
        "missingValues": ["-"],
    }


def test_describe_resource_schema_check_type_boolean_string_tie():
    resource = describe([["f"], ["stringish"]], headers=False, infer_names=["field"])
    assert resource.schema.get_field("field").type == "string"


# Issues


def test_describe_resource_schema_xlsx_file_with_boolean_column_issue_203():
    resource = describe("data/table-infer-boolean.xlsx")
    assert resource.schema == {
        "fields": [
            {"name": "number", "type": "integer"},
            {"name": "string", "type": "string"},
            {"name": "boolean", "type": "boolean"},
        ],
    }


def test_describe_resource_schema_increase_limit_issue_212():
    resource = describe("data/table-infer-increase-limit.csv", infer_volume=200)
    assert resource.schema == {
        "fields": [{"name": "a", "type": "integer"}, {"name": "b", "type": "number"}],
    }
