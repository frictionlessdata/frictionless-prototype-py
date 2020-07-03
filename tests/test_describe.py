from frictionless import describe


# General


def test_describe():
    schema = describe("data/table-infer.csv")
    assert schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_utf8():
    schema = describe("data/table-infer-utf8.csv")
    assert schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_expand():
    schema = describe("data/table-infer.csv", expand=True)
    assert schema == {
        "fields": [
            {"name": "id", "type": "integer", "format": "default"},
            {"name": "age", "type": "integer", "format": "default"},
            {"name": "name", "type": "string", "format": "default"},
        ],
        "missingValues": [""],
    }


def test_describe_infer_volume():
    schema = describe("data/table-infer-row-limit.csv", infer_volume=4)
    assert schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_with_missing_values_default():
    schema = describe("data/table-infer-missing-values.csv")
    assert schema == {
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }


def test_describe_with_missing_values_using_the_argument():
    schema = describe("data/table-infer-missing-values.csv", missing_values=["-"])
    assert schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "age", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
        "missingValues": ["-"],
    }


def test_describe_check_type_boolean_string_tie():
    schema = describe([["f"], ["stringish"]], headers_row=None, infer_names=["field"])
    assert schema["fields"][0]["type"] == "string"


def test_describe_xlsx_file_with_boolean_column_issue_203():
    schema = describe("data/table-infer-boolean.xlsx")
    assert schema == {
        "fields": [
            {"name": "number", "type": "integer"},
            {"name": "string", "type": "string"},
            {"name": "boolean", "type": "boolean"},
        ],
    }


# Issues


def test_describe_increase_limit_issue_212():
    schema = describe("data/table-infer-increase-limit.csv", infer_volume=200)
    assert schema == {
        "fields": [{"name": "a", "type": "integer"}, {"name": "b", "type": "number"}],
    }
