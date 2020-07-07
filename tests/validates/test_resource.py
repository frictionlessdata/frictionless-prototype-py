import pytest
from frictionless import validate


# General


def test_validate():
    report = validate({"path": "data/table.csv"})
    assert report.valid


def test_validate_invalid_source():
    report = validate("bad.json", source_type="resource")
    assert report.flatten(["code", "note"]) == [
        ["resource-error", 'Unable to load JSON at "bad.json"']
    ]


def test_validate_invalid_resource():
    report = validate({"path": "data/table.csv", "schema": "bad"})
    assert report.flatten(["code", "note"]) == [
        ["resource-error", 'Not resolved Local URI "bad" for resource.schema']
    ]


def test_validate_invalid_resource_exact():
    report = validate({"path": "data/table.csv"}, exact=True)
    assert report.flatten(["code", "note"]) == [
        [
            "resource-error",
            "Descriptor validation error: {'path': 'data/table.csv', 'profile': 'data-resource'} is not valid under any of the given schemas at \"\" in descriptor and at \"oneOf\" in profile",
        ]
    ]


def test_validate_invalid_table():
    report = validate({"path": "data/invalid.csv"})
    assert report.flatten(["rowPosition", "fieldPosition", "code"]) == [
        [None, 3, "blank-header"],
        [None, 4, "duplicate-header"],
        [2, 3, "missing-cell"],
        [2, 4, "missing-cell"],
        [3, 3, "missing-cell"],
        [3, 4, "missing-cell"],
        [4, None, "blank-row"],
        [5, 5, "extra-cell"],
    ]


@pytest.mark.skip
def test_validate_multipart_resource():
    report = validate({"path": ["data/chunk1.csv", "data/chunk2.csv"]})
    assert report.valid
    assert report.table.stats["rows"] == 2


# Integrity


def test_validate_foreign_key_error():
    source = {
        "path": "data/table.csv",
        "schema": {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
            "foreignKeys": [
                {"fields": "id", "reference": {"resource": "ids", "fields": "id"}}
            ],
        },
    }
    lookup = {"ids": {("id",): set([(1,), (2,)])}}
    report = validate(source, lookup=lookup)
    assert report.valid


def test_validate_foreign_key_error_invalid():
    source = {
        "path": "data/table.csv",
        "schema": {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
            "foreignKeys": [
                {"fields": "id", "reference": {"resource": "ids", "fields": "id"}}
            ],
        },
    }
    lookup = {"ids": {("id",): set([(1,)])}}
    report = validate(source, lookup=lookup)
    assert report.flatten(["rowPosition", "fieldPosition", "code"]) == [
        [3, None, "foreign-key-error"],
    ]


def test_validate_foreign_key_error_self_referencing():
    source = {
        "path": "data/nested.csv",
        "schema": {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "cat", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
            "foreignKeys": [
                {"fields": "cat", "reference": {"resource": "", "fields": "id"}}
            ],
        },
    }
    report = validate(source)
    assert report.valid


def test_validate_foreign_key_error_self_referencing_invalid():
    source = {
        "path": "data/nested-invalid.csv",
        "schema": {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "cat", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
            "foreignKeys": [
                {"fields": "cat", "reference": {"resource": "", "fields": "id"}}
            ],
        },
    }
    report = validate(source)
    assert report.flatten(["rowPosition", "fieldPosition", "code"]) == [
        [6, None, "foreign-key-error"],
    ]
