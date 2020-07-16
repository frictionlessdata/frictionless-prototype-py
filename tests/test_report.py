from frictionless import validate


# Report


def test_report():
    report = validate("data/table.csv")
    # Report
    assert report.version.startswith("0")
    assert report.time
    assert report.valid is True
    assert report.stats == {"errors": 0, "tables": 1}
    assert report.errors == []
    # Table
    assert report.table.path == "data/table.csv"
    assert report.table.scheme == "file"
    assert report.table.format == "csv"
    assert report.table.hashing == "md5"
    assert report.table.encoding == "utf-8"
    assert report.table.compression == "no"
    assert report.table.compression_path == ""
    assert report.table.dialect == {}
    assert report.table.query == {}
    assert report.table.schema == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }
    assert report.table.headers == ["id", "name"]
    assert report.table.time
    assert report.table.valid is True
    assert report.table.scope == [
        "schema-error",
        "extra-header",
        "missing-header",
        "blank-header",
        "duplicate-header",
        "non-matching-header",
        "extra-cell",
        "missing-cell",
        "blank-row",
        "required-error",
        "type-error",
        "constraint-error",
        "unique-error",
        "primary-key-error",
        "foreign-key-error",
        "checksum-error",
    ]
    assert report.table.stats == {
        "hash": "6c2c61dd9b0e9c6876139a449ed87933",
        "bytes": 30,
        "rows": 2,
        "errors": 0,
    }
    assert report.errors == []


def test_report_expand():
    report = validate("data/table.csv")
    report.expand()
    assert report.table.schema == {
        "fields": [
            {"name": "id", "type": "integer", "format": "default", "bareNumber": True},
            {"name": "name", "type": "string", "format": "default"},
        ],
        "missingValues": [""],
    }
