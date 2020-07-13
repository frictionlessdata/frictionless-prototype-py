from frictionless import validate, ReportTable, errors


# Report


def test_validate_report_props():
    report = validate("data/table.csv")
    # Report
    assert report.version.startswith("0")
    assert report.time
    assert report.valid is True
    assert report.stats == {"errors": 0, "tables": 1}
    assert report.errors == []
    # Table
    assert report.table["path"] == "data/table.csv"
    assert report.table["scheme"] == "file"
    assert report.table["format"] == "csv"
    assert report.table["hashing"] == "md5"
    assert report.table["encoding"] == "utf-8"
    assert report.table["dialect"] == {}
    assert report.table["headers"] == ["id", "name"]
    assert report.table["schema"] == {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }
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


# ReportTable


def test_table_report_valid():
    table = create_report_table(errors=[])
    assert table.valid is True
    assert table.stats["errors"] == 0
    assert table.flatten(["code"]) == []


def test_table_report_invalid():
    table = create_report_table(errors=[errors.SourceError(note="note")])
    assert table.valid is False
    assert table.stats["errors"] == 1
    assert table.flatten(["code"]) == [["source-error"]]


# Helpers


def create_report_table(
    *,
    path="path",
    scheme="scheme",
    format="format",
    hashing="hashing",
    encoding="encoding",
    compression="compression",
    compression_path="compression_path",
    dialect={},
    headers=None,
    pick_fields=None,
    skip_fields=None,
    limit_fields=None,
    offset_fields=None,
    pick_rows=None,
    skip_rows=None,
    limit_rows=None,
    offset_rows=None,
    schema=None,
    time=1,
    scope=[],
    partial=False,
    stats={},
    errors=[]
):
    return ReportTable(
        path=path,
        scheme=scheme,
        format=format,
        hashing=hashing,
        encoding=encoding,
        compression=compression,
        compression_path=compression_path,
        dialect=dialect,
        headers=headers,
        pick_fields=pick_fields,
        skip_fields=skip_fields,
        limit_fields=limit_fields,
        offset_fields=offset_fields,
        pick_rows=pick_rows,
        skip_rows=skip_rows,
        limit_rows=limit_rows,
        offset_rows=offset_rows,
        schema=schema,
        time=time,
        scope=scope,
        stats=stats,
        partial=partial,
        errors=errors,
    )
