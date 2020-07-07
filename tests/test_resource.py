import io
import os
import json
import pytest
from frictionless import Resource, exceptions


# General


BASE_URL = "https://raw.githubusercontent.com/frictionlessdata/datapackage-py/master/%s"


def test_resource():
    resource = Resource(path="data/table.csv")
    assert resource.basepath == ""
    assert resource.path == "data/table.csv"
    assert resource.source == "data/table.csv"
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_basepath():
    resource = Resource(path="table.csv", basepath="data")
    assert resource.basepath == "data"
    assert resource.path == "table.csv"
    assert resource.source == "data/table.csv"
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_from_dict():
    resource = Resource({"name": "name", "path": "data/table.csv"})
    assert resource == {
        "name": "name",
        "path": "data/table.csv",
    }
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_from_path():
    resource = Resource("data/resource.json")
    assert resource == {"name": "name", "path": "table.csv"}
    assert resource.basepath == "data"
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_from_path_error_bad_path():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource("data/bad.json")
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("bad.json")


@pytest.mark.slow
def test_resource_from_path_remote():
    resource = Resource(BASE_URL % "data/resource.json")
    assert resource.source == BASE_URL % "data/table.csv"
    assert resource.path == "table.csv"
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


@pytest.mark.slow
def test_resource_from_path_remote_error_bad_path():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource(BASE_URL % "data/bad.json")
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("bad.json")


def test_resource_source_path():
    path = "data/table.csv"
    resource = Resource({"path": path})
    assert resource.basepath == ""
    assert resource.path == path
    assert resource.data is None
    assert resource.source == path
    assert resource.inline is False
    assert resource.tabular is True
    assert resource.multipart is False
    assert (
        resource.read_bytes()
        == b"id,name\n1,english\n2,\xe4\xb8\xad\xe5\x9b\xbd\xe4\xba\xba\n"
    )
    assert resource.read_data() == [["1", "english"], ["2", "中国人"]]
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]
    assert resource.read_headers() == ["id", "name"]
    assert resource.read_sample() == [["1", "english"], ["2", "中国人"]]
    assert resource.read_stats() == {
        "hash": "6c2c61dd9b0e9c6876139a449ed87933",
        "bytes": 30,
        "rows": 2,
    }


@pytest.mark.slow
def test_resource_source_path_and_basepath_remote():
    resource = Resource(path="table.csv", basepath=BASE_URL % "data")
    assert resource.source == BASE_URL % "data/table.csv"
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


@pytest.mark.slow
def test_resource_source_path_remote_and_basepath_remote():
    resource = Resource(path=BASE_URL % "data/table.csv", basepath=BASE_URL % "data")
    assert resource.source == BASE_URL % "data/table.csv"
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_source_path_error_bad_path():
    resource = Resource({"name": "name", "path": "table.csv"})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "scheme-error"
    assert error.note == "[Errno 2] No such file or directory: 'table.csv'"


def test_resource_source_path_error_bad_path_not_safe_absolute():
    resource = Resource(path=os.path.abspath("data/table.csv"))
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("data/table.csv")


def test_resource_source_path_error_bad_path_not_safe_traversing():
    resource = Resource(path="data/../data/table.csv")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("data/table.csv")


def test_resource_source_data():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    resource = Resource({"data": data})
    assert resource.basepath == ""
    assert resource.path is None
    assert resource.data == data
    assert resource.source == data
    assert resource.inline is True
    assert resource.tabular is True
    assert resource.multipart is False
    assert resource.read_bytes() == b""
    assert resource.read_data() == data[1:]
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]
    assert resource.read_headers() == ["id", "name"]
    assert resource.read_sample() == data[1:]
    assert resource.read_stats() == {
        "hash": "",
        "bytes": 0,
        "rows": 2,
    }


def test_resource_source_path_and_data():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    resource = Resource({"data": data, "path": "path"})
    assert resource.path == "path"
    assert resource.data == data
    assert resource.source == data
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_source_no_path_and_no_data():
    resource = Resource({})
    assert resource.path is None
    assert resource.data is None
    assert resource.source == []
    assert resource.read_rows() == []


# Dialect


def test_resource_dialect():
    dialect = {
        "delimiter": "|",
        "quoteChar": "#",
        "escapeChar": "-",
        "doubleQuote": False,
        "skipInitialSpace": False,
    }
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "path": "dialect.csv",
        "schema": "resource-schema.json",
        "dialect": dialect,
    }
    resource = Resource(descriptor, basepath="data")
    assert resource.dialect == dialect
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": " |##"},
    ]


def test_resource_dialect_header_false():
    dialect = {"header": False}
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "path": "without-headers.csv",
        "dialect": dialect,
        "schema": "resource-schema.json",
    }
    resource = Resource(descriptor, basepath="data")
    assert resource.dialect == dialect
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
        {"id": 3, "name": "german"},
    ]


def test_resource_dialect_from_path():
    resource = Resource("data/resource-with-dereferencing.json")
    assert resource == {
        "name": "name",
        "path": "table.csv",
        "dialect": "dialect.json",
        "schema": "schema.json",
    }
    assert resource.dialect == {
        "delimiter": ";",
    }


@pytest.mark.slow
def test_resource_dialect_from_path_remote():
    resource = Resource(BASE_URL % "data/resource-with-dereferencing.json")
    assert resource == {
        "name": "name",
        "path": "table.csv",
        "dialect": "dialect.json",
        "schema": "schema-simple.json",
    }
    assert resource.dialect == {
        "delimiter": ";",
    }


def test_resource_dialect_from_path_error_path_not_safe():
    dialect = os.path.abspath("data/dialect.json")
    resource = Resource({"name": "name", "path": "path", "dialect": dialect})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("dialect.json")


# Schema


def test_resource_schema():
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "path": "table.csv",
        "schema": "resource-schema.json",
    }
    resource = Resource(descriptor, basepath="data")
    assert resource.schema == {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_schema_source_data():
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "data": [["id", "name"], ["1", "english"], ["2", "中国人"]],
        "schema": "resource-schema.json",
    }
    resource = Resource(descriptor, basepath="data")
    assert resource.schema == {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_schema_source_remote():
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "path": "table.csv",
        "schema": "resource_schema.json",
    }
    resource = Resource(descriptor, basepath=BASE_URL % "data")
    assert resource.schema == {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_resource_schema_from_path():
    resource = Resource("data/resource-with-dereferencing.json")
    assert resource == {
        "name": "name",
        "path": "table.csv",
        "dialect": "dialect.json",
        "schema": "schema.json",
    }
    assert resource.schema == {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }


def test_resource_schema_from_path_with_basepath():
    descriptor = {"name": "name", "path": "table.csv", "schema": "schema.json"}
    resource = Resource(descriptor, basepath="data")
    assert resource == descriptor
    assert resource.schema == {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }


@pytest.mark.slow
def test_resource_schema_from_path_remote():
    resource = Resource(BASE_URL % "data/resource-with-dereferencing.json")
    assert resource == {
        "name": "name",
        "path": "table.csv",
        "dialect": "dialect.json",
        "schema": "schema-simple.json",
    }
    assert resource.schema == {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }


def test_resource_schema_from_path_error_bad_path():
    resource = Resource({"name": "name", "path": "path", "schema": "data/bad.json"})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "schema-error"
    assert error.note.count("bad.json")


def test_resource_schema_from_path_error_path_not_safe():
    schema = os.path.abspath("data/schema.json")
    resource = Resource({"name": "name", "path": "path", "schema": schema})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("schema.json")


# Table


def test_resource_table():
    resource = Resource(path="data/table.csv")
    with resource.table as table:
        assert table.headers == ["id", "name"]
        assert table.read_rows() == [
            {"id": 1, "name": "english"},
            {"id": 2, "name": "中国人"},
        ]


def test_resource_table_source_data():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    resource = Resource(data=data)
    with resource.table as table:
        assert table.headers == ["id", "name"]
        assert table.read_rows() == [
            {"id": 1, "name": "english"},
            {"id": 2, "name": "中国人"},
        ]


@pytest.mark.slow
def test_resource_table_source_remote():
    resource = Resource(path=BASE_URL % "data/table.csv")
    with resource.table as table:
        assert table.headers == ["id", "name"]
        assert table.read_rows() == [
            {"id": 1, "name": "english"},
            {"id": 2, "name": "中国人"},
        ]


# Expand


def test_resource_expand():
    resource = Resource({"name": "name", "path": "data/table.csv"})
    resource.expand()
    assert resource == {
        "name": "name",
        "path": "data/table.csv",
        "profile": "data-resource",
    }


def test_resource_expand_with_dialect():
    dialect = {"delimiter": "custom"}
    resource = Resource({"name": "name", "path": "data/table.csv", "dialect": dialect})
    resource.expand()
    assert resource == {
        "name": "name",
        "path": "data/table.csv",
        "profile": "data-resource",
        "dialect": {
            "headers": {"join": " ", "rows": [1]},
            "delimiter": "custom",
            "lineTerminator": "\r\n",
            "doubleQuote": True,
            "quoteChar": '"',
            "skipInitialSpace": True,
            "caseSensitiveHeader": False,
            "header": True,
        },
    }


def test_resource_expand_with_schema():
    schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
        ],
    }
    resource = Resource({"name": "name", "path": "data/table.csv", "schema": schema})
    resource.expand()
    assert resource == {
        "name": "name",
        "path": "data/table.csv",
        "profile": "data-resource",
        "schema": {
            "fields": [
                {"name": "id", "type": "integer", "format": "default"},
                {"name": "name", "type": "string", "format": "default"},
            ],
            "missingValues": [""],
        },
    }


# Infer

# Save


def test_resource_save(tmpdir):
    path = str(tmpdir.join("resource.json"))
    resource = Resource("data/resource.json")
    resource.save(path)
    with io.open(path, encoding="utf-8") as file:
        descriptor = json.load(file)
    assert descriptor == {
        "name": "name",
        "path": "table.csv",
    }


# Multipart


def test_resource_source_multipart():
    descriptor = {
        "path": ["chunk1.csv", "chunk2.csv"],
        "schema": "resource-schema.json",
    }
    resource = Resource(descriptor, basepath="data")
    assert resource.inline is False
    assert resource.multipart is True
    assert resource.tabular is True
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


@pytest.mark.slow
def test_resource_source_multipart_remote():
    descriptor = {
        "name": "name",
        "path": ["chunk2.csv", "chunk3.csv"],
        "dialect": {"header": False},
        "schema": "resource_schema.json",
    }
    resource = Resource(descriptor, basepath=BASE_URL % "data")
    assert resource.inline is False
    assert resource.multipart is True
    assert resource.tabular is True
    assert resource.read_rows() == [
        {"id": 2, "name": "中国人"},
        {"id": 3, "name": "german"},
    ]


@pytest.mark.slow
def test_resource_source_multipart_remote_both_path_and_basepath():
    descriptor = {
        "name": "name",
        "path": ["chunk2.csv", BASE_URL % "data/chunk3.csv"],
        "dialect": {"header": False},
        "schema": "resource_schema.json",
    }
    resource = Resource(descriptor, basepath=BASE_URL % "data")
    assert resource.inline is False
    assert resource.multipart is True
    assert resource.tabular is True
    assert resource.read_rows() == [
        {"id": 2, "name": "中国人"},
        {"id": 3, "name": "german"},
    ]


def test_resource_source_multipart_error_bad_path():
    resource = Resource({"name": "name", "path": ["chunk1.csv", "chunk2.csv"]})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "source-error"
    assert error.note == "[Errno 2] No such file or directory: 'chunk1.csv'"


def test_resource_source_multipart_error_bad_path_not_safe_absolute():
    bad_path = os.path.abspath("data/chunk1.csv")
    resource = Resource({"name": "name", "path": [bad_path, "data/chunk2.csv"]})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("not safe")


def test_resource_source_multipart_error_bad_path_not_safe_traversing():
    bad_path = os.path.abspath("data/../chunk2.csv")
    resource = Resource({"name": "name", "path": ["data/chunk1.csv", bad_path]})
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_rows()
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("not safe")


@pytest.mark.skip
def test_source_multipart_local_infer():
    descriptor = {"path": ["data/chunk1.csv", "data/chunk2.csv"]}
    resource = Resource(descriptor)
    resource.infer()
    assert resource.descriptor == {
        "name": "chunk1",
        "profile": "tabular-data-resource",
        "path": ["data/chunk1.csv", "data/chunk2.csv"],
        "format": "csv",
        "mediatype": "text/csv",
        "encoding": "utf-8",
        "schema": {
            "fields": [
                {"name": "id", "type": "integer", "format": "default"},
                {"name": "name", "type": "string", "format": "default"},
            ],
            "missingValues": [""],
        },
    }


# Non-tabular


def test_resource_source_non_tabular():
    path = "data/text.txt"
    resource = Resource(path=path)
    assert resource.basepath == ""
    assert resource.path == path
    assert resource.data is None
    assert resource.source == path
    assert resource.inline is False
    assert resource.tabular is False
    assert resource.multipart is False
    assert resource.read_bytes() == b"text\n"
    assert resource.read_stats() == {
        "hash": "e1cbb0c3879af8347246f12c559a86b5",
        "bytes": 5,
        "rows": 0,
    }


@pytest.mark.slow
def test_resource_source_non_tabular_remote():
    path = BASE_URL % "data/foo.txt"
    resource = Resource(path=path)
    assert resource.basepath == ""
    assert resource.path == path
    assert resource.data is None
    assert resource.source == path
    assert resource.inline is False
    assert resource.tabular is False
    assert resource.multipart is False
    assert resource.read_bytes() == b"foo\n"
    assert resource.read_stats() == {
        "hash": "d3b07384d113edec49eaa6238ad5ff00",
        "bytes": 4,
        "rows": 0,
    }


def test_resource_source_non_tabular_error_bad_path():
    resource = Resource(path="data/bad.txt")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.read_bytes()
    error = excinfo.value.error
    assert error.code == "scheme-error"
    assert error.note.count("data/bad.txt")


def test_resource_source_non_tabular_table():
    resource = Resource(path="data/text.txt")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        resource.table.open()
    error = excinfo.value.error
    assert error.code == "format-error"
    assert error.note == 'cannot create parser "txt". Try installing "frictionless-txt"'


# Issues


@pytest.mark.skip
def test_source_relative_parent_path_with_unsafe_option_issue_171():
    descriptor = {"path": "data/../data/table.csv"}
    # unsafe=false (default)
    with pytest.raises(exceptions.DataPackageException) as excinfo:
        resource = Resource(descriptor)
    assert "is not safe" in str(excinfo.value)
    # unsafe=true
    resource = Resource(descriptor, unsafe=True)
    assert resource.read() == [["1", "english"], ["2", "中国人"]]


@pytest.mark.skip
def test_preserve_resource_format_from_descriptor_on_infer_issue_188():
    resource = Resource({"path": "data/data.csvformat", "format": "csv"})
    assert resource.infer() == {
        "encoding": "utf-8",
        "format": "csv",
        "mediatype": "text/csv",
        "name": "data",
        "path": "data/data.csvformat",
        "profile": "tabular-data-resource",
        "schema": {
            "fields": [
                {"format": "default", "name": "city", "type": "string"},
                {"format": "default", "name": "population", "type": "integer"},
            ],
            "missingValues": [""],
        },
    }
