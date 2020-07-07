import os
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
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource(path=os.path.abspath("data/table.csv"))
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("data/table.csv")


def test_resource_source_path_error_bad_path_not_safe_traversing():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource(path="data/../data/table.csv")
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


def test_resource_source_non_tabular():
    resource = Resource(path="data/text.txt")
    assert resource.read_bytes() == b"text\n"


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


@pytest.mark.skip
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
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource({"name": "name", "path": "path", "dialect": dialect})
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
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource({"name": "name", "path": "path", "schema": "data/bad.json"})
    error = excinfo.value.error
    assert error.code == "schema-error"
    assert error.note.count("bad.json")


def test_resource_schema_from_path_error_path_not_safe():
    schema = os.path.abspath("data/schema.json")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Resource({"name": "name", "path": "path", "schema": schema})
    error = excinfo.value.error
    assert error.code == "resource-error"
    assert error.note.count("schema.json")


# Table


@pytest.mark.skip
def test_resource_table():
    pass


# Expand


@pytest.mark.skip
def test_resource_expand():
    descriptor = {
        "name": "name",
        "data": "data",
    }
    resource = Resource(descriptor)
    assert resource.descriptor == {
        "name": "name",
        "data": "data",
        "profile": "data-resource",
    }


@pytest.mark.skip
def test_resource_expand_tabular_schema():
    descriptor = {
        "name": "name",
        "data": "data",
        "profile": "tabular-data-resource",
        "schema": {"fields": [{"name": "name"}]},
    }
    resource = Resource(descriptor)
    assert resource.descriptor == {
        "name": "name",
        "data": "data",
        "profile": "tabular-data-resource",
        "schema": {
            "fields": [{"name": "name", "type": "string", "format": "default"}],
            "missingValues": [""],
        },
    }


@pytest.mark.skip
def test_resource_expand_tabular_dialect():
    descriptor = {
        "name": "name",
        "data": "data",
        "profile": "tabular-data-resource",
        "dialect": {"delimiter": "custom"},
    }
    resource = Resource(descriptor)
    assert resource.descriptor == {
        "name": "name",
        "data": "data",
        "profile": "tabular-data-resource",
        "dialect": {
            "delimiter": "custom",
            "doubleQuote": True,
            "lineTerminator": "\r\n",
            "quoteChar": '"',
            "skipInitialSpace": True,
            "header": True,
            "caseSensitiveHeader": False,
        },
    }


# Infer


# Multipart


@pytest.mark.skip
def test_descriptor_table_tabular_multipart_local():
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "path": ["chunk1.csv", "chunk2.csv"],
        "schema": "resource_schema.json",
    }
    resource = Resource(descriptor, base_path="data")
    assert resource.table.read(keyed=True) == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


@pytest.mark.skip
def test_descriptor_table_tabular_multipart_remote(patch_get):
    descriptor = {
        "name": "name",
        "profile": "tabular-data-resource",
        "path": [
            "http://example.com/chunk1.csv",
            "http://example.com/chunk2.csv",
            "http://example.com/chunk3.csv",
        ],
        "schema": "resource_schema.json",
    }
    # Mocks
    patch_get("http://example.com/chunk1.csv", body="id,name\n")
    patch_get("http://example.com/chunk2.csv", body="1,english")
    patch_get("http://example.com/chunk3.csv", body="2,中国人\n")
    # Tests
    resource = Resource(descriptor, base_path="data")
    assert resource.table.read(keyed=True) == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


@pytest.mark.skip
def test_source_multipart_local():
    descriptor = {
        "name": "name",
        "path": ["chunk1.csv", "chunk2.csv"],
    }
    resource = Resource(descriptor, base_path="data")
    assert resource.source == ["data/chunk1.csv", "data/chunk2.csv"]
    assert resource.local is True
    assert resource.multipart is True


@pytest.mark.skip
def test_source_multipart_local_bad_no_base_path():
    descriptor = {
        "name": "name",
        "path": ["chunk1.csv", "chunk2.csv"],
    }
    with pytest.raises(exceptions.DataPackageException):
        Resource(descriptor, base_path="")


@pytest.mark.skip
def test_source_multipart_local_bad_not_safe_absolute():
    descriptor = {
        "name": "name",
        "path": ["/fixtures/chunk1.csv", "chunk2.csv"],
    }
    with pytest.raises(exceptions.DataPackageException):
        Resource(descriptor, base_path="data")


@pytest.mark.skip
def test_source_multipart_local_bad_not_safe_traversing():
    descriptor = {
        "name": "name",
        "path": ["chunk1.csv", "../fixtures/chunk2.csv"],
    }
    with pytest.raises(exceptions.DataPackageException):
        Resource(descriptor, base_path="data")


@pytest.mark.skip
def test_source_multipart_remote():
    descriptor = {
        "name": "name",
        "path": ["http://example.com/chunk1.csv", "http://example.com/chunk2.csv"],
    }
    resource = Resource(descriptor)
    assert resource.source == [
        "http://example.com/chunk1.csv",
        "http://example.com/chunk2.csv",
    ]
    assert resource.remote is True
    assert resource.multipart is True


@pytest.mark.skip
def test_source_multipart_remote_path_relative_and_base_path_remote():
    descriptor = {
        "name": "name",
        "path": ["chunk1.csv", "chunk2.csv"],
    }
    resource = Resource(descriptor, base_path="http://example.com")
    assert resource.source == [
        "http://example.com/chunk1.csv",
        "http://example.com/chunk2.csv",
    ]
    assert resource.remote is True
    assert resource.multipart is True


@pytest.mark.skip
def test_source_multipart_remote_path_remote_and_base_path_remote():
    descriptor = {
        "name": "name",
        "path": ["chunk1.csv", "http://example2.com/chunk2.csv"],
    }
    resource = Resource(descriptor, base_path="http://example1.com")
    assert resource.source == [
        "http://example1.com/chunk1.csv",
        "http://example2.com/chunk2.csv",
    ]
    assert resource.remote is True
    assert resource.multipart is True


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
