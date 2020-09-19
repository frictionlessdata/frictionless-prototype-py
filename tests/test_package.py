import os
import json
import zipfile
import pytest
from frictionless import Package, exceptions


# General


BASE_URL = "https://raw.githubusercontent.com/frictionlessdata/datapackage-py/master/%s"


def test_package():
    package = Package("data/package.json")
    assert package.name == "name"
    assert package.basepath == "data"
    assert package.profile == "data-package"
    assert package.resources == [
        {"name": "name", "path": "table.csv"},
    ]


def test_package_from_dict():
    package = Package({"name": "name", "profile": "data-package"})
    assert package == {
        "name": "name",
        "profile": "data-package",
    }


def test_package_from_path():
    package = Package("data/package.json")
    assert package.name == "name"
    assert package.basepath == "data"
    assert package.profile == "data-package"
    assert package.resources == [
        {"name": "name", "path": "table.csv"},
    ]


def test_package_from_path_error_bad_path():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package("data/bad.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("bad.json")


def test_package_from_path_error_non_json():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package("data/table.csv")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("table.csv")


def test_package_from_path_error_bad_json():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package("data/invalid.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("invalid.json")


def test_package_from_path_error_bad_json_not_dict():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package("data/table.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("table.json")


@pytest.mark.ci
def test_package_from_path_remote():
    package = Package(BASE_URL % "data/package.json")
    assert package.basepath == BASE_URL % "data"
    assert package == {"resources": [{"name": "name", "path": "path"}]}


@pytest.mark.ci
def test_package_from_path_remote_error_not_found():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package(BASE_URL % "data/bad.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("bad.json")


@pytest.mark.ci
def test_package_from_path_remote_error_bad_json():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package(BASE_URL % "data/invalid.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("invalid.json")


def test_package_from_path_remote_error_bad_json_not_dict():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package(BASE_URL % "data/table-lists.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("table-lists.json")


def test_package_from_invalid_descriptor_type():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package(51)
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("51")


def test_package_from_zip():
    package = Package("data/package.zip")
    assert package.name == "testing"
    assert len(package.resources) == 2
    assert package.get_resource("data2").read_rows() == [
        {"parent": "A3001", "comment": "comment1"},
        {"parent": "A3001", "comment": "comment2"},
        {"parent": "A5032", "comment": "comment3"},
    ]


@pytest.mark.ci
def test_package_from_zip_remote():
    package = Package(BASE_URL % "data/package.zip")
    assert package.name == "testing"
    assert len(package.resources) == 2
    assert package.get_resource("data2").read_rows() == [
        {"parent": "A3001", "comment": "comment1"},
        {"parent": "A3001", "comment": "comment2"},
        {"parent": "A5032", "comment": "comment3"},
    ]


def test_package_from_zip_no_descriptor(tmpdir):
    descriptor = str(tmpdir.join("package.zip"))
    with zipfile.ZipFile(descriptor, "w") as zip:
        zip.writestr("data.txt", "foobar")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package(descriptor)
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("datapackage.json")


# Resources


def test_package_resources():
    package = Package("data/package.json")
    assert package.name == "name"
    assert package.basepath == "data"
    assert package.profile == "data-package"
    assert package.resources == [
        {"name": "name", "path": "table.csv"},
    ]


def test_package_resources_inline():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    package = Package({"resources": [{"name": "name", "data": data}]})
    resource = package.get_resource("name")
    assert len(package.resources) == 1
    assert resource.path is None
    assert resource.data == data
    assert resource.source == data
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_package_resources_empty():
    package = Package()
    assert package.resources == []


def test_package_add_resource():
    package = Package({})
    resource = package.add_resource({"name": "name", "data": []})
    assert len(package.resources) == 1
    assert package.resources[0].name == "name"
    assert resource.name == "name"


def test_package_remove_resource():
    package = Package({"resources": [{"name": "name", "data": []}]})
    resource = package.remove_resource("name")
    assert len(package.resources) == 0
    assert resource.name == "name"


def test_package_update_resource():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    package = Package({"resources": [{"name": "name", "data": data}]})
    resource = package.get_resource("name")
    resource.name = "newname"
    assert package == {"resources": [{"name": "newname", "data": data}]}


def test_package_resources_append_in_place():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    package = Package({"resources": []})
    package.resources.append({"name": "newname", "data": data})
    assert package == {"resources": [{"name": "newname", "data": data}]}


def test_package_resources_remove_in_place():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    package = Package({"resources": [{"name": "newname", "data": data}]})
    del package.resources[0]
    assert package == {"resources": []}


# Expand


def test_package_expand():
    package = Package("data/package.json")
    package.expand()
    assert package == {
        "name": "name",
        "profile": "data-package",
        "resources": [{"name": "name", "path": "table.csv", "profile": "data-resource"}],
    }


def test_package_expand_empty():
    package = Package()
    package.expand()
    assert package == {
        "profile": "data-package",
        "resources": [],
    }


def test_package_expand_resource_schema():
    schema = {
        "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    }
    package = Package({"resources": [{"path": "data/table.csv", "schema": schema}]})
    package.expand()
    assert package == {
        "resources": [
            {
                "path": "data/table.csv",
                "schema": {
                    "fields": [
                        {
                            "name": "id",
                            "type": "integer",
                            "format": "default",
                            "bareNumber": True,
                        },
                        {"name": "name", "type": "string", "format": "default"},
                    ],
                    "missingValues": [""],
                },
                "profile": "data-resource",
            }
        ],
        "profile": "data-package",
    }


def test_package_expand_resource_dialect():
    dialect = {"delimiter": ";"}
    package = Package({"resources": [{"path": "data/table.csv", "dialect": dialect}]})
    package.expand()
    assert package == {
        "resources": [
            {
                "path": "data/table.csv",
                "dialect": {
                    "delimiter": ";",
                    "header": True,
                    "headerRows": [1],
                    "headerJoin": " ",
                    "lineTerminator": "\r\n",
                    "quoteChar": '"',
                    "doubleQuote": True,
                    "skipInitialSpace": False,
                    "caseSensitiveHeader": False,
                },
                "profile": "data-resource",
            }
        ],
        "profile": "data-package",
    }


# Infer


def test_package_infer():
    package = Package()
    package.infer("data/infer/*.csv")
    assert package.metadata_valid
    assert package == {
        "profile": "data-package",
        "resources": [
            {
                "path": "data/infer/data.csv",
                "hash": "c028f525f314c49ea48ed09e82292ed2",
                "bytes": 114,
                "rows": 2,
                "profile": "tabular-data-resource",
                "name": "data",
                "scheme": "file",
                "format": "csv",
                "hashing": "md5",
                "encoding": "utf-8",
                "compression": "no",
                "compressionPath": "",
                "dialect": {},
                "schema": {
                    "fields": [
                        {"name": "id", "type": "string"},
                        {"name": "name", "type": "string"},
                        {"name": "description", "type": "string"},
                        {"name": "amount", "type": "number"},
                    ]
                },
            },
            {
                "path": "data/infer/data2.csv",
                "hash": "cb4a683d8eecb72c9ac9beea91fd592e",
                "bytes": 60,
                "rows": 3,
                "profile": "tabular-data-resource",
                "name": "data2",
                "scheme": "file",
                "format": "csv",
                "hashing": "md5",
                "encoding": "utf-8",
                "compression": "no",
                "compressionPath": "",
                "dialect": {},
                "schema": {
                    "fields": [
                        {"name": "parent", "type": "string"},
                        {"name": "comment", "type": "string"},
                    ]
                },
            },
        ],
    }


def test_package_infer_with_basepath():
    package = Package(basepath="data/infer")
    package.infer("*.csv")
    assert package.metadata_valid
    assert len(package.resources) == 2
    assert package.resources[0].path == "data.csv"
    assert package.resources[1].path == "data2.csv"


def test_package_infer_multiple_paths():
    package = Package(basepath="data/infer")
    package.infer(["data.csv", "data2.csv"])
    assert package.metadata_valid
    assert len(package.resources) == 2
    assert package.resources[0].path == "data.csv"
    assert package.resources[1].path == "data2.csv"


def test_package_infer_non_utf8_file():
    package = Package()
    package.infer("data/table-with-accents.csv")
    assert package.metadata_valid
    assert len(package.resources) == 1
    assert package.resources[0].encoding == "iso8859-1"


def test_package_infer_empty_file():
    package = Package()
    package.infer("data/empty.csv")
    assert package.metadata_valid
    assert len(package.resources) == 1
    assert package.resources[0].stats["bytes"] == 0


# Import/Export


def test_package_to_json(tmpdir):

    # Write
    target = os.path.join(tmpdir, "datapackage.json")
    package = Package("data/package.json")
    package.to_json(target)

    # Read
    with open(target, encoding="utf-8") as file:
        assert package == json.load(file)


def test_package_to_zip(tmpdir):

    # Write
    target = os.path.join(tmpdir, "datapackage.zip")
    package = Package("data/package.json")
    package.to_zip(target)

    # Read
    package = Package(target)
    assert package == {
        "name": "name",
        "resources": [{"name": "name", "path": "table.csv"}],
    }
    assert package.get_resource("name").read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


@pytest.mark.ci
def test_package_to_zip_source_remote(tmpdir):

    # Write
    path = BASE_URL % "data/table.csv"
    target = os.path.join(tmpdir, "datapackage.zip")
    package = Package(name="name", resources=[{"name": "name", "path": path}])
    package.to_zip(target)

    # Read
    package = Package(target)
    assert package == {
        "name": "name",
        "resources": [{"name": "name", "path": path}],
    }
    assert package.get_resource("name").read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_package_to_zip_source_inline(tmpdir):

    # Read
    target = os.path.join(tmpdir, "datapackage.zip")
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    package = Package(name="name", resources=[{"name": "name", "data": data}])
    package.to_zip(target)

    # Write
    package = Package(target)
    assert package == {
        "name": "name",
        "resources": [{"name": "name", "data": data}],
    }
    assert package.get_resource("name").read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


# Compression


def test_package_compression_implicit_gz():
    package = Package("data/compression/datapackage.json")
    resource = package.get_resource("implicit-gz")
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_package_compression_implicit_zip():
    package = Package("data/compression/datapackage.json")
    resource = package.get_resource("implicit-zip")
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_package_compression_explicit_gz():
    package = Package("data/compression/datapackage.json")
    resource = package.get_resource("explicit-gz")
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


def test_package_compression_explicit_zip():
    package = Package("data/compression/datapackage.json")
    resource = package.get_resource("explicit-zip")
    assert resource.read_rows() == [
        {"id": 1, "name": "english"},
        {"id": 2, "name": "中国人"},
    ]


# Integrity

INTEGRITY_DESCRIPTOR = {
    "resources": [
        {
            "name": "main",
            "data": [
                ["id", "name", "surname", "parent_id"],
                ["1", "Alex", "Martin", ""],
                ["2", "John", "Dockins", "1"],
                ["3", "Walter", "White", "2"],
            ],
            "schema": {
                "fields": [
                    {"name": "id"},
                    {"name": "name"},
                    {"name": "surname"},
                    {"name": "parent_id"},
                ],
                "foreignKeys": [
                    {
                        "fields": "name",
                        "reference": {"resource": "people", "fields": "firstname"},
                    },
                ],
            },
        },
        {
            "name": "people",
            "data": [
                ["firstname", "surname"],
                ["Alex", "Martin"],
                ["John", "Dockins"],
                ["Walter", "White"],
            ],
        },
    ],
}


def test_package_integrity():
    package = Package(INTEGRITY_DESCRIPTOR)
    resource = package.get_resource("main")
    rows = resource.read_rows()
    assert rows[0].valid
    assert rows[1].valid
    assert rows[2].valid
    assert rows == [
        {"id": "1", "name": "Alex", "surname": "Martin", "parent_id": None},
        {"id": "2", "name": "John", "surname": "Dockins", "parent_id": "1"},
        {"id": "3", "name": "Walter", "surname": "White", "parent_id": "2"},
    ]


def test_package_integrity_foreign_key_invalid():
    package = Package(INTEGRITY_DESCRIPTOR)
    package.resources[1].data[3][0] = "bad"
    resource = package.get_resource("main")
    rows = resource.read_rows()
    assert rows[0].valid
    assert rows[1].valid
    assert rows[2].errors[0].code == "foreign-key-error"
    assert rows == [
        {"id": "1", "name": "Alex", "surname": "Martin", "parent_id": None},
        {"id": "2", "name": "John", "surname": "Dockins", "parent_id": "1"},
        {"id": "3", "name": "Walter", "surname": "White", "parent_id": "2"},
    ]


def test_package_integrity_foreign_key_self_reference():
    package = Package(INTEGRITY_DESCRIPTOR)
    package.resources[0].schema.foreign_keys = [
        {"fields": "parent_id", "reference": {"resource": "", "fields": "id"}}
    ]
    resource = package.get_resource("main")
    rows = resource.read_rows()
    assert rows[0].valid
    assert rows[1].valid
    assert rows[2].valid


def test_package_integrity_foreign_key_self_reference_invalid():
    package = Package(INTEGRITY_DESCRIPTOR)
    package.resources[0].data[2][0] = "0"
    package.resources[0].schema.foreign_keys = [
        {"fields": "parent_id", "reference": {"resource": "", "fields": "id"}}
    ]
    resource = package.get_resource("main")
    rows = resource.read_rows()
    assert rows[0].valid
    assert rows[1].valid
    assert rows[2].errors[0].code == "foreign-key-error"


def test_package_integrity_foreign_key_multifield():
    package = Package(INTEGRITY_DESCRIPTOR)
    package.resources[0].schema.foreign_keys = [
        {
            "fields": ["name", "surname"],
            "reference": {"resource": "people", "fields": ["firstname", "surname"]},
        }
    ]
    resource = package.get_resource("main")
    rows = resource.read_rows()
    assert rows[0].valid
    assert rows[1].valid
    assert rows[2].valid


def test_package_integrity_foreign_key_multifield_invalid():
    package = Package(INTEGRITY_DESCRIPTOR)
    package.resources[0].schema.foreign_keys = [
        {
            "fields": ["name", "surname"],
            "reference": {"resource": "people", "fields": ["firstname", "surname"]},
        }
    ]
    package.resources[1].data[3][0] = "bad"
    resource = package.get_resource("main")
    rows = resource.read_rows()
    assert rows[0].valid
    assert rows[1].valid
    assert rows[2].errors[0].code == "foreign-key-error"


def test_package_integrity_read_lookup():
    package = Package(INTEGRITY_DESCRIPTOR)
    resource = package.get_resource("main")
    lookup = resource.read_lookup()
    assert lookup == {"people": {("firstname",): {("Walter",), ("Alex",), ("John",)}}}


# Issues


def test_package_dialect_no_header_issue_167():
    package = Package("data/package-dialect-no-header.json")
    resource = package.get_resource("people")
    rows = resource.read_rows()
    assert rows[0]["score"] == 1
    assert rows[1]["score"] == 1
