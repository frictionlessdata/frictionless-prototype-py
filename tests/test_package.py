import os
import json
import mock
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


@pytest.mark.slow
def test_package_from_path_remote():
    package = Package(BASE_URL % "data/package.json")
    assert package.basepath == BASE_URL % "data"
    assert package == {"resources": [{"name": "name", "path": "path"}]}


@pytest.mark.slow
def test_package_from_path_remote_error_not_found():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Package(BASE_URL % "data/bad.json")
    error = excinfo.value.error
    assert error.code == "package-error"
    assert error.note.count("bad.json")


@pytest.mark.slow
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
                        {"name": "id", "type": "integer", "format": "default"},
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
    print(package)
    assert package == {
        "resources": [
            {
                "path": "data/table.csv",
                "dialect": {
                    "delimiter": ";",
                    "headers": {"rows": [1], "join": " "},
                    "lineTerminator": "\r\n",
                    "quoteChar": '"',
                    "doubleQuote": True,
                    "skipInitialSpace": True,
                    "header": True,
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
                "dialect": {
                    "delimiter": ",",
                    "lineTerminator": "\r\n",
                    "doubleQuote": True,
                    "quoteChar": '"',
                    "skipInitialSpace": False,
                },
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
                "dialect": {
                    "delimiter": ",",
                    "lineTerminator": "\r\n",
                    "doubleQuote": True,
                    "quoteChar": '"',
                    "skipInitialSpace": False,
                },
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
    package.infer("data.csv", "data2.csv")
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


# Save


@pytest.mark.skip
def test_save_as_json(json_tmpfile):
    package = Package({})
    package.save(json_tmpfile.name)
    assert json.loads(json_tmpfile.read().decode("utf-8")) == {
        "profile": "data-package",
    }


@pytest.mark.skip
# TODO: use a self-removing directory
def test_save_as_json_base_path(json_tmpfile):
    package = Package({}, base_path="/tmp")
    package.save(json_tmpfile.name, to_base_path=True)
    with open(os.path.join("/tmp", json_tmpfile.name), "r") as test_file:
        assert json.loads(test_file.read()) == {
            "profile": "data-package",
        }


@pytest.mark.skip("deprecated")
def test_saves_as_zip(tmpfile):
    package = Package(schema={})
    package.save(tmpfile)
    assert zipfile.is_zipfile(tmpfile)


@pytest.mark.skip
def test_accepts_file_paths(tmpfile):
    package = Package(schema={})
    package.save(tmpfile.name)
    assert zipfile.is_zipfile(tmpfile.name)


@pytest.mark.skip
def test_adds_datapackage_descriptor_at_zipfile_root(tmpfile):
    descriptor = {"name": "proverbs", "resources": [{"data": "万事开头难"}]}
    schema = {}
    package = Package(descriptor, schema)
    package.save(tmpfile)
    with zipfile.ZipFile(tmpfile, "r") as z:
        package_json = z.read("datapackage.json").decode("utf-8")
    assert json.loads(package_json) == json.loads(package.to_json())


@pytest.mark.skip
def test_generates_filenames_for_named_resources(tmpfile):
    descriptor = {
        "name": "proverbs",
        "resources": [
            {"name": "proverbs", "format": "TXT", "path": "unicode.txt"},
            {"name": "proverbs_without_format", "path": "unicode.txt"},
        ],
    }
    schema = {}
    package = Package(descriptor, schema, default_base_path="data")
    package.save(tmpfile)
    with zipfile.ZipFile(tmpfile, "r") as z:
        assert "data/proverbs.txt" in z.namelist()
        assert "data/proverbs_without_format" in z.namelist()


@pytest.mark.skip
def test_generates_unique_filenames_for_unnamed_resources(tmpfile):
    descriptor = {
        "name": "proverbs",
        "resources": [{"path": "unicode.txt"}, {"path": "foo.txt"}],
    }
    schema = {}
    package = Package(descriptor, schema, default_base_path="data")
    package.save(tmpfile)
    with zipfile.ZipFile(tmpfile, "r") as z:
        files = z.namelist()
        assert sorted(set(files)) == sorted(files)


@pytest.mark.skip
def test_adds_resources_inside_data_subfolder(tmpfile):
    descriptor = {
        "name": "proverbs",
        "resources": [{"name": "name", "path": "unicode.txt"}],
    }
    schema = {}
    package = Package(descriptor, schema, default_base_path="data")
    package.save(tmpfile)
    with zipfile.ZipFile(tmpfile, "r") as z:
        filename = [name for name in z.namelist() if name.startswith("data/")]
        assert len(filename) == 1
        resource_data = z.read(filename[0]).decode("utf-8")
    assert resource_data == "万事开头难\n"


@pytest.mark.skip
def test_fixes_resources_paths_to_be_relative_to_package(tmpfile):
    descriptor = {
        "name": "proverbs",
        "resources": [{"name": "unicode", "format": "txt", "path": "unicode.txt"}],
    }
    schema = {}
    pakage = Package(descriptor, schema, default_base_path="data")
    pakage.save(tmpfile)
    with zipfile.ZipFile(tmpfile, "r") as z:
        json_string = z.read("datapackage.json").decode("utf-8")
        generated_pakage_dict = json.loads(json_string)
    assert generated_pakage_dict["resources"][0]["path"] == "data/unicode.txt"


@pytest.mark.skip(reason="Wait for specs-v1.rc2 resource.data/path")
def test_works_with_resources_with_relative_paths(tmpfile):
    package = Package("data/datapackage_with_foo.txt_resource.json")
    package.save(tmpfile)
    with zipfile.ZipFile(tmpfile, "r") as z:
        assert len(z.filelist) == 2


@pytest.mark.skip
def test_should_raise_validation_error_if_datapackage_is_invalid(tmpfile):
    descriptor = {}
    schema = {
        "properties": {"name": {}},
        "required": ["name"],
    }
    package = Package(descriptor, schema)
    with pytest.raises(exceptions.ValidationError):
        package.save(tmpfile)


@pytest.mark.skip
def test_should_raise_if_path_doesnt_exist():
    package = Package({}, {})
    with pytest.raises(exceptions.DataPackageException):
        package.save("/non/existent/file/path")


@pytest.mark.skip
@mock.patch("zipfile.ZipFile")
def test_should_raise_if_zipfile_raised_BadZipfile(zipfile_mock, tmpfile):
    zipfile_mock.side_effect = zipfile.BadZipfile()
    package = Package({}, {})
    with pytest.raises(exceptions.DataPackageException):
        package.save(tmpfile)


@pytest.mark.skip
@mock.patch("zipfile.ZipFile")
def test_should_raise_if_zipfile_raised_LargeZipFile(zipfile_mock, tmpfile):
    zipfile_mock.side_effect = zipfile.LargeZipFile()
    package = Package({}, {})
    with pytest.raises(exceptions.DataPackageException):
        package.save(tmpfile)


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
