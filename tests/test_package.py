import os
import glob
import json
import mock
import zipfile
import pytest
import tempfile
import httpretty
from copy import deepcopy
from datapackage import infer
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


@pytest.mark.skip
def test_it_works_with_local_paths(datapackage_zip):
    package = Package(datapackage_zip.name)
    assert package.descriptor["name"] == "proverbs"
    assert len(package.resources) == 1
    assert package.resources[0].data == b"foo\n"


@pytest.mark.skip
def test_it_works_with_file_objects(datapackage_zip):
    package = Package(datapackage_zip)
    assert package.descriptor["name"] == "proverbs"
    assert len(package.resources) == 1
    assert package.resources[0].data == b"foo\n"


@pytest.mark.skip
def test_it_works_with_remote_files(datapackage_zip):
    httpretty.enable()
    datapackage_zip.seek(0)
    url = "http://someplace.com/datapackage.zip"
    httpretty.register_uri(
        httpretty.GET, url, body=datapackage_zip.read(), content_type="application/zip"
    )
    package = Package(url)
    assert package.descriptor["name"] == "proverbs"
    assert len(package.resources) == 1
    assert package.resources[0].data == b"foo\n"
    httpretty.disable()


@pytest.mark.skip
def test_it_removes_temporary_directories(datapackage_zip):
    tempdirs_glob = os.path.join(tempfile.gettempdir(), "*-datapackage")
    original_tempdirs = glob.glob(tempdirs_glob)
    package = Package(datapackage_zip)
    package.save(datapackage_zip)
    del package
    assert glob.glob(tempdirs_glob) == original_tempdirs


@pytest.mark.skip
def test_local_data_path(datapackage_zip):
    package = Package(datapackage_zip)
    assert package.resources[0].local_data_path is not None
    with open("data/foo.txt") as data_file:
        with open(package.resources[0].local_data_path) as local_data_file:
            assert local_data_file.read() == data_file.read()


@pytest.mark.skip
def test_it_can_load_from_zip_files_inner_folders(tmpfile):
    descriptor = {
        "profile": "data-package",
    }
    with zipfile.ZipFile(tmpfile.name, "w") as z:
        z.writestr("foo/datapackage.json", json.dumps(descriptor))
    package = Package(tmpfile.name, {})
    assert package.descriptor == descriptor


@pytest.mark.skip
def test_it_breaks_if_theres_no_datapackage_json(tmpfile):
    with zipfile.ZipFile(tmpfile.name, "w") as z:
        z.writestr("data.txt", "foobar")
    with pytest.raises(exceptions.DataPackageException):
        Package(tmpfile.name, {})


@pytest.mark.skip
def test_it_breaks_if_theres_more_than_one_datapackage_json(tmpfile):
    descriptor_foo = {
        "name": "foo",
    }
    descriptor_bar = {
        "name": "bar",
    }
    with zipfile.ZipFile(tmpfile.name, "w") as z:
        z.writestr("foo/datapackage.json", json.dumps(descriptor_foo))
        z.writestr("bar/datapackage.json", json.dumps(descriptor_bar))
    with pytest.raises(exceptions.DataPackageException):
        Package(tmpfile.name, {})


# Resources


def test_package_resources():
    package = Package("data/package.json")
    assert package.name == "name"
    assert package.basepath == "data"
    assert package.profile == "data-package"
    assert package.resources == [
        {"name": "name", "path": "table.csv"},
    ]


def test_pakcage_resources_inline():
    data = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    package = Package({"resources": [{"name": "name", "data": data}]})
    resource = package.get_resource("name")
    assert len(package.resources) == 1
    assert resource.path is None
    assert resource.data is data
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


@pytest.mark.skip
def test_infer():
    descriptor = infer("datapackage/*.csv", base_path="data")
    assert descriptor == {
        "profile": "tabular-data-package",
        "resources": [
            {
                "encoding": "utf-8",
                "format": "csv",
                "mediatype": "text/csv",
                "name": "data",
                "path": "datapackage/data.csv",
                "profile": "tabular-data-resource",
                "schema": {
                    "fields": [
                        {"format": "default", "name": "id", "type": "integer"},
                        {"format": "default", "name": "city", "type": "string"},
                    ],
                    "missingValues": [""],
                },
            }
        ],
    }


@pytest.mark.skip
def test_infer_non_utf8_file():
    descriptor = infer("data/data_with_accents.csv")
    assert descriptor["resources"][0]["encoding"] == "iso-8859-1"


@pytest.mark.skip
def test_infer_empty_file():
    descriptor = infer("data/empty.csv")
    assert descriptor["resources"][0].get("encoding") is None


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


# Integrity

FK_DESCRIPTOR = {
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


@pytest.mark.skip
def test_single_field_foreign_key():
    resource = Package(FK_DESCRIPTOR).get_resource("main")
    rows = resource.read(relations=True)
    assert rows == [
        ["1", {"firstname": "Alex", "surname": "Martin"}, "Martin", None],
        ["2", {"firstname": "John", "surname": "Dockins"}, "Dockins", "1"],
        ["3", {"firstname": "Walter", "surname": "White"}, "White", "2"],
    ]


@pytest.mark.skip
def test_single_field_foreign_key_invalid():
    descriptor = deepcopy(FK_DESCRIPTOR)
    descriptor["resources"][1]["data"][2][0] = "Max"
    resource = Package(descriptor).get_resource("main")
    with pytest.raises(exceptions.RelationError) as excinfo1:
        resource.read(relations=True)
    with pytest.raises(exceptions.RelationError) as excinfo2:
        resource.check_relations()
    assert "Foreign key" in str(excinfo1.value)
    assert "Foreign key" in str(excinfo2.value)


@pytest.mark.skip
def test_single_self_field_foreign_key():
    descriptor = deepcopy(FK_DESCRIPTOR)
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["fields"] = "parent_id"
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["reference"]["resource"] = ""
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["reference"]["fields"] = "id"
    resource = Package(descriptor).get_resource("main")
    keyed_rows = resource.read(keyed=True, relations=True)
    assert keyed_rows == [
        {"id": "1", "name": "Alex", "surname": "Martin", "parent_id": None},
        {
            "id": "2",
            "name": "John",
            "surname": "Dockins",
            "parent_id": {
                "id": "1",
                "name": "Alex",
                "surname": "Martin",
                "parent_id": None,
            },
        },
        {
            "id": "3",
            "name": "Walter",
            "surname": "White",
            "parent_id": {
                "id": "2",
                "name": "John",
                "surname": "Dockins",
                "parent_id": "1",
            },
        },
    ]


@pytest.mark.skip
def test_single_self_field_foreign_key_invalid():
    descriptor = deepcopy(FK_DESCRIPTOR)
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["fields"] = "parent_id"
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["reference"]["resource"] = ""
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["reference"]["fields"] = "id"
    descriptor["resources"][0]["data"][2][0] = "0"
    resource = Package(descriptor).get_resource("main")
    with pytest.raises(exceptions.RelationError) as excinfo1:
        resource.read(relations=True)
    with pytest.raises(exceptions.RelationError) as excinfo2:
        resource.check_relations()
    assert "Foreign key" in str(excinfo1.value)
    assert "Foreign key" in str(excinfo2.value)


@pytest.mark.skip
def test_multi_field_foreign_key():
    descriptor = deepcopy(FK_DESCRIPTOR)
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["fields"] = ["name", "surname"]
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["reference"]["fields"] = [
        "firstname",
        "surname",
    ]
    resource = Package(descriptor).get_resource("main")
    keyed_rows = resource.read(keyed=True, relations=True)
    assert keyed_rows == [
        {
            "id": "1",
            "name": {"firstname": "Alex", "surname": "Martin"},
            "surname": {"firstname": "Alex", "surname": "Martin"},
            "parent_id": None,
        },
        {
            "id": "2",
            "name": {"firstname": "John", "surname": "Dockins"},
            "surname": {"firstname": "John", "surname": "Dockins"},
            "parent_id": "1",
        },
        {
            "id": "3",
            "name": {"firstname": "Walter", "surname": "White"},
            "surname": {"firstname": "Walter", "surname": "White"},
            "parent_id": "2",
        },
    ]


@pytest.mark.skip
def test_multi_field_foreign_key_invalid():
    descriptor = deepcopy(FK_DESCRIPTOR)
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["fields"] = ["name", "surname"]
    descriptor["resources"][0]["schema"]["foreignKeys"][0]["reference"]["fields"] = [
        "firstname",
        "surname",
    ]
    descriptor["resources"][1]["data"][2][0] = "Max"
    resource = Package(descriptor).get_resource("main")
    with pytest.raises(exceptions.RelationError) as excinfo1:
        resource.read(relations=True)
    with pytest.raises(exceptions.RelationError) as excinfo2:
        resource.check_relations()
    assert "Foreign key" in str(excinfo1.value)
    assert "Foreign key" in str(excinfo2.value)


# Issues


@pytest.mark.skip
def test_package_dialect_no_header_issue_167():
    package = Package("data/package_dialect_no_header.json")
    keyed_rows = package.get_resource("people").read(keyed=True)
    assert keyed_rows[0]["score"] == 1
    assert keyed_rows[1]["score"] == 1


@pytest.mark.skip
def test_package_save_slugify_fk_resource_name_issue_181():
    descriptor = {
        "resources": [
            {
                "name": "my-langs",
                "data": [["en"], ["ch"]],
                "schema": {"fields": [{"name": "lang"}]},
            },
            {
                "name": "my-notes",
                "data": [["1", "en", "note1"], [2, "ch", "note2"]],
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "lang"},
                        {"name": "note"},
                    ],
                    "foreignKeys": [
                        {
                            "fields": "lang",
                            "reference": {"resource": "my-langs", "fields": "lang"},
                        }
                    ],
                },
            },
        ]
    }
    storage = None
    #  storage = Mock(buckets=["my_langs", "my_notes"], spec=Storage)
    package = Package(descriptor)
    package.save(storage=storage)
    assert storage.create.call_args[0][0] == ["my_langs", "my_notes"]
    assert storage.create.call_args[0][1][1]["foreignKeys"] == [
        {"fields": "lang", "reference": {"resource": "my_langs", "fields": "lang"}}
    ]


# Fixtures


@pytest.fixture
def datapackage_zip(tmpfile):
    descriptor = {"name": "proverbs", "resources": [{"name": "name", "path": "foo.txt"}]}
    package = Package(descriptor, default_base_path="data")
    package.save(tmpfile)
    return tmpfile
