import pytest
from frictionless import Package, Resource, exceptions
from frictionless.plugins.pandas import PandasStorage

# Storage


def test_storage():

    # Export/Import
    source = Package("data/package-storage.json")
    storage = source.to_pandas()
    target = Package.from_pandas(dataframes=storage.dataframes)

    # Compare meta (with expected discrepancies)
    source.get_resource("article").schema.get_field("parent").type = "number"
    source.get_resource("article").schema.pop("foreignKeys")
    source.get_resource("comment").schema.get_field("note").type = "string"
    source.get_resource("comment").schema.pop("foreignKeys")
    source.get_resource("location").schema.get_field("geojson").type = "object"
    source.get_resource("location").schema.get_field("geopoint").type = "array"
    source.get_resource("temporal").schema.get_field("date_year").pop("format")
    source.get_resource("temporal").schema.get_field("year").type = "integer"
    source.get_resource("temporal").schema.get_field("yearmonth").type = "array"
    for resource in source.resources:
        assert resource.schema == target.get_resource(resource.name).schema

    # Compare data (with expected discrepancies)
    source.get_resource("temporal").schema.get_field("date_year").format = "%Y"
    source.get_resource("location").schema.get_field("geopoint").type = "geopoint"
    for resource in source.resources:
        # NOTE: recover this check
        if resource.name in ["location", "temporal"]:
            continue
        assert resource.read_rows() == target.get_resource(resource.name).read_rows()

    # Cleanup storage
    storage.delete_package(target.resource_names)


def test_storage_read_resource_not_existent_error():
    storage = PandasStorage()
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        storage.read_resource("bad")
    error = excinfo.value.error
    assert error.code == "storage-error"
    assert error.note.count("does not exist")


def test_storage_write_resource_existent_error():
    resource = Resource(path="data/table.csv")
    storage = resource.to_pandas()
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        storage.write_resource(resource)
    error = excinfo.value.error
    assert error.code == "storage-error"
    assert error.note.count("already exists")


def test_storage_delete_resource_not_existent_error():
    storage = PandasStorage()
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        storage.delete_resource("bad")
    error = excinfo.value.error
    assert error.code == "storage-error"
    assert error.note.count("does not exist")


def test_storage_dataframe_property_not_single_error():
    package = Package("data/package-storage.json")
    storage = package.to_pandas()
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        storage.dataframe
    error = excinfo.value.error
    assert error.code == "storage-error"
    assert error.note.count("single dataframe storages")
