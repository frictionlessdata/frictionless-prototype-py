import os
import pytest
import sqlalchemy as sa
from frictionless import Table, Package, exceptions
from frictionless.plugins.sql import SqlDialect
from dotenv import load_dotenv

load_dotenv(".env")


# Parser


def test_table_format_sql(database_url):
    dialect = SqlDialect(table="data")
    with Table(database_url, dialect=dialect) as table:
        assert table.header == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_format_sql_order_by(database_url):
    dialect = SqlDialect(table="data", order_by="id")
    with Table(database_url, dialect=dialect) as table:
        assert table.header == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_format_sql_order_by_desc(database_url):
    dialect = SqlDialect(table="data", order_by="id desc")
    with Table(database_url, dialect=dialect) as table:
        assert table.header == ["id", "name"]
        assert table.read_data() == [[2, "中国人"], [1, "english"]]


def test_table_format_sql_table_is_required_error(database_url):
    table = Table(database_url)
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    error = excinfo.value.error
    assert error.code == "dialect-error"
    assert error.note.count("'table' is a required property")


def test_table_format_sql_headers_false(database_url):
    dialect = SqlDialect(table="data")
    with Table(database_url, dialect=dialect, headers=False) as table:
        assert table.header == []
        assert table.read_data() == [["id", "name"], [1, "english"], [2, "中国人"]]


def test_table_write_sqlite(database_url):
    source = "data/table.csv"
    dialect = SqlDialect(table="name", order_by="id")
    with Table(source) as table:
        table.write(database_url, dialect=dialect)
    with Table(database_url, dialect=dialect) as table:
        assert table.header == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


# Storage


def test_package_storage_sqlite(database_url):
    engine = sa.create_engine(database_url)
    prefix = "prefix_"

    # Export/Import
    source = Package("data/package-storage.json")
    storage = source.to_sql(engine=engine, prefix=prefix, force=True)
    target = Package.from_sql(engine=engine, prefix=prefix)

    # Compare meta (with expected discrepancies)
    source.get_resource("comment").schema.get_field("note").type = "string"
    source.get_resource("location").schema.get_field("geojson").type = "string"
    source.get_resource("location").schema.get_field("geopoint").type = "string"
    source.get_resource("structure").schema.get_field("object").type = "string"
    source.get_resource("structure").schema.get_field("array").type = "string"
    source.get_resource("temporal").schema.get_field("date_year").pop("format")
    source.get_resource("temporal").schema.get_field("duration").type = "string"
    source.get_resource("temporal").schema.get_field("year").type = "integer"
    source.get_resource("temporal").schema.get_field("yearmonth").type = "string"
    for resource in source.resources:
        assert resource.schema == target.get_resource(resource.name).schema

    # Compare data (with expected discrepancies)
    source.get_resource("temporal").schema.get_field("date_year").format = "%Y"
    for resource in source.resources:
        assert resource.read_rows() == target.get_resource(resource.name).read_rows()

    # Cleanup storage
    storage.delete_package(target.resource_names)


@pytest.mark.ci
def test_package_storage_postgresql():
    engine = sa.create_engine(os.environ["POSTGRESQL_URL"])
    prefix = "prefix_"

    # Export/Import
    source = Package("data/package-storage.json")
    storage = source.to_sql(engine=engine, prefix=prefix, force=True)
    target = Package.from_sql(engine=engine, prefix=prefix)

    # Compare meta (with expected discrepancies)
    source.get_resource("comment").schema.get_field("note").type = "string"
    source.get_resource("location").schema.get_field("geojson").type = "object"
    source.get_resource("location").schema.get_field("geopoint").type = "string"
    source.get_resource("structure").schema.get_field("array").type = "object"
    source.get_resource("temporal").schema.get_field("date_year").pop("format")
    source.get_resource("temporal").schema.get_field("duration").type = "string"
    source.get_resource("temporal").schema.get_field("year").type = "integer"
    source.get_resource("temporal").schema.get_field("yearmonth").type = "string"
    for resource in source.resources:
        assert resource.schema == target.get_resource(resource.name).schema

    # Compare data (with expected discrepancies)
    source.get_resource("temporal").schema.get_field("date_year").format = "%Y"
    for resource in source.resources:
        assert resource.read_rows() == target.get_resource(resource.name).read_rows()

    # Cleanup storage
    storage.delete_package(target.resource_names)
