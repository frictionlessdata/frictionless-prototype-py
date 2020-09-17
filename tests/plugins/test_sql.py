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


@pytest.mark.skip
def test_package_storage_sqlite():
    engine = sa.create_engine(os.environ["SQLITE_URL"])
    prefix = "prefix_"

    # Write
    package = Package("data/package-storage.json")
    package.to_sql(engine=engine, prefix=prefix)

    # Read
    package = Package.from_sql(engine=engine, prefix=prefix)
    print(package)
