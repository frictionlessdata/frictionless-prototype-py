import pytest
from frictionless import Table, exceptions
from frictionless.plugins.sql import SqlDialect


# Read


def test_table_format_sql(database_url):
    dialect = SqlDialect(table="data")
    with Table(database_url, dialect=dialect) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_format_sql_order_by(database_url):
    dialect = SqlDialect(table="data", order_by="id")
    with Table(database_url, dialect=dialect) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_format_sql_order_by_desc(database_url):
    dialect = SqlDialect(table="data", order_by="id desc")
    with Table(database_url, dialect=dialect) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[2, "中国人"], [1, "english"]]


def test_table_format_sql_table_is_required_error(database_url):
    table = Table(database_url)
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    error = excinfo.value.error
    assert error.code == "dialect-error"
    assert error.note.count("'table' is a required property")


def test_table_format_sql_headers_none(database_url):
    dialect = SqlDialect(table="data")
    with Table(database_url, dialect=dialect, headers=None) as table:
        assert table.headers is None
        assert table.read_data() == [["id", "name"], [1, "english"], [2, "中国人"]]


# Write


@pytest.mark.skip
def test_table_save_sqlite(database_url):
    source = "data/table.csv"
    with Table(source) as table:
        table.save(database_url, table="test_table_write_sqlite")
    with Table(database_url, table="test_table_write_sqlite", order_by="id") as table:
        assert table.read() == [["1", "english"], ["2", "中国人"]]
        assert table.headers == ["id", "name"]
