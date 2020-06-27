import pytest
from frictionless import Table, exceptions
from frictionless.plugins.sql import SqlDialect


# Read


def test_table_format_sql(database_url):
    dialect = SqlDialect(table='data')
    with Table(database_url, dialect=dialect) as table:
        assert table.read() == [[1, 'english'], [2, '中国人']]


def test_table_format_sql_order_by(database_url):
    dialect = SqlDialect(table='data', order_by='id')
    with Table(database_url, dialect=dialect) as table:
        assert table.read() == [[1, 'english'], [2, '中国人']]


def test_table_format_sql_order_by_desc(database_url):
    dialect = SqlDialect(table='data', order_by='id desc')
    with Table(database_url, dialect=dialect) as table:
        assert table.read() == [[2, '中国人'], [1, 'english']]


@pytest.mark.skip
def test_table_format_sql_table_is_required_error(database_url):
    with pytest.raises(exceptions.TabulatorException) as excinfo:
        Table(database_url).open()
    assert 'table' in str(excinfo.value)


def test_table_format_sql_headers(database_url):
    dialect = SqlDialect(table='data')
    with Table(database_url, dialect=dialect, headers=1) as table:
        assert table.headers == ['id', 'name']
        assert table.read() == [[1, 'english'], [2, '中国人']]


# Write


@pytest.mark.skip
def test_table_save_sqlite(database_url):
    source = 'data/table.csv'
    with Table(source, headers=1) as table:
        assert table.save(database_url, table='test_table_save_sqlite') == 2
    with Table(
        database_url, table='test_table_save_sqlite', order_by='id', headers=1
    ) as table:
        assert table.read() == [['1', 'english'], ['2', '中国人']]
        assert table.headers == ['id', 'name']
