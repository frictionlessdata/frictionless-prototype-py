import pytest
from datetime import datetime
from frictionless import Table, exceptions
from frictionless.plugins.ods import OdsDialect

BASE_URL = "https://raw.githubusercontent.com/okfn/tabulator-py/master/%s"


# Read


def test_table_ods():
    with Table("data/table.ods") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


@pytest.mark.remote
def test_table_ods_remote():
    source = BASE_URL % "data/table.ods"
    with Table(source) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_ods_sheet_by_index():
    dialect = OdsDialect(sheet=1)
    with Table("data/table.ods", dialect=dialect) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


@pytest.mark.skip
def test_table_ods_sheet_by_index_not_existent():
    dialect = OdsDialect(sheet=3)
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Table("data/table.ods", dialect=dialect).open()
    assert 'sheet "3"' in str(excinfo.value)


def test_table_ods_sheet_by_name():
    dialect = OdsDialect(sheet="Лист1")
    with Table("data/table.ods", dialect=dialect) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


@pytest.mark.skip
def test_table_ods_sheet_by_index_not_existent_2():
    dialect = OdsDialect(sheet="bad")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Table("data/table.ods", dialct=dialect.sheet).open()
    assert 'sheet "bad"' in str(excinfo.value)


def test_table_ods_with_boolean():
    with Table("data/special/table-with-booleans.ods") as table:
        assert table.headers == ["id", "boolean"]
        assert table.read_data() == [[1, True], [2, False]]


def test_table_ods_with_ints_floats_dates():
    source = "data/special/table-with-ints-floats-dates.ods"
    with Table(source) as table:
        assert table.headers == ["Int", "Float", "Date", "Datetime"]
        assert table.read_data() == [
            [2013, 3.3, datetime(2009, 8, 16).date(), datetime(2009, 8, 16, 5, 43, 21)],
            [1997, 5.6, datetime(2009, 9, 20).date(), datetime(2009, 9, 20, 15, 30, 0)],
            [1969, 11.7, datetime(2012, 8, 23).date(), datetime(2012, 8, 23, 20, 40, 59)],
        ]
