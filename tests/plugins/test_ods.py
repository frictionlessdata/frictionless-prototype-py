import pytest
from datetime import datetime
from frictionless import Table, exceptions

BASE_URL = 'https://raw.githubusercontent.com/okfn/tabulator-py/master/%s'


# Read


def test_table_ods():
    with Table('data/table.ods', headers=1) as table:
        assert table.headers == ['id', 'name']
        assert table.read(keyed=True) == [
            {'id': 1, 'name': 'english'},
            {'id': 2, 'name': '中国人'},
        ]


@pytest.mark.remote
def test_table_ods_remote():
    source = BASE_URL % 'data/table.ods'
    with Table(source) as table:
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_table_ods_sheet_by_index():
    with Table('data/table.ods', sheet=1) as table:
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_table_ods_sheet_by_index_not_existent():
    with pytest.raises(exceptions.SourceError) as excinfo:
        Table('data/table.ods', sheet=3).open()
    assert 'sheet "3"' in str(excinfo.value)


def test_table_ods_sheet_by_name():
    with Table('data/table.ods', sheet='Лист1') as table:
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_table_ods_sheet_by_index_not_existent_2():
    with pytest.raises(exceptions.SourceError) as excinfo:
        Table('data/table.ods', sheet='not-existent').open()
    assert 'sheet "not-existent"' in str(excinfo.value)


def test_table_ods_with_boolean():
    with Table('data/special/table-with-booleans.ods') as table:
        assert table.headers is None
        assert table.read() == [['id', 'boolean'], [1, True], [2, False]]


def test_table_ods_with_ints_floats_dates():
    source = 'data/special/table-with-ints-floats-dates.ods'
    with Table(source) as table:
        assert table.read() == [
            ['Int', 'Float', 'Date', 'Datetime'],
            [2013, 3.3, datetime(2009, 8, 16).date(), datetime(2009, 8, 16, 5, 43, 21)],
            [1997, 5.6, datetime(2009, 9, 20).date(), datetime(2009, 9, 20, 15, 30, 0)],
            [1969, 11.7, datetime(2012, 8, 23).date(), datetime(2012, 8, 23, 20, 40, 59)],
        ]
