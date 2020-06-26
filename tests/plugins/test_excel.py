import io
import pytest
from datetime import datetime
from frictionless import Table, dialects, exceptions

BASE_URL = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/%s"


# Read


def test_table_xlsx_table():
    source = io.open("data/table.xlsx", mode="rb")
    with Table(source, format="xlsx") as table:
        assert table.headers is None
        assert table.read() == [["id", "name"], [1.0, "english"], [2.0, "中国人"]]


@pytest.mark.slow
def test_table_xlsx_remote():
    source = BASE_URL % "data/table.xlsx"
    with Table(source) as table:
        assert table.read() == [["id", "name"], [1.0, "english"], [2.0, "中国人"]]


def test_table_xlsx_sheet_by_index():
    source = "data/special/sheet2.xlsx"
    dialect = dialects.ExcelDialect(sheet=2)
    with Table(source, dialect=dialect) as table:
        assert table.read() == [["id", "name"], [1, "english"], [2, "中国人"]]


@pytest.mark.skip
def test_table_xlsx_sheet_by_index_not_existent():
    source = "data/special/sheet2.xlsx"
    with pytest.raises(exceptions.SourceError) as excinfo:
        Table(source, sheet=3).open()
    assert 'sheet "3"' in str(excinfo.value)


def test_table_xlsx_sheet_by_name():
    source = "data/special/sheet2.xlsx"
    dialect = dialects.ExcelDialect(sheet='Sheet2')
    with Table(source, dialect=dialect) as table:
        assert table.read() == [["id", "name"], [1, "english"], [2, "中国人"]]


def test_table_xlsx_sheet_by_name_not_existent():
    source = "data/special/sheet2.xlsx"
    dialect = dialects.ExcelDialect(sheet='bad')
    with pytest.raises(exceptions.SourceError) as excinfo:
        Table(source, dialect=dialect).open()
    assert 'sheet "bad"' in str(excinfo.value)


def test_table_xlsx_merged_cells():
    source = "data/special/merged-cells.xlsx"
    with Table(source) as table:
        assert table.read() == [["data", None]]


def test_table_xlsx_merged_cells_fill():
    source = "data/special/merged-cells.xlsx"
    dialect = dialects.ExcelDialect(fill_merged_cells=True)
    with Table(source, dialect=dialect) as table:
        assert table.read() == [["data", "data"], ["data", "data"], ["data", "data"]]


def test_table_xlsx_adjust_floating_point_error():
    source = "data/special/adjust_floating_point_error.xlsx"
    dialect = dialects.ExcelDialect(preserve_formatting=True)
    with Table(source, headers=1, ignore_blank_headers=True, dialect=dialect) as table:
        assert table.read(keyed=True)[1]["actual PO4 (values)"] == 274.65999999999997
    dialect = dialects.ExcelDialect(
        fill_merged_cells=False,
        preserve_formatting=True,
        adjust_floating_point_error=True,
    )
    with Table(source, headers=1, ignore_blank_headers=True, dialect=dialect) as table:
        assert table.read(keyed=True)[1]["actual PO4 (values)"] == 274.66


def test_table_xlsx_preserve_formatting():
    source = "data/special/preserve-formatting.xlsx"
    dialect = dialects.ExcelDialect(preserve_formatting=True)
    with Table(source, headers=1, dialect=dialect) as table:
        assert table.read(keyed=True) == [
            {
                # general
                "empty": None,
                # numeric
                "0": "1001",
                "0.00": "1000.56",
                "0.0000": "1000.5577",
                "0.00000": "1000.55770",
                "0.0000#": "1000.5577",
                # temporal
                "m/d/yy": "5/20/40",
                "d-mmm": "20-May",
                "mm/dd/yy": "05/20/40",
                "mmddyy": "052040",
                "mmddyyam/pmdd": "052040AM20",
            }
        ]


def test_table_xlsx_preserve_formatting_percentage():
    source = "data/special/preserve-formatting-percentage.xlsx"
    dialect = dialects.ExcelDialect(preserve_formatting=True)
    with Table(source, headers=1, dialect=dialect) as table:
        assert table.read() == [
            [123, "52.00%"],
            [456, "30.00%"],
            [789, "6.00%"],
        ]


def test_table_xlsx_preserve_formatting_number_multicode():
    source = "data/special/number_format_multicode.xlsx"
    dialect = dialects.ExcelDialect(preserve_formatting=True)
    with Table(source, headers=1, ignore_blank_headers=True, dialect=dialect) as table:
        assert table.read() == [["4.5"], ["-9.032"], ["15.8"]]


@pytest.mark.skip
def test_table_xlsx_workbook_cache():
    source = BASE_URL % "data/special/sheets.xlsx"
    for sheet in ["Sheet1", "Sheet2", "Sheet3"]:
        dialect = dialects.ExcelDialect(sheet=sheet, workbook_cache={})
        with Table(source, sheet=sheet, dialect=dialect) as table:
            assert len(dialect.workbook_cache) == 1
            assert table.read()


def test_table_local_xls():
    with Table('data/table.xls') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


@pytest.mark.slow
def test_table_remote_xls():
    with Table(BASE_URL % 'data/table.xls') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_table_xls_sheet_by_index():
    source = 'data/special/sheet2.xls'
    dialect = dialects.ExcelDialect(sheet=2)
    with Table(source, dialect=dialect) as table:
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_table_xls_sheet_by_index_not_existent():
    source = 'data/special/sheet2.xls'
    dialect = dialects.ExcelDialect(sheet=3)
    with pytest.raises(exceptions.SourceError) as excinfo:
        Table(source, dialect=dialect).open()
    assert 'sheet "3"' in str(excinfo.value)


def test_table_xls_sheet_by_name():
    source = 'data/special/sheet2.xls'
    dialect = dialects.ExcelDialect(sheet='Sheet2')
    with Table(source, dialect=dialect) as table:
        assert table.read() == [['id', 'name'], [1, 'english'], [2, '中国人']]


def test_table_xls_sheet_by_name_not_existent():
    source = 'data/special/sheet2.xls'
    dialect = dialects.ExcelDialect(sheet='bad')
    with pytest.raises(exceptions.SourceError) as excinfo:
        Table(source, dialect=dialect).open()
    assert 'sheet "bad"' in str(excinfo.value)


def test_table_xls_merged_cells():
    source = 'data/special/merged-cells.xls'
    with Table(source) as table:
        assert table.read() == [['data', ''], ['', ''], ['', '']]


def test_table_xls_merged_cells_fill():
    source = 'data/special/merged-cells.xls'
    dialect = dialects.ExcelDialect(fill_merged_cells=True)
    with Table(source, dialect=dialect) as table:
        assert table.read() == [['data', 'data'], ['data', 'data'], ['data', 'data']]


def test_table_xls_with_boolean():
    with Table('data/special/table-with-booleans.xls') as table:
        assert table.headers is None
        assert table.read() == [['id', 'boolean'], [1, True], [2, False]]


def test_table_xlsx_merged_cells_boolean():
    source = 'data/special/merged-cells-boolean.xls'
    with Table(source) as table:
        assert table.read() == [[True, ''], ['', ''], ['', '']]


def test_table_xlsx_merged_cells_fill_boolean():
    source = 'data/special/merged-cells-boolean.xls'
    dialect = dialects.ExcelDialect(fill_merged_cells=True)
    with Table(source, dialect=dialect) as table:
        assert table.read() == [[True, True], [True, True], [True, True]]


def test_table_xls_with_ints_floats_dates():
    source = 'data/special/table-with-ints-floats-dates.xls'
    with Table(source) as table:
        assert table.read() == [
            ['Int', 'Float', 'Date'],
            [2013, 3.3, datetime(2009, 8, 16)],
            [1997, 5.6, datetime(2009, 9, 20)],
            [1969, 11.7, datetime(2012, 8, 23)],
        ]


@pytest.mark.slow
def test_fix_for_2007_xls():
    source = 'https://ams3.digitaloceanspaces.com/budgetkey-files/spending-reports/2018-3-משרד התרבות והספורט-לשכת הפרסום הממשלתית-2018-10-22-c457.xls'
    with Table(source) as table:
        assert len(table.read()) > 10


# Write


@pytest.mark.skip
def test_table_save_xlsx(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.xlsx"))
    with Table(source, headers=1) as table:
        assert table.save(target) == 2
    with Table(target, headers=1) as table:
        assert table.headers == ["id", "name"]
        assert table.read(extended=True) == [
            (2, ["id", "name"], ["1", "english"]),
            (3, ["id", "name"], ["2", "中国人"]),
        ]


@pytest.mark.skip
def test_table_save_xlsx_sheet_name(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.xlsx"))
    with Table(source, headers=1) as table:
        assert table.save(target, sheet="my-data") == 2
    with Table(target, headers=1, sheet="my-data") as table:
        assert table.headers == ["id", "name"]
        assert table.read(extended=True) == [
            (2, ["id", "name"], ["1", "english"]),
            (3, ["id", "name"], ["2", "中国人"]),
        ]
