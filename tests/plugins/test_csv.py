import io
import pytest
from frictionless import Table, dialects

BASE_URL = 'https://raw.githubusercontent.com/okfn/tabulator-py/master/%s'


# Read


def test_table_local_csv():
    with Table('data/table.csv') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_table_local_csv_with_bom():
    with Table('data/special/bom.csv') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_table_local_csv_with_bom_with_encoding():
    with Table('data/special/bom.csv', encoding='utf-8') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_table_csv_excel():
    source = 'value1,value2\nvalue3,value4'
    with Table(source, scheme='text', format='csv') as table:
        assert table.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_table_csv_excel_tab():
    source = 'value1\tvalue2\nvalue3\tvalue4'
    dialect = dialects.CsvDialect(delimiter='\t')
    with Table(source, scheme='text', format='csv', dialect=dialect) as table:
        assert table.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_table_csv_unix():
    source = '"value1","value2"\n"value3","value4"'
    with Table(source, scheme='text', format='csv') as table:
        assert table.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_table_csv_escaping():
    dialect = dialects.CsvDialect(escape_char='\\')
    with Table('data/special/escaping.csv', dialect=dialect) as table:
        assert table.read() == [
            ['ID', 'Test'],
            ['1', 'Test line 1'],
            ['2', 'Test " line 2'],
            ['3', 'Test " line 3'],
        ]


def test_table_csv_doublequote():
    with Table('data/special/doublequote.csv') as table:
        for row in table:
            assert len(row) == 17


def test_table_table_csv():
    source = io.open('data/table.csv', mode='rb')
    with Table(source, format='csv') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_table_text_csv():
    source = 'text://id,name\n1,english\n2,中国人\n'
    with Table(source, format='csv') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.slow
def test_table_remote_csv():
    with Table(BASE_URL % 'data/table.csv') as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.slow
def test_table_remote_csv_non_ascii_url():
    with Table(
        'http://data.defra.gov.uk/ops/government_procurement_card/over_£500_GPC_apr_2013.csv'
    ) as table:
        assert table.sample[0] == [
            'Entity',
            'Transaction Posting Date',
            'Merchant Name',
            'Amount',
            'Description',
        ]


def test_table_csv_delimiter():
    source = '"value1";"value2"\n"value3";"value4"'
    dialect = dialects.CsvDialect(delimiter=';')
    with Table(source, scheme='text', format='csv', dialect=dialect) as table:
        assert table.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_table_csv_escapechar():
    source = 'value1%,value2\nvalue3%,value4'
    dialect = dialects.CsvDialect(escape_char='%')
    with Table(source, scheme='text', format='csv', dialect=dialect) as table:
        assert table.read() == [['value1,value2'], ['value3,value4']]


def test_table_csv_quotechar():
    source = '%value1,value2%\n%value3,value4%'
    dialect = dialects.CsvDialect(quote_char='%')
    with Table(source, scheme='text', format='csv', dialect=dialect) as table:
        assert table.read() == [['value1,value2'], ['value3,value4']]


def test_table_csv_skipinitialspace():
    source = 'value1, value2\nvalue3, value4'
    dialect = dialects.CsvDialect(skip_initial_space=False)
    with Table(source, scheme='text', format='csv', dialect=dialect) as table:
        assert table.read() == [['value1', ' value2'], ['value3', ' value4']]


def test_table_csv_skipinitialspace_default():
    source = 'value1, value2\nvalue3, value4'
    with Table(source, scheme='text', format='csv') as table:
        assert table.read() == [['value1', 'value2'], ['value3', 'value4']]


def test_table_csv_detect_delimiter_tab():
    source = 'a1\tb1\tc1A,c1B\na2\tb2\tc2\n'
    with Table(source, scheme='text', format='csv') as table:
        assert table.read() == [['a1', 'b1', 'c1A,c1B'], ['a2', 'b2', 'c2']]


def test_table_csv_detect_delimiter_semicolon():
    source = 'a1;b1\na2;b2\n'
    with Table(source, scheme='text', format='csv') as table:
        assert table.read() == [['a1', 'b1'], ['a2', 'b2']]


def test_table_csv_detect_delimiter_pipe():
    source = 'a1|b1\na2|b2\n'
    with Table(source, scheme='text', format='csv') as table:
        assert table.read() == [['a1', 'b1'], ['a2', 'b2']]


def test_table_csv_dialect_should_not_persist_if_sniffing_fails_issue_goodtables_228():
    source1 = 'a;b;c\n#comment'
    source2 = 'a,b,c\n#comment'
    dialect = dialects.CsvDialect(delimiter=';')
    with Table(source1, scheme='text', format='csv', headers=1, dialect=dialect) as table:
        assert table.headers == ['a', 'b', 'c']
    with Table(source2, scheme='text', format='csv', headers=1) as table:
        assert table.headers == ['a', 'b', 'c']


def test_table_csv_quotechar_is_empty_string():
    source = 'value1,value2",value3'
    with Table(source, scheme='text', format='csv', quotechar='') as table:
        table.read() == ['value1', 'value2"', 'value3']


# Write


@pytest.mark.skip
def test_table_save_csv(tmpdir):
    source = 'data/table.csv'
    target = str(tmpdir.join('table.csv'))
    with Table(source, headers=1) as table:
        assert table.save(target) == 2
    with Table(target, headers=1) as table:
        assert table.headers == ['id', 'name']
        assert table.read(extended=True) == [
            (2, ['id', 'name'], ['1', 'english']),
            (3, ['id', 'name'], ['2', '中国人']),
        ]
