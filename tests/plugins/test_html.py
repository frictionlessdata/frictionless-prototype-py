import pytest
from frictionless import Table


# Read


@pytest.mark.parametrize(
    'source, selector',
    [
        ('data/table1.html', 'table'),
        ('data/table2.html', 'table'),
        ('data/table3.html', '.mememe'),
        ('data/table4.html', ''),
    ],
)
def test_table_html(source, selector):
    with Table(source, selector=selector, headers=1, encoding='utf8') as table:
        assert table.headers == ['id', 'name']
        assert table.read(keyed=True) == [
            {'id': '1', 'name': 'english'},
            {'id': '2', 'name': '中国人'},
        ]
