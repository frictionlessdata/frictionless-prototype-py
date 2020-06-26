import pytest
from frictionless import Table


# Read


@pytest.mark.skip
def test_table_format_tsv():
    with Table('data/table.tsv') as table:
        assert table.headers is None
        assert table.read() == [
            ['id', 'name'],
            ['1', 'english'],
            ['2', '中国人'],
            ['3', None],
        ]
