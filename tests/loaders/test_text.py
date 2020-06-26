from frictionless import Table


# Read


def test_table_text():
    source = 'text://value1,value2\nvalue3,value4'
    with Table(source, format='csv') as table:
        assert table.read() == [['value1', 'value2'], ['value3', 'value4']]
