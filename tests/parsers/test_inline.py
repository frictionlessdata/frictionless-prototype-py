import pytest
from collections import OrderedDict
from frictionless import Table, exceptions


# Read


def test_table_inline():
    source = [['id', 'name'], ['1', 'english'], ['2', '中国人']]
    with Table(source) as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


@pytest.mark.skip
def test_table_inline_iterator():
    source = iter([['id', 'name'], ['1', 'english'], ['2', '中国人']])
    with Table(source) as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_table_inline_generator_not_callable():
    def generator():
        yield ['id', 'name']
        yield ['1', 'english']
        yield ['2', '中国人']

    with pytest.raises(exceptions.SourceError) as excinfo:
        iterator = generator()
        Table(iterator).open()
    assert 'callable' in str(excinfo.value)


def test_table_inline_generator():
    def generator():
        yield ['id', 'name']
        yield ['1', 'english']
        yield ['2', '中国人']

    with Table(generator) as table:
        assert table.headers is None
        assert table.read() == [['id', 'name'], ['1', 'english'], ['2', '中国人']]


def test_table_inline_keyed():
    source = [{'id': '1', 'name': 'english'}, {'id': '2', 'name': '中国人'}]
    with Table(source, format='inline') as table:
        assert table.headers is None
        assert table.read() == [['1', 'english'], ['2', '中国人']]


def test_table_inline_keyed_with_headers_argument():
    source = [{'id': '1', 'name': 'english'}, {'id': '2', 'name': '中国人'}]
    with Table(source, format='inline', headers=['name', 'id']) as table:
        assert table.headers == ['name', 'id']
        assert table.read() == [['english', '1'], ['中国人', '2']]


def test_table_inline_ordered_dict():
    source = [
        OrderedDict([('name', 'english'), ('id', '1')]),
        OrderedDict([('name', '中国人'), ('id', '2')]),
    ]
    with Table(source, headers=1) as table:
        assert table.headers == ['name', 'id']
        assert table.read() == [['english', '1'], ['中国人', '2']]


# Write


def test_table_save_inline_keyed_with_headers_argument(tmpdir):
    source = [{'key1': 'value1', 'key2': 'value2'}]
    target = str(tmpdir.join('table.csv'))
    with Table(source, headers=['key2', 'key1']) as table:
        table.save(target)
    with Table(target, headers=1) as table:
        assert table.headers == ['key2', 'key1']
        assert table.read() == [['value2', 'value1']]
