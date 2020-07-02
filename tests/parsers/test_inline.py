import pytest
from collections import OrderedDict
from frictionless import Table


# Read


def test_table_inline():
    source = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    with Table(source) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_inline_iterator():
    source = iter([["id", "name"], ["1", "english"], ["2", "中国人"]])
    with Table(source) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_inline_generator_not_callable():
    def generator():
        yield ["id", "name"]
        yield ["1", "english"]
        yield ["2", "中国人"]

    with Table(generator()) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_inline_generator():
    def generator():
        yield ["id", "name"]
        yield ["1", "english"]
        yield ["2", "中国人"]

    with Table(generator) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_inline_keyed():
    source = [{"id": "1", "name": "english"}, {"id": "2", "name": "中国人"}]
    with Table(source, format="inline") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_inline_keyed_with_headers_argument():
    source = [{"id": "1", "name": "english"}, {"id": "2", "name": "中国人"}]
    with Table(source, format="inline") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_inline_ordered_dict():
    source = [
        OrderedDict([("name", "english"), ("id", "1")]),
        OrderedDict([("name", "中国人"), ("id", "2")]),
    ]
    with Table(source) as table:
        assert table.headers == ["name", "id"]
        assert table.read_data() == [["english", "1"], ["中国人", "2"]]
