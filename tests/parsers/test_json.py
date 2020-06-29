import json
import pytest
from frictionless import Table

BASE_URL = "https://raw.githubusercontent.com/okfn/tabulator-py/master/%s"


# Read


def test_table_local_json_dicts():
    with Table("data/table-dicts.json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_local_json_lists():
    with Table("data/table-lists.json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_text_json_dicts():
    source = '[{"id": 1, "name": "english" }, {"id": 2, "name": "中国人" }]'
    with Table(source, scheme="text", format="json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


@pytest.mark.skip
def test_table_text_json_dicts_with_headers_argument():
    source = '[{"id": 1, "name": "english" }, {"id": 2, "name": "中国人" }]'
    with Table(source, scheme="text", format="json", headers=["name", "id"]) as table:
        assert table.headers == ["name", "id"]
        assert table.read() == [["english", 1], ["中国人", 2]]


def test_table_text_json_lists():
    source = '[["id", "name"], [1, "english"], [2, "中国人"]]'
    with Table(source, scheme="text", format="json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


@pytest.mark.slow
def test_table_remote_json_dicts():
    with Table(BASE_URL % "data/table-dicts.json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


@pytest.mark.slow
def test_table_remote_json_lists():
    with Table(BASE_URL % "data/table-lists.json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_ndjson():
    with Table("data/table.ndjson") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


# Write


@pytest.mark.skip
def test_table_save_json(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.json"))
    with Table(source, headers=1) as table:
        assert table.save(target) == 2
    with open(target) as file:
        assert json.load(file) == [
            ["id", "name"],
            ["1", "english"],
            ["2", "中国人"],
        ]


@pytest.mark.skip
def test_table_save_json_keyed(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.json"))
    with Table(source, headers=1) as table:
        assert table.save(target, keyed=True) == 2
    with open(target) as file:
        assert json.load(file) == [
            {"id": "1", "name": "english"},
            {"id": "2", "name": "中国人"},
        ]
