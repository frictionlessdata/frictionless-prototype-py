from frictionless import Table


# Read


def test_table_format_tsv():
    with Table("data/table.tsv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [
            ["1", "english"],
            ["2", "中国人"],
            ["3", None],
        ]
