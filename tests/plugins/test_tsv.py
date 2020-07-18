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


# Write


def test_table_csv_write(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.tsv"))
    with Table(source) as table:
        table.write(target)
    with Table(target) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]
