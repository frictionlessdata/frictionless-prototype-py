from frictionless import File


# General


BASE_URL = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/%s"


def test_file():
    with File("data/table.csv") as file:
        assert file.path == "data/table.csv"
        assert file.source == "data/table.csv"
        assert file.scheme == "file"
        assert file.format == "csv"
        assert file.encoding == "utf-8"
        assert file.compression == "no"
        assert file.compression_path == ""
        assert file.read_text() == "id,name\n1,english\n2,中国人\n"
        assert file.stats == {
            "hash": "6c2c61dd9b0e9c6876139a449ed87933",
            "bytes": 30,
            "rows": 0,
        }
