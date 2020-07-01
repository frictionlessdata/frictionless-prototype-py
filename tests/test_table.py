import io
import pytest
from frictionless import Table, controls, dialects, exceptions, describe


# General


BASE_URL = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/%s"


def test_table():
    with Table("data/table.csv") as table:
        assert table.source == "data/table.csv"
        assert table.scheme == "file"
        assert table.format == "csv"
        assert table.encoding == "utf-8"
        assert table.compression == "no"
        assert table.headers == ["id", "name"]
        assert table.sample == [["1", "english"], ["2", "中国人"]]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]
        assert table.schema == {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ]
        }


def test_table_read_data():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_data_stream():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert list(table.data_stream) == [["1", "english"], ["2", "中国人"]]
        assert list(table.data_stream) == []


def test_table_data_stream_iterate():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        for cells in table.data_stream:
            assert len(cells) == 2


def test_table_read_rows():
    with Table("data/table.csv") as table:
        headers = table.headers
        row1, row2 = table.read_rows()
        assert headers == ["id", "name"]
        assert headers.field_positions == [1, 2]
        assert headers.errors == []
        assert headers.valid is True
        assert row1 == {"id": 1, "name": "english"}
        assert row1.field_positions == [1, 2]
        assert row1.row_position == 2
        assert row1.row_number == 1
        assert row1.errors == []
        assert row1.valid is True
        assert row2 == {"id": 2, "name": "中国人"}
        assert row2.field_positions == [1, 2]
        assert row2.row_position == 3
        assert row2.row_number == 2
        assert row2.errors == []
        assert row2.valid is True


def test_table_row_stream():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert list(table.row_stream) == [
            {"id": 1, "name": "english"},
            {"id": 2, "name": "中国人"},
        ]
        assert list(table.row_stream) == []


def test_table_row_stream():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert list(table.data_stream) == [["1", "english"], ["2", "中国人"]]
        assert list(table.data_stream) == []


def test_table_row_stream_iterate():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        for row in table.row_stream:
            assert len(row) == 2
            assert row.row_number in [1, 2]
            if row.row_number == 1:
                assert row == {"id": 1, "name": "english"}
            if row.row_number == 2:
                assert row == {"id": 2, "name": "中国人"}


def test_table_row_stream_error_cells():
    with Table("data/table.csv", infer_type="integer") as table:
        row1, row2 = table.read_rows()
        assert table.headers == ["id", "name"]
        assert row1.errors[0].code == "type-error"
        assert row1.error_cells == {"name": "english"}
        assert row1 == {"id": 1, "name": None}
        assert row1.valid is False
        assert row2.errors[0].code == "type-error"
        assert row2.error_cells == {"name": "中国人"}
        assert row2 == {"id": 2, "name": None}
        assert row2.valid is False


def test_table_row_stream_blank_cells():
    patch_schema = {"missingValues": ["1", "2"]}
    with Table("data/table.csv", patch_schema=patch_schema) as table:
        row1, row2 = table.read_rows()
        assert table.headers == ["id", "name"]
        assert row1.blank_cells == {"id": "1"}
        assert row1 == {"id": None, "name": "english"}
        assert row1.valid is True
        assert row2.blank_cells == {"id": "2"}
        assert row2 == {"id": None, "name": "中国人"}
        assert row2.valid is True


def test_table_empty():
    with Table("data/empty.csv") as table:
        assert table.headers is None
        assert table.schema == {}
        assert table.read_data() == []


def test_table_without_rows():
    with Table("data/without-rows.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == []
        assert table.schema == {
            "fields": [{"name": "id", "type": "any"}, {"name": "name", "type": "any"}]
        }


def test_table_without_headers():
    with Table("data/without-headers.csv", headers=None) as table:
        assert table.headers is None
        assert table.read_data() == [["1", "english"], ["2", "中国人"], ["3", "german"]]
        assert table.schema == {
            "fields": [
                {"name": "field1", "type": "integer"},
                {"name": "field2", "type": "string"},
            ]
        }


def test_table_source_error_data():
    table = Table("[1,2]", scheme="text", format="json")
    with pytest.raises(exceptions.FrictionlessException):
        table.open()
        table.read_data()


def test_table_read_closed():
    table = Table("data/table.csv")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.read_data()
    assert "table.open()" in str(excinfo.value)


def test_table_reopen():
    with Table("data/table.csv") as table:
        headers1 = table.headers
        contents1 = table.read_data()
        table.open()
        headers2 = table.headers
        contents2 = table.read_data()
        assert headers1 == ["id", "name"]
        assert contents1 == [["1", "english"], ["2", "中国人"]]
        assert headers1 == headers2
        assert contents1 == contents2


def test_table_reopen_and_sample_size():
    with Table("data/special/long.csv", infer_volume=3) as table:
        # Before reset
        assert table.sample == [["1", "a"], ["2", "b"], ["3", "c"]]
        assert table.read_data() == [
            ["1", "a"],
            ["2", "b"],
            ["3", "c"],
            ["4", "d"],
            ["5", "e"],
            ["6", "f"],
        ]
        assert table.read_data() == []
        # Reopen table
        table.open()
        # After reopen
        assert table.sample == [["1", "a"], ["2", "b"], ["3", "c"]]
        assert table.read_data() == [
            ["1", "a"],
            ["2", "b"],
            ["3", "c"],
            ["4", "d"],
            ["5", "e"],
            ["6", "f"],
        ]


def test_table_reopen_generator():
    def generator():
        yield [1]
        yield [2]

    with Table(generator, headers=None) as table:
        # Before reopen
        assert table.read_data() == [[1], [2]]
        # Reset table
        table.open()
        # After reopen
        assert table.read_data() == [[1], [2]]


# Scheme


def test_table_scheme_file():
    with Table("data/table.csv") as table:
        assert table.scheme == "file"


@pytest.mark.slow
def test_table_scheme_https():
    with Table(BASE_URL % "data/table.csv") as table:
        assert table.scheme == "https"


def test_table_scheme_stream():
    with Table(io.open("data/table.csv", mode="rb"), format="csv") as table:
        assert table.scheme == "stream"


def test_table_scheme_text():
    with Table("text://a\nb", format="csv") as table:
        assert table.scheme == "text"


def test_table_scheme_error():
    table = Table("", scheme="bad")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert "bad" in str(excinfo.value)


def test_table_io_error():
    table = Table("bad.csv")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert "bad.csv" in str(excinfo.value)


def test_table_http_error():
    table = Table("http://github.com/bad.csv")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert "bad.csv" in str(excinfo.value)


# Format


def test_table_format_csv():
    with Table("data/table.csv") as table:
        assert table.format == "csv"


def test_table_format_ndjson():
    with Table("data/table.ndjson") as table:
        assert table.format == "ndjson"


def test_table_format_ods():
    with Table("data/table.ods") as table:
        assert table.format == "ods"


def test_table_format_tsv():
    with Table("data/table.tsv") as table:
        assert table.format == "tsv"


def test_table_format_xls():
    with Table("data/table.xls") as table:
        assert table.format == "xls"


def test_table_format_xlsx():
    with Table("data/table.xlsx") as table:
        assert table.format == "xlsx"


@pytest.mark.skip
def test_table_format_html():
    with Table("data/table1.html") as table:
        assert table.format == "html"


def test_table_format_error():
    table = Table("data/special/table.bad")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert "bad" in str(excinfo.value)


# Encoding


def test_table_encoding():
    with Table("data/table.csv") as table:
        assert table.encoding == "utf-8"
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_encoding_explicit_utf8():
    with Table("data/table.csv", encoding="utf-8") as table:
        assert table.encoding == "utf-8"
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_encoding_explicit_latin1():
    with Table("data/special/latin1.csv", encoding="latin1") as table:
        assert table.encoding == "iso8859-1"
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "©"]]


def test_table_encoding_utf_16():
    # Bytes encoded as UTF-16 with BOM in platform order is detected
    bio = io.BytesIO(u"en,English\nja,日本語".encode("utf-16"))
    with Table(bio, format="csv", headers=None) as table:
        assert table.encoding == "utf-16"
        assert table.read_data() == [[u"en", u"English"], [u"ja", u"日本語"]]


@pytest.mark.skip
def test_table_encoding_missmatch_handle_errors():
    table = Table("data/table.csv", encoding="ascii")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert (
        str(excinfo.value)
        == 'Cannot parse the source "data/table.csv" using "ascii" encoding at "20"'
    )


# Compression


def test_table_local_csv_zip():
    with Table("data/table.csv.zip") as table:
        assert table.compression == "zip"
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_local_csv_zip_multiple_files():
    with Table("data/2-files.zip", compression_path="table.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]
    with Table("data/2-files.zip", compression_path="table-reverse.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "中国人"], ["2", "english"]]


def test_table_local_csv_zip_multiple_open():
    # That's how `tableschema.iter()` acts
    table = Table("data/table.csv.zip")
    table.open()
    assert table.headers == ["id", "name"]
    assert table.read_data() == [["1", "english"], ["2", "中国人"]]
    table.close()
    table.open()
    assert table.headers == ["id", "name"]
    assert table.read_data() == [["1", "english"], ["2", "中国人"]]
    table.close()


def test_table_local_csv_gz():
    with Table("data/table.csv.gz") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_filelike_csv_zip():
    with open("data/table.csv.zip", "rb") as file:
        with Table(file, format="csv", compression="zip") as table:
            assert table.headers == ["id", "name"]
            assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_filelike_csv_gz():
    with open("data/table.csv.gz", "rb") as file:
        with Table(file, format="csv", compression="gz") as table:
            assert table.headers == ["id", "name"]
            assert table.read_data() == [["1", "english"], ["2", "中国人"]]


@pytest.mark.slow
def test_table_remote_csv_zip():
    source = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/data/table.csv.zip"
    with Table(source) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


@pytest.mark.slow
def test_table_remote_csv_gz():
    source = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/data/table.csv.gz"
    with Table(source) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


@pytest.mark.skip
def test_table_compression_invalid():
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        Table("table.csv", compression="bad").open()
    assert "bad" in str(excinfo.value)


@pytest.mark.skip
def test_table_compression_error_zip():
    source = "id,filename\n1,archive.zip"
    table = Table(source, scheme="text", format="csv")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert excinfo


@pytest.mark.skip
def test_table_compression_error_gz():
    source = "id,filename\n\1,dump.tar.gz"
    table = Table(source, scheme="text", format="csv")
    with pytest.raises(exceptions.FrictionlessException) as excinfo:
        table.open()
    assert excinfo


# Control


@pytest.mark.slow
def test_table_control():
    control = controls.RemoteControl(http_preload=True)
    with Table(BASE_URL % "data/table.csv", control=control) as table:
        assert table.headers == ["id", "name"]
        assert table.sample == [["1", "english"], ["2", "中国人"]]
        assert table.control == {"httpPreload": True}


def test_table_control_bad_property():
    table = Table(BASE_URL % "data/table.csv", control={"bad": True})
    with pytest.raises(exceptions.FrictionlessException):
        table.open()


# Dialect


def test_table_dialect():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]
        assert table.dialect == {
            "delimiter": ",",
            "lineTerminator": "\r\n",
            "doubleQuote": True,
            "quoteChar": '"',
            "skipInitialSpace": False,
        }


def test_table_dialect_json_property():
    source = '{"root": [["header1", "header2"], ["value1", "value2"]]}'
    dialect = dialects.JsonDialect(property="root")
    with Table(source, scheme="text", format="json", dialect=dialect) as table:
        assert table.headers == ["header1", "header2"]
        assert table.read_data() == [["value1", "value2"]]


def test_table_dialect_bad_property():
    table = Table("data/table.csv", dialect={"bad": True})
    with pytest.raises(exceptions.FrictionlessException):
        table.open()


# Headers


def test_table_headers():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_headers_unicode():
    with Table("data/table-unicode-headers.csv") as table:
        assert table.headers == ["id", "国人"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_headers_stream_context_manager():
    source = io.open("data/table.csv", mode="rb")
    with Table(source, format="csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_headers_inline():
    source = [[], ["id", "name"], ["1", "english"], ["2", "中国人"]]
    with Table(source, headers=2) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_headers_json_keyed():
    source = "text://[" '{"id": 1, "name": "english"},' '{"id": 2, "name": "中国人"}]'
    with Table(source, format="json") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[1, "english"], [2, "中国人"]]


def test_table_headers_inline_keyed():
    source = [{"id": "1", "name": "english"}, {"id": "2", "name": "中国人"}]
    with Table(source) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_headers_inline_keyed_headers_is_none():
    source = [{"id": "1", "name": "english"}, {"id": "2", "name": "中国人"}]
    with Table(source, headers=None) as table:
        assert table.headers is None
        assert table.read_data() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


def test_table_headers_xlsx_multiline():
    source = "data/special/multiline-headers.xlsx"
    dialect = dialects.ExcelDialect(fill_merged_cells=True)
    with Table(source, dialect=dialect, headers=[1, 2, 3, 4, 5]) as table:
        assert table.headers == [
            "Region",
            "Caloric contribution (%)",
            "Cumulative impact of changes on cost of food basket from previous quarter",
            "Cumulative impact of changes on cost of food basket from baseline (%)",
        ]
        assert table.read_data() == [["A", "B", "C", "D"]]


def test_table_headers_csv_multiline_headers_joiner():
    source = "text://k1\nk2\nv1\nv2\nv3"
    with Table(source, format="csv", headers=[[1, 2], ":"]) as table:
        assert table.headers == ["k1:k2"]
        assert table.read_data() == [["v1"], ["v2"], ["v3"]]


def test_table_headers_csv_multiline_headers_duplicates():
    source = "text://k1\nk1\nv1\nv2\nv3"
    with Table(source, format="csv", headers=[1, 2]) as table:
        assert table.headers == ["k1"]
        assert table.read_data() == [["v1"], ["v2"], ["v3"]]


def test_table_headers_strip_and_non_strings():
    source = [[" header ", 2, 3, None], ["value1", "value2", "value3", "value4"]]
    with Table(source) as table:
        assert table.headers == ["header", "2", "3", ""]
        assert table.read_data() == [["value1", "value2", "value3", "value4"]]


# Fields


def test_table_pick_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", pick_fields=["header2"]) as table:
        assert table.headers == ["header2"]
        assert table.headers.field_positions == [2]
        assert table.read_rows() == [
            {"header2": "value2"},
        ]


def test_table_pick_fields_position():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", pick_fields=[2]) as table:
        assert table.headers == ["header2"]
        assert table.headers.field_positions == [2]
        assert table.read_rows() == [
            {"header2": "value2"},
        ]


def test_table_pick_fields_regex():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", pick_fields=["<regex>header(2)"]) as table:
        assert table.headers == ["header2"]
        assert table.headers.field_positions == [2]
        assert table.read_rows() == [
            {"header2": "value2"},
        ]


def test_table_pick_fields_position_and_prefix():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", pick_fields=[2, "header3"]) as table:
        assert table.headers == ["header2", "header3"]
        assert table.headers.field_positions == [2, 3]
        assert table.read_rows() == [
            {"header2": "value2", "header3": "value3"},
        ]


def test_table_skip_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", skip_fields=["header2"]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.headers.field_positions == [1, 3]
        assert table.read_rows() == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_fields_position():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", skip_fields=[2]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.headers.field_positions == [1, 3]
        assert table.read_rows() == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_fields_regex():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", skip_fields=["<regex>header(1|3)"]) as table:
        assert table.headers == ["header2"]
        assert table.headers.field_positions == [2]
        assert table.read_rows() == [
            {"header2": "value2"},
        ]


def test_table_skip_fields_position_and_prefix():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", skip_fields=[2, "header3"]) as table:
        assert table.headers == ["header1"]
        assert table.headers.field_positions == [1]
        assert table.read_rows() == [
            {"header1": "value1"},
        ]


def test_table_skip_fields_blank_header():
    source = "text://header1,,header3\nvalue1,value2,value3"
    with Table(source, format="csv", skip_fields=[""]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.headers.field_positions == [1, 3]
        assert table.read_rows() == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_fields_blank_header_notation():
    source = "text://header1,,header3\nvalue1,value2,value3"
    with Table(source, format="csv", skip_fields=["<blank>"]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.headers.field_positions == [1, 3]
        assert table.read_rows() == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_fields_keyed_source():
    source = [{"id": 1, "name": "london"}, {"id": 2, "name": "paris"}]
    with Table(source, skip_fields=["id"]) as table:
        assert table.headers == ["name"]
        assert table.read_data() == [["london"], ["paris"]]
    with Table(source, skip_fields=[1]) as table:
        assert table.headers == ["name"]
        assert table.read_data() == [["london"], ["paris"]]
    with Table(source, skip_fields=["name"]) as table:
        assert table.headers == ["id"]
        assert table.read_data() == [[1], [2]]
    with Table(source, skip_fields=[2]) as table:
        assert table.headers == ["id"]
        assert table.read_data() == [[1], [2]]


def test_table_limit_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", limit_fields=1) as table:
        assert table.headers == ["header1"]
        assert table.headers.field_positions == [1]
        assert table.read_rows() == [
            {"header1": "value1"},
        ]


def test_table_offset_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", offset_fields=1) as table:
        assert table.headers == ["header2", "header3"]
        assert table.headers.field_positions == [2, 3]
        assert table.read_rows() == [
            {"header2": "value2", "header3": "value3"},
        ]


def test_table_limit_offset_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", limit_fields=1, offset_fields=1) as table:
        assert table.headers == ["header2"]
        assert table.headers.field_positions == [2]
        assert table.read_rows() == [
            {"header2": "value2"},
        ]


# Rows


def test_table_pick_rows():
    source = "data/special/skip-rows.csv"
    with Table(source, headers=None, pick_rows=["1", "2"]) as table:
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_pick_rows_number():
    source = "data/special/skip-rows.csv"
    with Table(source, headers=None, pick_rows=[3, 5]) as table:
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_pick_rows_regex():
    source = [
        ["# comment"],
        ["name", "order"],
        ["# cat"],
        ["# dog"],
        ["John", 1],
        ["Alex", 2],
    ]
    pick_rows = [r"<regex>(name|John|Alex)"]
    with Table(source, pick_rows=pick_rows) as table:
        assert table.headers == ["name", "order"]
        assert table.read_data() == [["John", 1], ["Alex", 2]]


def test_table_skip_rows():
    source = "data/special/skip-rows.csv"
    with Table(source, skip_rows=["#", 5]) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"]]


def test_table_skip_rows_excel_empty_column():
    source = "data/special/skip-rows.xlsx"
    with Table(source, skip_rows=[""]) as table:
        assert table.read_data() == [["A", "B"], [8, 9]]


def test_table_skip_rows_with_headers():
    source = "data/special/skip-rows.csv"
    with Table(source, skip_rows=["#"]) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_skip_rows_with_headers_example_from_readme():
    source = [["#comment"], ["name", "order"], ["John", 1], ["Alex", 2]]
    with Table(source, skip_rows=["#"]) as table:
        assert table.headers == ["name", "order"]
        assert table.read_data() == [["John", 1], ["Alex", 2]]


def test_table_skip_rows_regex():
    source = [
        ["# comment"],
        ["name", "order"],
        ["# cat"],
        ["# dog"],
        ["John", 1],
        ["Alex", 2],
    ]
    skip_rows = ["# comment", r"<regex># (cat|dog)"]
    with Table(source, skip_rows=skip_rows) as table:
        assert table.headers == ["name", "order"]
        assert table.read_data() == [["John", 1], ["Alex", 2]]


def test_table_skip_rows_preset():
    source = [
        ["name", "order"],
        ["", ""],
        [],
        ["Ray", 0],
        ["John", 1],
        ["Alex", 2],
        ["", 3],
        [None, 4],
        ["", None],
    ]
    with Table(source, skip_rows=["<blank>"]) as table:
        assert table.headers == ["name", "order"]
        assert table.read_data() == [
            ["Ray", 0],
            ["John", 1],
            ["Alex", 2],
            ["", 3],
            [None, 4],
        ]


def test_table_limit_rows():
    source = "data/special/long.csv"
    with Table(source, limit_rows=1) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "a"]]


def test_table_offset_rows():
    source = "data/special/long.csv"
    with Table(source, offset_rows=5) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["6", "f"]]


def test_table_limit_offset_rows():
    source = "data/special/long.csv"
    with Table(source, limit_rows=2, offset_rows=2) as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["3", "c"], ["4", "d"]]


# Schema


def test_table_schema():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.schema == {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ]
        }
        assert table.read_rows() == [
            {"id": 1, "name": "english"},
            {"id": 2, "name": "中国人"},
        ]


def test_table_schema_provided():
    schema = {
        "fields": [
            {"name": "new1", "type": "string"},
            {"name": "new2", "type": "string"},
        ]
    }
    with Table("data/table.csv", schema=schema) as table:
        assert table.headers == ["id", "name"]
        assert table.schema == schema
        assert table.read_rows() == [
            {"new1": "1", "new2": "english"},
            {"new1": "2", "new2": "中国人"},
        ]


def test_table_sync_schema():
    schema = describe("data/table.csv")
    with Table("data/sync-schema.csv", schema=schema, sync_schema=True) as table:
        assert table.headers == ["name", "id"]
        assert table.schema == {
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "id", "type": "integer"},
            ]
        }
        assert table.sample == [["english", "1"], ["中国人", "2"]]
        assert table.read_rows() == [
            {"id": 1, "name": "english"},
            {"id": 2, "name": "中国人"},
        ]


def test_table_schema_patch_schema():
    patch_schema = {"fields": {"id": {"name": "new", "type": "string"}}}
    with Table("data/table.csv", patch_schema=patch_schema) as table:
        assert table.headers == ["id", "name"]
        assert table.schema == {
            "fields": [
                {"name": "new", "type": "string"},
                {"name": "name", "type": "string"},
            ]
        }
        assert table.read_rows() == [
            {"new": "1", "name": "english"},
            {"new": "2", "name": "中国人"},
        ]


def test_table_schema_patch_schema_missing_values():
    patch_schema = {"missingValues": ["1", "2"]}
    with Table("data/table.csv", patch_schema=patch_schema) as table:
        assert table.headers == ["id", "name"]
        assert table.schema == {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
            ],
            "missingValues": ["1", "2"],
        }
        assert table.read_rows() == [
            {"id": None, "name": "english"},
            {"id": None, "name": "中国人"},
        ]


def test_table_schema_infer_type():
    with Table("data/table.csv", infer_type="string") as table:
        assert table.headers == ["id", "name"]
        assert table.schema == {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
            ]
        }
        assert table.read_rows() == [
            {"id": "1", "name": "english"},
            {"id": "2", "name": "中国人"},
        ]


def test_table_schema_infer_names():
    with Table("data/table.csv", infer_names=["new1", "new2"]) as table:
        assert table.headers == ["id", "name"]
        assert table.schema == {
            "fields": [
                {"name": "new1", "type": "integer"},
                {"name": "new2", "type": "string"},
            ]
        }
        assert table.read_rows() == [
            {"new1": 1, "new2": "english"},
            {"new1": 2, "new2": "中国人"},
        ]


# Stats


def test_table_size():
    with Table("data/special/doublequote.csv") as table:
        table.read_data()
        assert table.stats["bytes"] == 7346


def test_table_size_compressed():
    with Table("data/special/doublequote.csv.zip") as table:
        table.read_data()
        assert table.stats["bytes"] == 1265


@pytest.mark.slow
def test_table_size_remote():
    with Table(BASE_URL % "data/special/doublequote.csv") as table:
        table.read_data()
        assert table.stats["bytes"] == 7346


def test_table_hash():
    with Table("data/special/doublequote.csv") as table:
        table.read_data()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "d82306001266c4343a2af4830321ead8"


def test_table_hash_md5():
    with Table("data/special/doublequote.csv", hashing="md5") as table:
        table.read_data()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "d82306001266c4343a2af4830321ead8"


def test_table_hash_sha1():
    with Table("data/special/doublequote.csv", hashing="sha1") as table:
        table.read_data()
        assert table.hashing == "sha1"
        assert table.stats["hash"] == "2842768834a6804d8644dd689da61c7ab71cbb33"


def test_table_hash_sha256():
    with Table("data/special/doublequote.csv", hashing="sha256") as table:
        table.read_data()
        assert table.hashing == "sha256"
        assert (
            table.stats["hash"]
            == "41fdde1d8dbcb3b2d4a1410acd7ad842781f076076a73b049863d6c1c73868db"
        )


def test_table_hash_sha512():
    with Table("data/special/doublequote.csv", hashing="sha512") as table:
        table.read_data()
        assert table.hashing == "sha512"
        assert (
            table.stats["hash"]
            == "fa555b28a01959c8b03996cd4757542be86293fd49641d61808e4bf9fe4115619754aae9ae6af6a0695585eaade4488ce00dfc40fc4394b6376cd20d6967769c"
        )


def test_table_hash_not_supported():
    with pytest.raises(exceptions.FrictionlessException):
        with Table("data/special/doublequote.csv", hashing="bad") as table:
            table.read_data()


def test_table_hash_compressed():
    with Table("data/special/doublequote.csv.zip") as table:
        table.read_data()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "2a72c90bd48c1fa48aec632db23ce8f7"


@pytest.mark.slow
def test_table_hash_remote():
    with Table(BASE_URL % "data/special/doublequote.csv") as table:
        table.read_data()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "d82306001266c4343a2af4830321ead8"


# Save


@pytest.mark.skip
def test_table_save_xls_not_supported(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.xls"))
    with Table(source) as table:
        with pytest.raises(exceptions.FormatError) as excinfo:
            table.save(target)
        assert "xls" in str(excinfo.value)


@pytest.mark.skip
def test_table_save_sqlite(database_url):
    source = "data/table.csv"
    with Table(source) as table:
        table.save(database_url, table="test_stream_save_sqlite")
    with Table(database_url, table="test_table_save_sqlite", order_by="id") as table:
        assert table.read() == [["1", "english"], ["2", "中国人"]]
        assert table.headers == ["id", "name"]


# Issues


def test_table_reset_on_close_issue_190():
    source = [["1", "english"], ["2", "中国人"]]
    table = Table(source, headers=None, limit_rows=1)
    table.open()
    table.read_data() == [["1", "english"]]
    table.open()
    table.read_data() == [["1", "english"]]
    table.close()


def test_table_skip_blank_at_the_end_issue_bco_dmo_33():
    source = "data/special/skip-blank-at-the-end.csv"
    with Table(source, skip_rows=["#"]) as table:
        assert table.headers == ["test1", "test2"]
        assert table.read_data() == [["1", "2"], []]


@pytest.mark.skip
def test_table_not_existent_local_file_with_no_format_issue_287():
    with pytest.raises(exceptions.IOError) as excinfo:
        Table("bad").open()
    assert "bad" in str(excinfo.value)


@pytest.mark.skip
def test_table_not_existent_remote_file_with_no_format_issue_287():
    with pytest.raises(exceptions.HTTPError) as excinfo:
        Table("http://example.com/bad").open()
    assert "bad" in str(excinfo.value)


@pytest.mark.slow
def test_table_chardet_raises_remote_issue_305():
    source = "https://gist.githubusercontent.com/roll/56b91d7d998c4df2d4b4aeeefc18cab5/raw/a7a577cd30139b3396151d43ba245ac94d8ddf53/tabulator-issue-305.csv"
    with Table(source) as table:
        assert table.encoding == "utf-8"
        assert len(table.read_data()) == 343


def test_table_wrong_encoding_detection_issue_265():
    with Table("data/special/accent.csv") as table:
        #  Underlaying "chardet" can't detect correct utf-8 here
        assert table.encoding == "iso8859-1"


def test_table_skip_rows_non_string_cell_issue_322():
    source = [["id", "name"], [1, "english"], [2, "spanish"]]
    with Table(source, skip_rows="1") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [[2, "spanish"]]


def test_table_skip_rows_non_string_cell_issue_320():
    source = "data/special/issue320.xlsx"
    dialect = dialects.ExcelDialect(fill_merged_cells=True)
    with Table(source, dialect=dialect, headers=[10, 11, 12]) as table:
        assert table.headers[7] == "Current Population Analysed % of total county Pop"
