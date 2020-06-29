import io
import ast
import pytest
import datetime
from frictionless import Table, dialects, exceptions


# General


BASE_URL = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/%s"


def test_table():
    with Table("data/table.csv") as table:
        assert table.source == "data/table.csv"
        assert table.scheme == "file"
        assert table.format == "csv"
        assert table.encoding == "utf-8"
        assert table.compression == "no"
        assert table.dialect == {
            "delimiter": ",",
            "lineTerminator": "\r\n",
            "doubleQuote": True,
            "quoteChar": '"',
            "skipInitialSpace": False,
        }


# Headers


def test_table_headers():
    with Table("data/table.csv") as table:
        assert table.headers == ["id", "name"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


def test_table_headers_unicode():
    with Table("data/table-unicode-headers.csv") as table:
        assert table.headers == ["id", "国人"]
        assert table.read_data() == [["1", "english"], ["2", "中国人"]]


@pytest.mark.skip
def test_table_headers_user_set():
    source = [["1", "english"], ["2", "中国人"]]
    with Table(source, headers=["id", "name"]) as table:
        assert table.headers == ["id", "name"]
        assert list(table.iter(keyed=True)) == [
            {"id": "1", "name": "english"},
            {"id": "2", "name": "中国人"},
        ]


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


@pytest.mark.skip
def test_table_headers_xlsx_multiline():
    source = "data/special/multiline-headers.xlsx"
    with Table(source, headers=[1, 5], fill_merged_cells=True) as table:
        assert table.headers == [
            "Region",
            "Caloric contribution (%)",
            "Cumulative impact of changes on cost of food basket from previous quarter",
            "Cumulative impact of changes on cost of food basket from baseline (%)",
        ]
        assert table.read() == [["A", "B", "C", "D"]]


def test_table_headers_csv_multiline_headers_joiner():
    source = "text://k1\nk2\nv1\nv2\nv3"
    with Table(
        source, format="csv", headers=[1, 2], multiline_headers_joiner=":"
    ) as table:
        assert table.headers == ["k1:k2"]
        assert table.read() == [["v1"], ["v2"], ["v3"]]


def test_table_headers_csv_multiline_headers_duplicates():
    source = "text://k1\nk1\nv1\nv2\nv3"
    with Table(
        source, format="csv", headers=[1, 2], multiline_headers_duplicates=True
    ) as table:
        assert table.headers == ["k1 k1"]
        assert table.read() == [["v1"], ["v2"], ["v3"]]


def test_table_headers_strip_and_non_strings():
    source = [[" header ", 2, 3, None], ["value1", "value2", "value3", "value4"]]
    with Table(source, headers=1) as table:
        assert table.headers == ["header", "2", "3", ""]
        assert table.read() == [["value1", "value2", "value3", "value4"]]


# Compression errors


@pytest.mark.skip
def test_table_compression_error_gz():
    source = "id,filename\n\1,dump.tar.gz"
    table = Table(source, scheme="text", format="csv")
    table.open()


@pytest.mark.skip
def test_table_compression_error_zip():
    source = "id,filename\n1,archive.zip"
    table = Table(source, scheme="text", format="csv")
    table.open()


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


# Format


def test_table_format_csv():
    with Table("data/table.csv") as table:
        assert table.format == "csv"


def test_table_format_ndjson():
    with Table("data/table.ndjson") as table:
        assert table.format == "ndjson"


@pytest.mark.skip
def test_table_format_ods():
    with Table("data/table.ods") as table:
        assert table.format == "ods"


@pytest.mark.skip
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


# Encoding


def test_table_encoding():
    with Table("data/table.csv") as table:
        assert table.encoding == "utf-8"
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


def test_table_encoding_explicit_utf8():
    with Table("data/table.csv", encoding="utf-8") as table:
        assert table.encoding == "utf-8"
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


def test_table_encoding_explicit_latin1():
    with Table("data/special/latin1.csv", encoding="latin1") as table:
        assert table.encoding == "iso8859-1"
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "©"]]


def test_table_encoding_utf_16():
    # Bytes encoded as UTF-16 with BOM in platform order is detected
    bio = io.BytesIO(u"en,English\nja,日本語".encode("utf-16"))
    with Table(bio, format="csv") as table:
        assert table.encoding == "utf-16"
        assert table.read() == [[u"en", u"English"], [u"ja", u"日本語"]]


def test_table_encoding_missmatch_handle_errors():
    with pytest.raises(exceptions.EncodingError) as excinfo:
        with Table("data/table.csv", encoding="ascii") as table:
            table.read()
    assert (
        str(excinfo.value)
        == 'Cannot parse the source "data/table.csv" using "ascii" encoding at "20"'
    )


# Allow html


@pytest.mark.slow
def test_table_html_content():
    # Link to html file containing information about csv file
    source = "https://github.com/frictionlessdata/tabulator-py/blob/master/data/table.csv"
    with pytest.raises(exceptions.FormatError) as excinfo:
        Table(source).open()
    assert "HTML" in str(excinfo.value)


@pytest.mark.skip
@pytest.mark.slow
def test_table_html_content_with_allow_html():
    # Link to html file containing information about csv file
    source = "https://github.com/frictionlessdata/tabulator-py/blob/master/data/table.csv"
    with Table(source, allow_html=True) as table:
        assert table


# Sample size


def test_table_sample():
    source = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    with Table(source, headers=1) as table:
        assert table.headers == ["id", "name"]
        assert table.sample == [["1", "english"], ["2", "中国人"]]


# Bytes sample size


@pytest.mark.skip
def test_table_bytes_sample_size():
    source = "data/special/latin1.csv"
    with Table(source) as table:
        assert table.encoding == "iso8859-1"
    with Table(source, sample_size=0, bytes_sample_size=10) as table:
        assert table.encoding == "utf-8"


# Ignore blank headers


def test_table_ignore_blank_headers_false():
    source = "text://header1,,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1) as table:
        assert table.headers == ["header1", "", "header3"]
        assert table.read(keyed=True) == [
            {"header1": "value1", "": "value2", "header3": "value3"},
        ]


def test_table_ignore_blank_headers_true():
    source = "text://header1,,header3,,header5\nvalue1,value2,value3,value4,value5"
    data = [
        {"header1": "value1", "header3": "value3", "header5": "value5"},
    ]
    with Table(source, format="csv", headers=1, ignore_blank_headers=True) as table:
        assert table.headers == ["header1", "header3", "header5"]
        assert table.sample == [["value1", "value3", "value5"]]
        assert table.sample == [["value1", "value3", "value5"]]
        assert table.read(keyed=True) == data
        table.close()
        table.open()
        assert table.read(keyed=True) == data


# Ignore listed/not_listed headers


def test_table_ignore_listed_headers():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(
        source, format="csv", headers=1, ignore_listed_headers=["header2"]
    ) as table:
        assert table.headers == ["header1", "header3"]
        assert table.read(keyed=True) == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_ignore_not_listed_headers():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(
        source, format="csv", headers=1, ignore_not_listed_headers=["header2"]
    ) as table:
        assert table.headers == ["header2"]
        assert table.read(keyed=True) == [
            {"header2": "value2"},
        ]


# Pick/skip columns


def test_table_skip_columns():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, skip_columns=["header2"]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.read(keyed=True) == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_columns_blank_header():
    source = "text://header1,,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, skip_columns=[""]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.read(keyed=True) == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_pick_columns():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, pick_columns=["header2"]) as table:
        assert table.headers == ["header2"]
        assert table.read(keyed=True) == [
            {"header2": "value2"},
        ]


# Force strings


def test_table_force_strings():
    temp = datetime.datetime(2000, 1, 1, 17)
    date = datetime.date(2000, 1, 1)
    time = datetime.time(17, 00)
    source = [["John", 21, 1.5, temp, date, time]]
    with Table(source, force_strings=True) as table:
        assert table.read() == [
            ["John", "21", "1.5", "2000-01-01T17:00:00", "2000-01-01", "17:00:00"]
        ]
        assert table.sample == [
            ["John", "21", "1.5", "2000-01-01T17:00:00", "2000-01-01", "17:00:00"]
        ]


# Force parse


@pytest.mark.skip
def test_table_force_parse_inline():
    source = [["John", 21], "bad-row", ["Alex", 33]]
    with Table(source, force_parse=True) as table:
        assert table.read(extended=True) == [
            (1, None, ["John", 21]),
            (2, None, []),
            (3, None, ["Alex", 33]),
        ]


@pytest.mark.skip
def test_table_force_parse_json():
    source = '[["John", 21], "bad-row", ["Alex", 33]]'
    with Table(source, scheme="text", format="json", force_parse=True) as table:
        assert table.read(extended=True) == [
            (1, None, ["John", 21]),
            (2, None, []),
            (3, None, ["Alex", 33]),
        ]


# Skip/pick/limit/offset fields


def test_table_skip_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, skip_fields=["header2"]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.field_positions == [1, 3]
        assert table.read(keyed=True) == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_fields_position():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, skip_fields=[2]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.field_positions == [1, 3]
        assert table.read(keyed=True) == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_skip_fields_position_and_prefix():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, skip_fields=[2, "header3"]) as table:
        assert table.headers == ["header1"]
        assert table.field_positions == [1]
        assert table.read(keyed=True) == [
            {"header1": "value1"},
        ]


def test_table_skip_fields_blank_header():
    source = "text://header1,,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, skip_fields=[""]) as table:
        assert table.headers == ["header1", "header3"]
        assert table.field_positions == [1, 3]
        assert table.read(keyed=True) == [
            {"header1": "value1", "header3": "value3"},
        ]


def test_table_pick_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, pick_fields=["header2"]) as table:
        assert table.headers == ["header2"]
        assert table.field_positions == [2]
        assert table.read(keyed=True) == [
            {"header2": "value2"},
        ]


def test_table_pick_fields_position():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, pick_fields=[2]) as table:
        assert table.headers == ["header2"]
        assert table.field_positions == [2]
        assert table.read(keyed=True) == [
            {"header2": "value2"},
        ]


def test_table_pick_fields_position_and_prefix():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, pick_fields=[2, "header3"]) as table:
        assert table.headers == ["header2", "header3"]
        assert table.field_positions == [2, 3]
        assert table.read(keyed=True) == [
            {"header2": "value2", "header3": "value3"},
        ]


def test_table_pick_fields_keyed_source():
    source = [{"id": 1, "name": "london"}, {"id": 2, "name": "paris"}]
    with Table(source, headers=1, skip_fields=["id"]) as table:
        assert table.headers == ["name"]
        assert table.read() == [["london"], ["paris"]]
    with Table(source, headers=1, skip_fields=[1]) as table:
        assert table.headers == ["name"]
        assert table.read() == [["london"], ["paris"]]
    with Table(source, headers=1, skip_fields=["name"]) as table:
        assert table.headers == ["id"]
        assert table.read() == [[1], [2]]
    with Table(source, headers=1, skip_fields=[2]) as table:
        assert table.headers == ["id"]
        assert table.read() == [[1], [2]]


def test_table_limit_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, limit_fields=1) as table:
        assert table.headers == ["header1"]
        assert table.field_positions == [1]
        assert table.read(keyed=True) == [
            {"header1": "value1"},
        ]


def test_table_offset_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, offset_fields=1) as table:
        assert table.headers == ["header2", "header3"]
        assert table.field_positions == [2, 3]
        assert table.read(keyed=True) == [
            {"header2": "value2", "header3": "value3"},
        ]


def test_table_limit_offset_fields():
    source = "text://header1,header2,header3\nvalue1,value2,value3"
    with Table(source, format="csv", headers=1, limit_fields=1, offset_fields=1) as table:
        assert table.headers == ["header2"]
        assert table.field_positions == [2]
        assert table.read(keyed=True) == [
            {"header2": "value2"},
        ]


# Pick/skip/limit/offset rows


def test_table_pick_rows():
    source = "data/special/skip-rows.csv"
    with Table(source, pick_rows=["1", "2"]) as table:
        assert table.read() == [["1", "english"], ["2", "中国人"]]


def test_table_pick_rows_number():
    source = "data/special/skip-rows.csv"
    with Table(source, pick_rows=[3, 5]) as table:
        assert table.read() == [["1", "english"], ["2", "中国人"]]


def test_table_pick_rows_regex():
    source = [
        ["# comment"],
        ["name", "order"],
        ["# cat"],
        ["# dog"],
        ["John", 1],
        ["Alex", 2],
    ]
    pick_rows = [{"type": "regex", "value": r"^(name|John|Alex)"}]
    with Table(source, headers=1, pick_rows=pick_rows) as table:
        assert table.headers == ["name", "order"]
        assert table.read() == [["John", 1], ["Alex", 2]]


def test_table_skip_rows():
    source = "data/special/skip-rows.csv"
    with Table(source, skip_rows=["#", 5]) as table:
        assert table.read() == [["id", "name"], ["1", "english"]]


def test_table_skip_rows_from_the_end():
    source = "data/special/skip-rows.csv"
    with Table(source, skip_rows=[1, -2]) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]
    with Table(source, skip_rows=[1, -1, -2]) as table:
        assert table.read() == [["id", "name"], ["1", "english"]]


def test_table_skip_rows_no_double_skip():
    source = "data/special/skip-rows.csv"
    with Table(source, skip_rows=[1, 4, -2]) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]
    # no double skip at the very last row
    with Table(source, skip_rows=[1, 5, -1]) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["# it's a comment!"]]


def test_table_skip_rows_excel_empty_column():
    source = "data/special/skip-rows.xlsx"
    with Table(source, headers=1, skip_rows=[""]) as table:
        assert table.read() == [["A", "B"], [8, 9]]


def test_table_skip_rows_with_headers():
    source = "data/special/skip-rows.csv"
    with Table(source, headers=1, skip_rows=["#"]) as table:
        assert table.headers == ["id", "name"]
        assert table.read() == [["1", "english"], ["2", "中国人"]]


def test_table_skip_rows_with_headers_example_from_readme():
    source = [["#comment"], ["name", "order"], ["John", 1], ["Alex", 2]]
    with Table(source, headers=1, skip_rows=["#"]) as table:
        assert table.headers == ["name", "order"]
        assert table.read() == [["John", 1], ["Alex", 2]]


def test_table_skip_rows_regex():
    source = [
        ["# comment"],
        ["name", "order"],
        ["# cat"],
        ["# dog"],
        ["John", 1],
        ["Alex", 2],
    ]
    skip_rows = ["# comment", {"type": "regex", "value": r"^# (cat|dog)"}]
    with Table(source, headers=1, skip_rows=skip_rows) as table:
        assert table.headers == ["name", "order"]
        assert table.read() == [["John", 1], ["Alex", 2]]


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
    skip_rows = [{"type": "preset", "value": "blank"}]
    with Table(source, headers=1, skip_rows=skip_rows) as table:
        assert table.headers == ["name", "order"]
        assert table.read() == [["Ray", 0], ["John", 1], ["Alex", 2], ["", 3], [None, 4]]


def test_table_limit_rows():
    source = "data/special/long.csv"
    with Table(source, headers=1, limit_rows=1) as table:
        assert table.headers == ["id", "name"]
        assert table.read() == [["1", "a"]]


def test_table_offset_rows():
    source = "data/special/long.csv"
    with Table(source, headers=1, offset_rows=5) as table:
        assert table.headers == ["id", "name"]
        assert table.read() == [["6", "f"]]


def test_table_limit_offset_rows():
    source = "data/special/long.csv"
    with Table(source, headers=1, limit_rows=2, offset_rows=2) as table:
        assert table.headers == ["id", "name"]
        assert table.read() == [["3", "c"], ["4", "d"]]


# Post parse


def test_table_post_parse_headers():

    # Processors
    def extract_headers(extended_rows):
        headers = None
        for row_number, _, row in extended_rows:
            if row_number == 1:
                headers = row
                continue
            yield (row_number, headers, row)

    # Table
    source = [["id", "name"], ["1", "english"], ["2", "中国人"]]
    with Table(source, post_parse=[extract_headers]) as table:
        assert table.headers is None
        assert table.read(extended=True) == [
            (2, ["id", "name"], ["1", "english"]),
            (3, ["id", "name"], ["2", "中国人"]),
        ]


def test_table_post_parse_chain():

    # Processors
    def skip_commented_rows(extended_rows):
        for row_number, headers, row in extended_rows:
            if row and hasattr(row[0], "startswith") and row[0].startswith("#"):
                continue
            yield (row_number, headers, row)

    def skip_blank_rows(extended_rows):
        for row_number, headers, row in extended_rows:
            if not row:
                continue
            yield (row_number, headers, row)

    def cast_rows(extended_rows):
        for row_number, headers, row in extended_rows:
            crow = []
            for value in row:
                try:
                    if isinstance(value, str):
                        value = ast.literal_eval(value)
                except Exception:
                    pass
                crow.append(value)
            yield (row_number, headers, crow)

    # Table
    source = [["id", "name"], ["#1", "english"], [], ["2", "中国人"]]
    post_parse = [skip_commented_rows, skip_blank_rows, cast_rows]
    with Table(source, headers=1, post_parse=post_parse) as table:
        assert table.headers == ["id", "name"]
        assert table.read() == [[2, "中国人"]]


def test_table_post_parse_sample():

    # Processors
    def only_first_row(extended_rows):
        for row_number, header, row in extended_rows:
            if row_number == 1:
                yield (row_number, header, row)

    # Table
    with Table("data/table.csv", post_parse=[only_first_row]) as table:
        assert table.sample == [["id", "name"]]


# Custom loaders


@pytest.mark.skip
def test_table_custom_loaders():
    source = "custom://data/table.csv"

    #  class CustomLoader(LocalLoader):
    class CustomLoader:
        def load(self, source, *args, **kwargs):
            return super(CustomLoader, self).load(
                source.replace("custom://", ""), *args, **kwargs
            )

    with Table(source, custom_loaders={"custom": CustomLoader}) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


# Custom parsers


@pytest.mark.skip
def test_table_custom_parsers():
    source = "data/table.custom"

    #  class CustomParser(CsvParser):
    class CustomParser:
        def open(self, source, *args, **kwargs):
            return super(CustomParser, self).open(
                source.replace("custom", "csv"), *args, **kwargs
            )

    with Table(source, custom_parsers={"custom": CustomParser}) as table:
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


# Custom writers


@pytest.mark.skip
def test_table_save_custom_writers(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.csv"))

    #  class CustomWriter(CsvWriter):
    class CustomWriter:
        pass

    with Table(source, headers=1, custom_writers={"csv": CustomWriter}) as table:
        table.save(target)
    with Table(target, headers=1) as table:
        assert table.headers == ["id", "name"]
        assert table.read(extended=True) == [
            (2, ["id", "name"], ["1", "english"]),
            (3, ["id", "name"], ["2", "中国人"]),
        ]


# Loader/parser options


def test_table_json_property():
    source = '{"root": [["value1", "value2"], ["value3", "value4"]]}'
    dialect = {"property": "root"}
    with Table(source, scheme="text", format="json", dialect=dialect) as table:
        assert table.read() == [["value1", "value2"], ["value3", "value4"]]


# Open errors


def test_table_source_error_data():
    table = Table("[1,2]", scheme="text", format="json")
    with pytest.raises(exceptions.SourceError):
        table.open()
        table.read()


def test_table_format_error_html():
    table = Table("data/special/table.csv.html", format="csv")
    with pytest.raises(exceptions.FormatError):
        table.open()


@pytest.mark.skip
def test_table_scheme_error():
    table = Table("", scheme="bad-scheme")
    with pytest.raises(exceptions.SchemeError) as excinfo:
        table.open()
    assert "bad-scheme" in str(excinfo.value)


@pytest.mark.skip
def test_table_format_error():
    table = Table("data/special/table.bad-format")
    with pytest.raises(exceptions.FormatError) as excinfo:
        table.open()
    assert "bad-format" in str(excinfo.value)


@pytest.mark.skip
def test_table_bad_options_warning():
    Table("", scheme="text", format="csv", bad_option=True).open()
    with pytest.warns(UserWarning) as record:
        Table("", scheme="text", format="csv", bad_option=True).open()
    assert "bad_option" in str(record[0].message.args[0])


@pytest.mark.skip
def test_table_io_error():
    table = Table("bad_path.csv")
    with pytest.raises(exceptions.IOError) as excinfo:
        table.open()
    assert "bad_path.csv" in str(excinfo.value)


@pytest.mark.skip
def test_table_http_error():
    table = Table("http://github.com/bad_path.csv")
    with pytest.raises(exceptions.HTTPError):
        table.open()


# Stats


def test_table_size():
    with Table("data/special/doublequote.csv") as table:
        table.read()
        assert table.stats["bytes"] == 7346


def test_table_size_compressed():
    with Table("data/special/doublequote.csv.zip") as table:
        table.read()
        assert table.stats["bytes"] == 1265


@pytest.mark.slow
def test_table_size_remote():
    with Table(BASE_URL % "data/special/doublequote.csv") as table:
        table.read()
        assert table.stats["bytes"] == 7346


def test_table_hash():
    with Table("data/special/doublequote.csv") as table:
        table.read()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "d82306001266c4343a2af4830321ead8"


def test_table_hash_md5():
    with Table("data/special/doublequote.csv", hashing="md5") as table:
        table.read()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "d82306001266c4343a2af4830321ead8"


def test_table_hash_sha1():
    with Table("data/special/doublequote.csv", hashing="sha1") as table:
        table.read()
        assert table.hashing == "sha1"
        assert table.stats["hash"] == "2842768834a6804d8644dd689da61c7ab71cbb33"


def test_table_hash_sha256():
    with Table("data/special/doublequote.csv", hashing="sha256") as table:
        table.read()
        assert table.hashing == "sha256"
        assert (
            table.stats["hash"]
            == "41fdde1d8dbcb3b2d4a1410acd7ad842781f076076a73b049863d6c1c73868db"
        )


def test_table_hash_sha512():
    with Table("data/special/doublequote.csv", hashing="sha512") as table:
        table.read()
        assert table.hashing == "sha512"
        assert (
            table.stats["hash"]
            == "fa555b28a01959c8b03996cd4757542be86293fd49641d61808e4bf9fe4115619754aae9ae6af6a0695585eaade4488ce00dfc40fc4394b6376cd20d6967769c"
        )


@pytest.mark.skip
def test_table_hash_not_supported():
    with pytest.raises(AssertionError):
        with Table("data/special/doublequote.csv", hashing_algorithm="bad") as table:
            table.read()


def test_table_hash_compressed():
    with Table("data/special/doublequote.csv.zip") as table:
        table.read()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "2a72c90bd48c1fa48aec632db23ce8f7"


@pytest.mark.slow
def test_table_hash_remote():
    with Table(BASE_URL % "data/special/doublequote.csv") as table:
        table.read()
        assert table.hashing == "md5"
        assert table.stats["hash"] == "d82306001266c4343a2af4830321ead8"


# Field positions


def test_table_field_positions():
    with Table("data/table.csv", headers=1) as table:
        assert table.field_positions == [1, 2]


# Reset


def test_table_reset():
    with Table("data/table.csv", headers=1) as table:
        headers1 = table.headers
        contents1 = table.read()
        table.open()
        headers2 = table.headers
        contents2 = table.read()
        assert headers1 == ["id", "name"]
        assert contents1 == [["1", "english"], ["2", "中国人"]]
        assert headers1 == headers2
        assert contents1 == contents2


def test_table_reset_and_sample_size():
    with Table("data/special/long.csv", headers=1, sample_size=3) as table:
        # Before reset
        assert table.read(extended=True) == [
            (2, ["id", "name"], ["1", "a"]),
            (3, ["id", "name"], ["2", "b"]),
            (4, ["id", "name"], ["3", "c"]),
            (5, ["id", "name"], ["4", "d"]),
            (6, ["id", "name"], ["5", "e"]),
            (7, ["id", "name"], ["6", "f"]),
        ]
        assert table.sample == [["1", "a"], ["2", "b"]]
        assert table.read() == []
        # Reset table
        table.open()
        # After reset
        assert table.read(extended=True, limit=3) == [
            (2, ["id", "name"], ["1", "a"]),
            (3, ["id", "name"], ["2", "b"]),
            (4, ["id", "name"], ["3", "c"]),
        ]
        assert table.sample == [["1", "a"], ["2", "b"]]
        assert table.read(extended=True) == [
            (5, ["id", "name"], ["4", "d"]),
            (6, ["id", "name"], ["5", "e"]),
            (7, ["id", "name"], ["6", "f"]),
        ]


def test_table_reset_generator():
    def generator():
        yield [1]
        yield [2]

    with Table(generator, sample_size=0) as table:
        # Before reset
        assert table.read() == [[1], [2]]
        # Reset table
        table.open()
        # After reset
        assert table.read() == [[1], [2]]


# Save


@pytest.mark.skip
def test_table_save_xls_not_supported(tmpdir):
    source = "data/table.csv"
    target = str(tmpdir.join("table.xls"))
    with Table(source, headers=1) as table:
        with pytest.raises(exceptions.FormatError) as excinfo:
            table.save(target)
        assert "xls" in str(excinfo.value)


@pytest.mark.skip
def test_table_save_sqlite(database_url):
    source = "data/table.csv"
    with Table(source, headers=1) as table:
        table.save(database_url, table="test_stream_save_sqlite")
    with Table(
        database_url, table="test_table_save_sqlite", order_by="id", headers=1
    ) as table:
        assert table.read() == [["1", "english"], ["2", "中国人"]]
        assert table.headers == ["id", "name"]


# Reading closed


@pytest.mark.skip
def test_table_read_closed():
    table = Table("data/table.csv")
    with pytest.raises(exceptions.TabulatorException) as excinfo:
        table.read()
    assert "table.open()" in str(excinfo.value)


# Support for compressed files


def test_table_local_csv_zip():
    with Table("data/table.csv.zip") as table:
        assert table.headers is None
        assert table.compression == "zip"
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


def test_table_local_csv_zip_multiple_files():
    with Table("data/2-files.zip", compression_path="table.csv") as table:
        assert table.headers is None
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]
    with Table("data/2-files.zip", compression_path="table-reverse.csv") as table:
        assert table.headers is None
        assert table.read() == [["id", "name"], ["1", "中国人"], ["2", "english"]]


def test_table_local_csv_zip_multiple_open():
    # That's how `tableschema.iter()` acts
    table = Table("data/table.csv.zip")
    table.open()
    assert table.headers is None
    assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]
    table.close()
    table.open()
    assert table.headers is None
    assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]
    table.close()


def test_table_local_csv_gz():
    with Table("data/table.csv.gz") as table:
        assert table.headers is None
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


def test_table_filelike_csv_zip():
    with open("data/table.csv.zip", "rb") as file:
        with Table(file, format="csv", compression="zip") as table:
            assert table.headers is None
            assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


def test_table_filelike_csv_gz():
    with open("data/table.csv.gz", "rb") as file:
        with Table(file, format="csv", compression="gz") as table:
            assert table.headers is None
            assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


@pytest.mark.slow
def test_table_remote_csv_zip():
    source = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/data/table.csv.zip"
    with Table(source) as table:
        assert table.headers is None
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


@pytest.mark.slow
def test_table_remote_csv_gz():
    source = "https://raw.githubusercontent.com/frictionlessdata/tabulator-py/master/data/table.csv.gz"
    with Table(source) as table:
        assert table.headers is None
        assert table.read() == [["id", "name"], ["1", "english"], ["2", "中国人"]]


@pytest.mark.skip
def test_table_compression_invalid():
    with pytest.raises(exceptions.CompressionError) as excinfo:
        Table("table.csv", compression="bad").open()
    assert "bad" in str(excinfo.value)


# Issues


def test_table_reset_on_close_issue_190():
    source = [["1", "english"], ["2", "中国人"]]
    table = Table(source)
    table.open()
    table.read(limit=1) == [["1", "english"]]
    table.open()
    table.read(limit=1) == [["1", "english"]]
    table.close()


def test_table_skip_blank_at_the_end_issue_bco_dmo_33():
    source = "data/special/skip-blank-at-the-end.csv"
    with Table(source, headers=1, skip_rows=["#"]) as table:
        assert table.headers == ["test1", "test2"]
        assert table.read() == [["1", "2"], []]


@pytest.mark.skip
def test_table_not_existent_local_file_with_no_format_issue_287():
    with pytest.raises(exceptions.IOError) as excinfo:
        Table("bad-path").open()
    assert "bad-path" in str(excinfo.value)


@pytest.mark.skip
def test_table_not_existent_remote_file_with_no_format_issue_287():
    with pytest.raises(exceptions.HTTPError) as excinfo:
        Table("http://example.com/bad-path").open()
    assert "bad-path" in str(excinfo.value)


@pytest.mark.slow
def test_table_chardet_raises_remote_issue_305():
    source = "https://gist.githubusercontent.com/roll/56b91d7d998c4df2d4b4aeeefc18cab5/raw/a7a577cd30139b3396151d43ba245ac94d8ddf53/tabulator-issue-305.csv"
    with Table(source, headers=1) as table:
        assert table.encoding == "utf-8"
        assert len(table.read()) == 343


@pytest.mark.skip
def test_table_wrong_encoding_detection_issue_265():
    with Table("data/special/accent.csv") as table:
        assert table.encoding == "utf-8"


def test_table_skip_rows_non_string_cell_issue_322():
    source = [["id", "name"], [1, "english"], [2, "spanish"]]
    with Table(source, headers=1, skip_rows="1") as table:
        assert table.headers == ["id", "name"]
        assert table.read() == [[2, "spanish"]]


def test_table_skip_rows_non_string_cell_issue_320():
    source = "data/special/issue320.xlsx"
    dialect = dialects.ExcelDialect(fill_merged_cells=True)
    with Table(source, dialect=dialect, headers=[10, 12]) as table:
        assert table.headers[7] == "Current Population Analysed % of total county Pop"

