<a name="frictionless"></a>
# frictionless

<a name="frictionless.file"></a>
# frictionless.file

<a name="frictionless.pipeline"></a>
# frictionless.pipeline

<a name="frictionless.pipeline.Pipeline"></a>
## Pipeline Objects

```python
class Pipeline(Metadata)
```

Pipeline representation

<a name="frictionless.table"></a>
# frictionless.table

<a name="frictionless.table.Table"></a>
## Table Objects

```python
class Table()
```

Table representation

This is the main `tabulator` class. It loads a data source, and allows you
to stream its parsed contents.

__Arguments__


- __source__ (`str`):
        Path to file as ``<scheme>\\://path/to/file.<format>``.
        If not explicitly set, the scheme (file, http, ...) and
        format (csv, xls, ...) are inferred from the source string.

- __headers__ (`Union[int, List[int], List[str]], optional`):
        Either a row
        number or list of row numbers (in case of multi-line headers) to be
        considered as headers (rows start counting at 1), or the actual
        headers defined a list of strings. If not set, all rows will be
        treated as containing values.

- __scheme__ (`str, optional`):
        Scheme for loading the file (file, http, ...).
        If not set, it'll be inferred from `source`.

- __format__ (`str, optional`):
        File source's format (csv, xls, ...). If not
        set, it'll be inferred from `source`. inferred

- __encoding__ (`str, optional`):
        Source encoding. If not set, it'll be inferred.

- __compression__ (`str, optional`):
        Source file compression (zip, ...). If not set, it'll be inferred.

- __pick_rows__ (`List[Union[int, str, dict]], optional`):
        The same as `skip_rows` but it's for picking rows instead of skipping.

- __skip_rows__ (`List[Union[int, str, dict]], optional`):
        List of row numbers, strings and regex patterns as dicts to skip.
        If a string, it'll skip rows that their first cells begin with it e.g. '#' and '//'.
        To skip only completely blank rows use `{'type'\\: 'preset', 'value'\\: 'blank'}`
        To provide a regex pattern use  `{'type'\\: 'regex', 'value'\\: '^#'}`
        For example\\: `skip_rows=[1, '# comment', {'type'\\: 'regex', 'value'\\: '^# (regex|comment)'}]`

- __pick_fields__ (`List[Union[int, str]], optional`):
        When passed, ignores all columns with headers
        that the given list DOES NOT include

- __skip_fields__ (`List[Union[int, str]], optional`):
        When passed, ignores all columns with headers
        that the given list includes. If it contains an empty string it will skip
        empty headers

- __sample_size__ (`int, optional`):
        Controls the number of sample rows used to
        infer properties from the data (headers, encoding, etc.). Set to
        ``0`` to disable sampling, in which case nothing will be inferred
        from the data. Defaults to ``config.DEFAULT_SAMPLE_SIZE``.

- __allow_html__ (`bool, optional`):
        Allow the file source to be an HTML page.
        If False, raises ``exceptions.FormatError`` if the loaded file is
        an HTML page. Defaults to False.

- __multiline_headers_joiner__ (`str, optional`):
        When passed, it's used to join multiline headers
        as `<passed-value>.join(header1_1, header1_2)`
        Defaults to ' ' (space).

- __multiline_headers_duplicates__ (`bool, optional`):
        By default tabulator will exclude a cell of a miltilne header from joining
        if it's exactly the same as the previous seen value in this field.
        Enabling this option will force duplicates inclusion
        Defaults to False.

- __hashing_algorithm__ (`func, optional`):
- __It supports__: md5, sha1, sha256, sha512
        Defaults to sha256

- __force_strings__ (`bool, optional`):
        When True, casts all data to strings.
        Defaults to False.

- __post_parse__ (`List[function], optional`):
        List of generator functions that
        receives a list of rows and headers, processes them, and yields
        them (or not). Useful to pre-process the data. Defaults to None.

- __custom_loaders__ (`dict, optional`):
        Dictionary with keys as scheme names,
        and values as their respective ``Loader`` class implementations.
        Defaults to None.

- __custom_parsers__ (`dict, optional`):
        Dictionary with keys as format names,
        and values as their respective ``Parser`` class implementations.
        Defaults to None.

- __custom_loaders__ (`dict, optional`):
        Dictionary with keys as writer format
        names, and values as their respective ``Writer`` class
        implementations. Defaults to None.

- __**options (Any, optional)__: Extra options passed to the loaders and parsers.

<a name="frictionless.table.Table.path"></a>
#### path

```python
 | @property
 | path()
```

Path

__Returns__

`any`: stream path

<a name="frictionless.table.Table.source"></a>
#### source

```python
 | @property
 | source()
```

Source

__Returns__

`any`: stream source

<a name="frictionless.table.Table.scheme"></a>
#### scheme

```python
 | @property
 | scheme()
```

Path's scheme

__Returns__

`str`: scheme

<a name="frictionless.table.Table.format"></a>
#### format

```python
 | @property
 | format()
```

Path's format

__Returns__

`str`: format

<a name="frictionless.table.Table.hashing"></a>
#### hashing

```python
 | @property
 | hashing()
```

Stream's encoding

__Returns__

`str`: encoding

<a name="frictionless.table.Table.encoding"></a>
#### encoding

```python
 | @property
 | encoding()
```

Stream's encoding

__Returns__

`str`: encoding

<a name="frictionless.table.Table.compression"></a>
#### compression

```python
 | @property
 | compression()
```

Stream's compression ("no" if no compression)

__Returns__

`str`: compression

<a name="frictionless.table.Table.compression_path"></a>
#### compression\_path

```python
 | @property
 | compression_path()
```

Stream's compression path

__Returns__

`str`: compression

<a name="frictionless.table.Table.control"></a>
#### control

```python
 | @property
 | control()
```

Control (if available)

__Returns__

`dict/None`: dialect

<a name="frictionless.table.Table.dialect"></a>
#### dialect

```python
 | @property
 | dialect()
```

Dialect

__Returns__

`dict/None`: dialect

<a name="frictionless.table.Table.query"></a>
#### query

```python
 | @property
 | query()
```

Query

__Returns__

`dict/None`: query

<a name="frictionless.table.Table.schema"></a>
#### schema

```python
 | @property
 | schema()
```

Schema

__Returns__

`str[]/None`: schema

<a name="frictionless.table.Table.headers"></a>
#### headers

```python
 | @property
 | headers()
```

Headers

__Returns__

`str[]/None`: headers if available

<a name="frictionless.table.Table.sample"></a>
#### sample

```python
 | @property
 | sample()
```

Returns the stream's rows used as sample.

These sample rows are used internally to infer characteristics of the
source file (e.g. encoding, headers, ...).

__Returns__

`list[]`: sample

<a name="frictionless.table.Table.data_stream"></a>
#### data\_stream

```python
 | @property
 | data_stream()
```

Data stream

__Returns__

`str[]/None`: data_stream

<a name="frictionless.table.Table.row_stream"></a>
#### row\_stream

```python
 | @property
 | row_stream()
```

Row stream

__Returns__

`str[]/None`: row_stream

<a name="frictionless.table.Table.stats"></a>
#### stats

```python
 | @property
 | stats()
```

Returns stats

__Returns__

`int/None`: BYTE count

<a name="frictionless.table.Table.open"></a>
#### open

```python
 | open()
```

Opens the stream for reading.

# Raises:
TabulatorException: if an error

<a name="frictionless.table.Table.close"></a>
#### close

```python
 | close()
```

Closes the stream.

<a name="frictionless.row"></a>
# frictionless.row

<a name="frictionless.row.Row"></a>
## Row Objects

```python
class Row(OrderedDict)
```

Row representation

__Arguments__

    cells
    fields
    field_positions
    row_position
    row_number

<a name="frictionless.checks"></a>
# frictionless.checks

<a name="frictionless.checks.checksum"></a>
# frictionless.checks.checksum

<a name="frictionless.checks.regulation"></a>
# frictionless.checks.regulation

<a name="frictionless.checks.baseline"></a>
# frictionless.checks.baseline

<a name="frictionless.checks.heuristic"></a>
# frictionless.checks.heuristic

<a name="frictionless.package"></a>
# frictionless.package

<a name="frictionless.package.Package"></a>
## Package Objects

```python
class Package(Metadata)
```

Package representation

__Arguments__

- __descriptor? (str|dict)__: package descriptor

- __profile? (str)__: profile
- __resources? (dict[])__: list of resource descriptors

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.package.Package.resources"></a>
#### resources

```python
 | @Metadata.property
 | resources()
```

Package's resources

__Returns__

`Resource[]`: an array of resource instances

<a name="frictionless.package.Package.resource_names"></a>
#### resource\_names

```python
 | @Metadata.property(write=False)
 | resource_names()
```

Schema's resource names

__Returns__

`str[]`: an array of resource names

<a name="frictionless.package.Package.add_resource"></a>
#### add\_resource

```python
 | add_resource(descriptor)
```

Add new resource to schema.

The schema descriptor will be validated with newly added resource descriptor.

__Arguments__

- __descriptor__ (`dict`): resource descriptor

__Returns__

`Resource/None`: added `Resource` instance or `None` if not added

<a name="frictionless.package.Package.get_resource"></a>
#### get\_resource

```python
 | get_resource(name)
```

Get resource by name.

__Arguments__

- __name__ (`str`): resource name

__Returns__

`Resource/None`: `Resource` instance or `None` if not found

<a name="frictionless.package.Package.has_resource"></a>
#### has\_resource

```python
 | has_resource(name)
```

Check if a resource is present

__Arguments__

- __name__ (`str`): schema resource name

__Returns__

`bool`: whether there is the resource

<a name="frictionless.package.Package.remove_resource"></a>
#### remove\_resource

```python
 | remove_resource(name)
```

Remove resource by name.

The schema descriptor will be validated after resource descriptor removal.

__Arguments__

- __name__ (`str`): resource name

__Returns__

`Resource/None`: removed `Resource` instances or `None` if not found

<a name="frictionless.package.Package.expand"></a>
#### expand

```python
 | expand()
```

Expand the package

It will add default values to the package.

<a name="frictionless.plugin"></a>
# frictionless.plugin

<a name="frictionless.plugin.Plugin"></a>
## Plugin Objects

```python
class Plugin()
```

Plugin representation

<a name="frictionless.program"></a>
# frictionless.program

<a name="frictionless.program.main"></a>
# frictionless.program.main

<a name="frictionless.program.validate"></a>
# frictionless.program.validate

<a name="frictionless.program.transform"></a>
# frictionless.program.transform

<a name="frictionless.program.describe"></a>
# frictionless.program.describe

<a name="frictionless.program.api"></a>
# frictionless.program.api

<a name="frictionless.program.extract"></a>
# frictionless.program.extract

<a name="frictionless.type"></a>
# frictionless.type

<a name="frictionless.metadata"></a>
# frictionless.metadata

<a name="frictionless.metadata.Metadata"></a>
## Metadata Objects

```python
class Metadata(helpers.ControlledDict)
```

Metadata representation

__Arguments__

- __descriptor? (str|dict)__: schema descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.field"></a>
# frictionless.field

<a name="frictionless.field.Field"></a>
## Field Objects

```python
class Field(Metadata)
```

Field representation

__Arguments__

- __descriptor? (str|dict)__: field descriptor

- __name? (str)__: name
- __type? (str)__: type
- __format? (str)__: format
- __missing_values? (str[])__: missing_values
- __constraints? (dict)__: constraints
- __schema? (Schema)__: parent schema object

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.field.Field.read_cell"></a>
#### read\_cell

```python
 | read_cell(cell)
```

Read cell (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`(any, OrderedDict)`: processed cell and dict of notes

<a name="frictionless.field.Field.read_cell_cast"></a>
#### read\_cell\_cast

```python
 | read_cell_cast(cell)
```

Read cell low-level (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`any/None`: processed cell or None if an error

<a name="frictionless.field.Field.read_cell_checks"></a>
#### read\_cell\_checks

```python
 | @Metadata.property(write=False)
 | read_cell_checks()
```

Read cell low-level (cast)

__Returns__

`OrderedDict`: dictionlary of check function by a constraint name

<a name="frictionless.field.Field.write_cell"></a>
#### write\_cell

```python
 | write_cell(cell)
```

Write cell (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`(any, OrderedDict)`: processed cell and dict of notes

<a name="frictionless.field.Field.write_cell_cast"></a>
#### write\_cell\_cast

```python
 | write_cell_cast(cell)
```

Write cell low-level (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`any/None`: processed cell or None if an error

<a name="frictionless.headers"></a>
# frictionless.headers

<a name="frictionless.headers.Headers"></a>
## Headers Objects

```python
class Headers(list)
```

Headers representation

__Arguments__

    cells
    fields
    field_positions

<a name="frictionless.query"></a>
# frictionless.query

<a name="frictionless.query.Query"></a>
## Query Objects

```python
class Query(Metadata)
```

Query representation

<a name="frictionless.system"></a>
# frictionless.system

<a name="frictionless.system.System"></a>
## System Objects

```python
class System()
```

System representation

<a name="frictionless.helpers"></a>
# frictionless.helpers

<a name="frictionless.inquiry"></a>
# frictionless.inquiry

<a name="frictionless.inquiry.Inquiry"></a>
## Inquiry Objects

```python
class Inquiry(Metadata)
```

Inquiry representation.

**Arguments**:

- `descriptor?` _str|dict_ - schema descriptor
  

**Raises**:

- `FrictionlessException` - raise any error that occurs during the process

<a name="frictionless.config"></a>
# frictionless.config

<a name="frictionless.plugins"></a>
# frictionless.plugins

<a name="frictionless.plugins.gsheet"></a>
# frictionless.plugins.gsheet

<a name="frictionless.plugins.gsheet.GsheetDialect"></a>
## GsheetDialect Objects

```python
class GsheetDialect(Dialect)
```

Gsheet dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.dataflows"></a>
# frictionless.plugins.dataflows

<a name="frictionless.plugins.pandas"></a>
# frictionless.plugins.pandas

<a name="frictionless.plugins.spss"></a>
# frictionless.plugins.spss

<a name="frictionless.plugins.elastic"></a>
# frictionless.plugins.elastic

<a name="frictionless.plugins.bigquery"></a>
# frictionless.plugins.bigquery

<a name="frictionless.plugins.ods"></a>
# frictionless.plugins.ods

<a name="frictionless.plugins.ods.OdsDialect"></a>
## OdsDialect Objects

```python
class OdsDialect(Dialect)
```

Ods dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __sheet? (str)__: sheet

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.html"></a>
# frictionless.plugins.html

<a name="frictionless.plugins.html.HtmlDialect"></a>
## HtmlDialect Objects

```python
class HtmlDialect(Dialect)
```

Html dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __selector? (str)__: selector

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.tsv"></a>
# frictionless.plugins.tsv

<a name="frictionless.plugins.tsv.TsvDialect"></a>
## TsvDialect Objects

```python
class TsvDialect(Dialect)
```

Tsv dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.sql"></a>
# frictionless.plugins.sql

<a name="frictionless.plugins.sql.SqlDialect"></a>
## SqlDialect Objects

```python
class SqlDialect(Dialect)
```

Sql dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __table__ (`str`): table
- __order_by? (str)__: order_by

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.ckan"></a>
# frictionless.plugins.ckan

<a name="frictionless.plugins.server"></a>
# frictionless.plugins.server

<a name="frictionless.plugins.aws"></a>
# frictionless.plugins.aws

<a name="frictionless.plugins.aws.S3Control"></a>
## S3Control Objects

```python
class S3Control(Control)
```

S3 control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __endpoint_url? (string)__: endpoint_url
- __detect_encoding? (string)__: detect_encoding

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.parsers"></a>
# frictionless.parsers

<a name="frictionless.parsers.excel"></a>
# frictionless.parsers.excel

<a name="frictionless.parsers.excel.convert_excel_date_format_string"></a>
#### convert\_excel\_date\_format\_string

```python
convert_excel_date_format_string(excel_date)
```

Created using documentation here:
https://support.office.com/en-us/article/review-guidelines-for-customizing-a-number-format-c0a1d1fa-d3f4-4018-96b7-9c9354dd99f5

<a name="frictionless.parsers.excel.convert_excel_number_format_string"></a>
#### convert\_excel\_number\_format\_string

```python
convert_excel_number_format_string(excel_number, value)
```

A basic attempt to convert excel number_format to a number string

The important goal here is to get proper amount of rounding

<a name="frictionless.parsers.csv"></a>
# frictionless.parsers.csv

<a name="frictionless.parsers.json"></a>
# frictionless.parsers.json

<a name="frictionless.parsers.inline"></a>
# frictionless.parsers.inline

<a name="frictionless.extract"></a>
# frictionless.extract

<a name="frictionless.extract.main"></a>
# frictionless.extract.main

<a name="frictionless.extract.table"></a>
# frictionless.extract.table

<a name="frictionless.extract.package"></a>
# frictionless.extract.package

<a name="frictionless.extract.resource"></a>
# frictionless.extract.resource

<a name="frictionless.errors"></a>
# frictionless.errors

<a name="frictionless.errors.Error"></a>
## Error Objects

```python
class Error(Metadata)
```

Error representation

__Arguments__

- __descriptor? (str|dict)__: error descriptor

- __note__ (`str`): note

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.errors.HeaderError"></a>
## HeaderError Objects

```python
class HeaderError(Error)
```

Header error representation

__Arguments__

- __descriptor? (str|dict)__: error descriptor

- __note__ (`str`): note
- __cells__ (`any[]`): cells
- __cell__ (`any`): cell
- __field_name__ (`str`): field_name
- __field_number__ (`int`): field_number
- __field_position__ (`int`): field_position

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.errors.RowError"></a>
## RowError Objects

```python
class RowError(Error)
```

Row error representation

__Arguments__

- __descriptor? (str|dict)__: error descriptor

- __cells__ (`any[]`): cells
- __row_number__ (`int`): row_number
- __row_position__ (`int`): row_position

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.errors.CellError"></a>
## CellError Objects

```python
class CellError(RowError)
```

Cell error representation

__Arguments__

- __descriptor? (str|dict)__: error descriptor

- __note__ (`str`): note
- __cells__ (`any[]`): cells
- __row_number__ (`int`): row_number
- __row_position__ (`int`): row_position
- __cell__ (`any`): cell
- __field_name__ (`str`): field_name
- __field_number__ (`int`): field_number
- __field_position__ (`int`): field_position

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.describe"></a>
# frictionless.describe

<a name="frictionless.describe.main"></a>
# frictionless.describe.main

<a name="frictionless.describe.package"></a>
# frictionless.describe.package

<a name="frictionless.describe.schema"></a>
# frictionless.describe.schema

<a name="frictionless.describe.resource"></a>
# frictionless.describe.resource

<a name="frictionless.schema"></a>
# frictionless.schema

<a name="frictionless.schema.Schema"></a>
## Schema Objects

```python
class Schema(Metadata)
```

Schema representation

__Arguments__

- __descriptor? (str|dict)__: schema descriptor

- __fields? (dict[])__: list of field descriptors
- __missing_values? (str[])__: missing_values
- __primary_key? (str[])__: primary_key
- __foreign_keys? (dict[])__: foreign_keys

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.schema.Schema.fields"></a>
#### fields

```python
 | @Metadata.property
 | fields()
```

Schema's fields

__Returns__

`Field[]`: an array of field instances

<a name="frictionless.schema.Schema.field_names"></a>
#### field\_names

```python
 | @Metadata.property(write=False)
 | field_names()
```

Schema's field names

__Returns__

`str[]`: an array of field names

<a name="frictionless.schema.Schema.add_field"></a>
#### add\_field

```python
 | add_field(descriptor)
```

Add new field to schema.

The schema descriptor will be validated with newly added field descriptor.

__Arguments__

- __descriptor__ (`dict`): field descriptor

__Returns__

`Field/None`: added `Field` instance or `None` if not added

<a name="frictionless.schema.Schema.get_field"></a>
#### get\_field

```python
 | get_field(name)
```

Get schema's field by name.

__Arguments__

- __name__ (`str`): schema field name

__Returns__

`Field/None`: `Field` instance or `None` if not found

<a name="frictionless.schema.Schema.has_field"></a>
#### has\_field

```python
 | has_field(name)
```

Check if a field is present

__Arguments__

- __name__ (`str`): schema field name

__Returns__

`bool`: whether there is the field

<a name="frictionless.schema.Schema.remove_field"></a>
#### remove\_field

```python
 | remove_field(name)
```

Remove field by name.

The schema descriptor will be validated after field descriptor removal.

__Arguments__

- __name__ (`str`): schema field name

__Returns__

`Field/None`: removed `Field` instances or `None` if not found

<a name="frictionless.schema.Schema.expand"></a>
#### expand

```python
 | expand()
```

Expand the schema

It will add default values to the schema.

<a name="frictionless.schema.Schema.infer"></a>
#### infer

```python
 | infer(sample, *, type=None, names=None, confidence=config.DEFAULT_INFER_CONFIDENCE, missing_values=config.DEFAULT_MISSING_VALUES)
```

Infer schema

__Arguments__

    sample
    names
    confidence

<a name="frictionless.schema.Schema.read_data"></a>
#### read\_data

```python
 | read_data(cells)
```

Read a list of cells (normalize/cast)

__Arguments__

- __cells__ (`any[]`): list of cells

__Returns__

`any[]`: list of processed cells

<a name="frictionless.schema.Schema.write_data"></a>
#### write\_data

```python
 | write_data(cells, *, native_types=[])
```

Write a list of cells (normalize/uncast)

__Arguments__

- __cells__ (`any[]`): list of cells

__Returns__

`any[]`: list of processed cells

<a name="frictionless.schema.Schema.from_sample"></a>
#### from\_sample

```python
 | @staticmethod
 | from_sample(sample, *, type=None, names=None, confidence=config.DEFAULT_INFER_CONFIDENCE, missing_values=config.DEFAULT_MISSING_VALUES)
```

Infer schema from sample

__Arguments__

    sample
    names
    confidence

<a name="frictionless.check"></a>
# frictionless.check

<a name="frictionless.check.Check"></a>
## Check Objects

```python
class Check(Metadata)
```

Check representation.

__Arguments__

- __descriptor? (str|dict)__: schema descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.resource"></a>
# frictionless.resource

<a name="frictionless.exceptions"></a>
# frictionless.exceptions

<a name="frictionless.exceptions.FrictionlessException"></a>
## FrictionlessException Objects

```python
class FrictionlessException(Exception)
```

Main Frictionless exception

__Arguments__

    error

<a name="frictionless.transform"></a>
# frictionless.transform

<a name="frictionless.transform.main"></a>
# frictionless.transform.main

<a name="frictionless.transform.package"></a>
# frictionless.transform.package

<a name="frictionless.transform.resource"></a>
# frictionless.transform.resource

<a name="frictionless.parser"></a>
# frictionless.parser

<a name="frictionless.parser.Parser"></a>
## Parser Objects

```python
class Parser()
```

Parser representation

__Arguments__

- __file__ (`File`): file

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.report"></a>
# frictionless.report

<a name="frictionless.report.Report"></a>
## Report Objects

```python
class Report(Metadata)
```

Report representation.

__Arguments__

- __descriptor? (str|dict)__: schema descriptor
    time
    errors
    tables

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.report.Report.flatten"></a>
#### flatten

```python
 | flatten(spec)
```

Flatten the report

__Arguments__

    spec

<a name="frictionless.report.ReportTable"></a>
## ReportTable Objects

```python
class ReportTable(Metadata)
```

Report table representation.

__Arguments__

- __descriptor? (str|dict)__: schema descriptor
    time
    scope
    partial
    row_count
    path
    scheme
    format
    encoding
    compression
    headers
    headers_row
    headers_joiner
    pick_fields
    skip_fields
    limit_fields
    offset_fields
    pick_rows
    skip_rows
    limit_rows
    offset_rows
    schema
    dialect
    errors

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.report.ReportTable.flatten"></a>
#### flatten

```python
 | flatten(spec)
```

Flatten the report table

__Arguments__

    spec

<a name="frictionless.dialects"></a>
# frictionless.dialects

<a name="frictionless.dialects.Dialect"></a>
## Dialect Objects

```python
class Dialect(Metadata)
```

Dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __headers? (int|list)__: headers

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.dialects.CsvDialect"></a>
## CsvDialect Objects

```python
class CsvDialect(Dialect)
```

Csv dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __delimiter? (str)__: delimiter
- __line_terminator? (str)__: line_terminator
- __quote_char? (str)__: quote_char
- __double_quote? (bool)__: double_quote
- __escape_char? (str)__: escape_char
- __null_sequence? (str)__: null_sequence
- __skip_initial_space? (bool)__: skip_initial_space
- __comment_char? (str)__: comment_char
- __case_sensitive_header? (bool)__: case_sensitive_header

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.dialects.ExcelDialect"></a>
## ExcelDialect Objects

```python
class ExcelDialect(Dialect)
```

Excel dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __sheet? (int|str)__: sheet
- __workbook_cache? (dict)__: workbook_cache
- __fill_merged_cells? (bool)__: fill_merged_cells
- __preserve_formatting? (bool)__: preserve_formatting
- __adjust_floating_point_error? (bool)__: adjust_floating_point_error

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.dialects.InlineDialect"></a>
## InlineDialect Objects

```python
class InlineDialect(Dialect)
```

Inline dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __keyed? (bool)__: keyed

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.dialects.JsonDialect"></a>
## JsonDialect Objects

```python
class JsonDialect(Dialect)
```

Json dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __keyed? (bool)__: keyed
- __property? (str)__: property

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.__main__"></a>
# frictionless.\_\_main\_\_

<a name="frictionless.server"></a>
# frictionless.server

<a name="frictionless.server.Server"></a>
## Server Objects

```python
class Server()
```

Server representation

<a name="frictionless.types"></a>
# frictionless.types

<a name="frictionless.types.yearmonth"></a>
# frictionless.types.yearmonth

<a name="frictionless.types.datetime"></a>
# frictionless.types.datetime

<a name="frictionless.types.date"></a>
# frictionless.types.date

<a name="frictionless.types.string"></a>
# frictionless.types.string

<a name="frictionless.types.object"></a>
# frictionless.types.object

<a name="frictionless.types.geojson"></a>
# frictionless.types.geojson

<a name="frictionless.types.year"></a>
# frictionless.types.year

<a name="frictionless.types.integer"></a>
# frictionless.types.integer

<a name="frictionless.types.time"></a>
# frictionless.types.time

<a name="frictionless.types.geopoint"></a>
# frictionless.types.geopoint

<a name="frictionless.types.array"></a>
# frictionless.types.array

<a name="frictionless.types.boolean"></a>
# frictionless.types.boolean

<a name="frictionless.types.any"></a>
# frictionless.types.any

<a name="frictionless.types.duration"></a>
# frictionless.types.duration

<a name="frictionless.types.number"></a>
# frictionless.types.number

<a name="frictionless.loader"></a>
# frictionless.loader

<a name="frictionless.loader.Loader"></a>
## Loader Objects

```python
class Loader()
```

Loader representation

__Arguments__

- __file__ (`File`): file

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.loader.Loader.read_byte_stream"></a>
#### read\_byte\_stream

```python
 | read_byte_stream()
```

Create bytes stream

__Returns__

`BinaryIO`: I/O stream

<a name="frictionless.loader.Loader.read_text_stream"></a>
#### read\_text\_stream

```python
 | read_text_stream()
```

Create texts stream

__Returns__

`TextIO`: I/O stream

<a name="frictionless.validate"></a>
# frictionless.validate

<a name="frictionless.validate.main"></a>
# frictionless.validate.main

<a name="frictionless.validate.table"></a>
# frictionless.validate.table

<a name="frictionless.validate.table.validate_table"></a>
#### validate\_table

```python
@Report.from_validate
validate_table(source, *, headers=None, scheme=None, format=None, hashing=None, encoding=None, compression=None, compression_path=None, control=None, dialect=None, query=None, schema=None, sync_schema=False, patch_schema=False, infer_type=None, infer_names=None, infer_volume=config.DEFAULT_INFER_VOLUME, infer_confidence=config.DEFAULT_INFER_CONFIDENCE, infer_missing_values=config.DEFAULT_MISSING_VALUES, lookup=None, checksum=None, extra_checks=None, pick_errors=None, skip_errors=None, limit_errors=None, limit_memory=config.DEFAULT_LIMIT_MEMORY)
```

Validate table

__Arguments__

    source (any)

    scheme? (str)
    format? (str)
    hashing? (str)
    encoding? (str)
    compression? (str)
    compression_path? (str)
    dialect? (dict)
    control? (dict)

    headers? (int | int[])
    pick_fields? ((int | str)[])
    skip_fields? ((int | str)[])
    limit_fields? (int)
    offset_fields? (int)
    pick_rows? ((int | str)[])
    skip_rows? ((int | str)[])
    limit_rows? (int)
    offset_rows? (int)

    schema? (str | dict)
    sync_schema? (bool)
    patch_schema? (dict)
    infer_type? (str)
    infer_names? (str[])
    infer_volume? (int)
    infer_confidence? (float)

    stats? (dict)
    lookup? (dict)

    pick_errors? (str[])
    skip_errors? (str[])
    limit_errors? (int)
    extra_checks? (list)

__Returns__

    Report

<a name="frictionless.validate.package"></a>
# frictionless.validate.package

<a name="frictionless.validate.package.validate_package"></a>
#### validate\_package

```python
@Report.from_validate
validate_package(source, basepath=None, noinfer=False, **options)
```

Validate package

<a name="frictionless.validate.inquiry"></a>
# frictionless.validate.inquiry

<a name="frictionless.validate.inquiry.validate_inquiry"></a>
#### validate\_inquiry

```python
@Report.from_validate
validate_inquiry(source)
```

Validate inquiry

<a name="frictionless.validate.schema"></a>
# frictionless.validate.schema

<a name="frictionless.validate.schema.validate_schema"></a>
#### validate\_schema

```python
@Report.from_validate
validate_schema(source)
```

Validate schema

<a name="frictionless.validate.resource"></a>
# frictionless.validate.resource

<a name="frictionless.validate.resource.validate_resource"></a>
#### validate\_resource

```python
@Report.from_validate
validate_resource(source, basepath=None, noinfer=False, lookup=None, **options)
```

Validate resource

<a name="frictionless.controls"></a>
# frictionless.controls

<a name="frictionless.controls.Control"></a>
## Control Objects

```python
class Control(Metadata)
```

Control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __detectEncoding? (func)__: detectEncoding

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.controls.LocalControl"></a>
## LocalControl Objects

```python
class LocalControl(Control)
```

Local control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.controls.RemoteControl"></a>
## RemoteControl Objects

```python
class RemoteControl(Control)
```

Remote control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor
- __http_session? (any)__: http_session
- __http_preload? (bool)__: http_preload
- __http_timeout? (int)__: http_timeout
- __detectEncoding? (func)__: detectEncoding

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.controls.StreamControl"></a>
## StreamControl Objects

```python
class StreamControl(Control)
```

Stream control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.controls.TextControl"></a>
## TextControl Objects

```python
class TextControl(Control)
```

Text control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.loaders"></a>
# frictionless.loaders

<a name="frictionless.loaders.remote"></a>
# frictionless.loaders.remote

<a name="frictionless.loaders.stream"></a>
# frictionless.loaders.stream

<a name="frictionless.loaders.text"></a>
# frictionless.loaders.text

<a name="frictionless.loaders.local"></a>
# frictionless.loaders.local

