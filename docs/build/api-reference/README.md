<a name="frictionless"></a>
# API Reference

<a name="frictionless.file"></a>
## frictionless.file

<a name="frictionless.file.File"></a>
### File

```python
class File(Metadata)
```

File representation

API      | Usage
-------- | --------
Public   | `from frictionless import File`

Under the hood, File uses available loaders so it can open from local, remote,
and any other supported schemes. The File class inherits from the Metadata class
all the metadata's functionality


```python
from frictionless import File

with File('data/text.txt') as file:
    file.read_text()
```

**Arguments**:

- `source` _any_ - file source
- `scheme?` _str_ - file scheme
- `format?` _str_ - file format
- `hashing?` _str_ - file hashing
- `encoding?` _str_ - file encoding
- `compression?` _str_ - file compression
- `compression_path?` _str_ - file compression path
- `control?` _dict_ - file control
- `dialect?` _dict_ - table dialect
- `query?` _dict_ - table query
- `newline?` _str_ - python newline e.g. '\n',
- `stats?` _{hash: str, bytes: int, rows: int}_ - stats object
  

**Raises**:

- `FrictionlessException` - if there is a metadata validation error

<a name="frictionless.file.File.path"></a>
#### <big>path</big>

```python
 | @property
 | path()
```

**Returns**:

- `str` - file path

<a name="frictionless.file.File.source"></a>
#### <big>source</big>

```python
 | @Metadata.property
 | source()
```

**Returns**:

- `any` - file source

<a name="frictionless.file.File.scheme"></a>
#### <big>scheme</big>

```python
 | @Metadata.property
 | scheme()
```

**Returns**:

- `str?` - file scheme

<a name="frictionless.file.File.format"></a>
#### <big>format</big>

```python
 | @Metadata.property
 | format()
```

**Returns**:

- `str?` - file format

<a name="frictionless.file.File.hashing"></a>
#### <big>hashing</big>

```python
 | @Metadata.property
 | hashing()
```

**Returns**:

- `str?` - file hashing

<a name="frictionless.file.File.encoding"></a>
#### <big>encoding</big>

```python
 | @Metadata.property
 | encoding()
```

**Returns**:

- `str?` - file encoding

<a name="frictionless.file.File.compression"></a>
#### <big>compression</big>

```python
 | @Metadata.property
 | compression()
```

**Returns**:

- `str?` - file compression

<a name="frictionless.file.File.compression_path"></a>
#### <big>compression\_path</big>

```python
 | @Metadata.property
 | compression_path()
```

**Returns**:

- `str?` - file compression path

<a name="frictionless.file.File.control"></a>
#### <big>control</big>

```python
 | @Metadata.property
 | control()
```

**Returns**:

- `Control?` - file control

<a name="frictionless.file.File.dialect"></a>
#### <big>dialect</big>

```python
 | @Metadata.property
 | dialect()
```

**Returns**:

- `Dialect?` - table dialect

<a name="frictionless.file.File.query"></a>
#### <big>query</big>

```python
 | @Metadata.property
 | query()
```

**Returns**:

- `Query?` - table query

<a name="frictionless.file.File.newline"></a>
#### <big>newline</big>

```python
 | @Metadata.property
 | newline()
```

**Returns**:

- `str?` - file newline

<a name="frictionless.file.File.stats"></a>
#### <big>stats</big>

```python
 | @Metadata.property
 | stats()
```

**Returns**:

- `dict` - file stats

<a name="frictionless.file.File.byte_stream"></a>
#### <big>byte\_stream</big>

```python
 | @Metadata.property(cache=False)
 | byte_stream()
```

File byte stream

The stream is available after opening the file

**Returns**:

- `io.ByteStream` - file byte stream

<a name="frictionless.file.File.text_stream"></a>
#### <big>text\_stream</big>

```python
 | @Metadata.property(cache=False)
 | text_stream()
```

File text stream

The stream is available after opening the file

**Returns**:

- `io.TextStream` - file text stream

<a name="frictionless.file.File.expand"></a>
#### <big>expand</big>

```python
 | expand()
```

Expand metadata

<a name="frictionless.file.File.open"></a>
#### <big>open</big>

```python
 | open()
```

Open the file as "io.open" does

<a name="frictionless.file.File.close"></a>
#### <big>close</big>

```python
 | close()
```

Close the file as "filelike.close" does

<a name="frictionless.file.File.closed"></a>
#### <big>closed</big>

```python
 | @property
 | closed()
```

Whether the file is closed

**Returns**:

- `bool` - if closed

<a name="frictionless.file.File.read_bytes"></a>
#### <big>read\_bytes</big>

```python
 | read_bytes()
```

Read bytes from the file

**Returns**:

- `bytes` - file bytes

<a name="frictionless.file.File.read_text"></a>
#### <big>read\_text</big>

```python
 | read_text()
```

Read bytes from the file

**Returns**:

- `str` - file text

<a name="frictionless.file.File.write"></a>
#### <big>write</big>

```python
 | write(target)
```

Write the file to the target

**Arguments**:

- `target` _str_ - target path

<a name="frictionless.pipeline"></a>
## frictionless.pipeline

<a name="frictionless.pipeline.Pipeline"></a>
### Pipeline

```python
class Pipeline(Metadata)
```

Pipeline representation

API      | Usage
-------- | --------
Public   | `from frictionless import Pipeline`

For now, only the `package` type is supported where `steps` should
conform to the `dataflows`s processors. The File class inherits
from the Metadata class all the metadata's functionality



```python
pipeline = Pipeline(
    {
        "type": "package",
        "steps": [
            {"type": "load", "spec": {"loadSource": "data/table.csv"}},
            {"type": "set_type", "spec": {"name": "id", "type": "string"}},
            {"type": "dump_to_path", "spec": {"outPath": tmpdir}},
        ],
    }
)
pipeline.run()
```

**Arguments**:

- `descriptor` _str|dict_ - pipeline descriptor
- `name?` _str_ - pipeline name
- `type?` _str_ - pipeline type
- `steps?` _dict[]_ - pipeline steps

<a name="frictionless.pipeline.Pipeline.name"></a>
#### <big>name</big>

```python
 | @Metadata.property
 | name()
```

**Returns**:

- `str?` - pipeline name

<a name="frictionless.pipeline.Pipeline.type"></a>
#### <big>type</big>

```python
 | @Metadata.property
 | type()
```

**Returns**:

- `str?` - pipeline type

<a name="frictionless.pipeline.Pipeline.steps"></a>
#### <big>steps</big>

```python
 | @Metadata.property
 | steps()
```

**Returns**:

- `dict[]?` - pipeline steps

<a name="frictionless.pipeline.Pipeline.run"></a>
#### <big>run</big>

```python
 | run()
```

Run the pipeline

<a name="frictionless.table"></a>
## frictionless.table

<a name="frictionless.table.Table"></a>
### Table

```python
class Table()
```

Table representation

API      | Usage
-------- | --------
Public   | `from frictionless import Table`

This class is at heart of the whole Frictionless framwork.
It loads a data source, and allows you to stream its parsed contents.


```python
with Table("data/table.csv") as table:
    assert table.headers == ["id", "name"]
    assert table.read_rows() == [
        {'id': 1, 'name': 'english'},
        {'id': 2, 'name': '中国人'},
    ]
```

**Arguments**:

  
- `source` _any_ - Source of the file; can be in various forms.
  Usually, it's a string as `<scheme>://path/to/file.<format>`.
  It also can be, for example, an array of data arrays/dictionaries.
  
- `headers?` _int|int[]|[int[], str]_ - Either a row
  number or list of row numbers (in case of multi-line headers) to be
  considered as headers (rows start counting at 1), or a pair
  where the first element is header rows and the second the
  header joiner.  It defaults to 1.
  
- `scheme?` _str_ - Scheme for loading the file (file, http, ...).
  If not set, it'll be inferred from `source`.
  
- `format?` _str_ - File source's format (csv, xls, ...).
  If not set, it'll be inferred from `source`.
  
- `encoding?` _str_ - An algorithm to hash data.
  It defaults to 'md5'.
  
- `encoding?` _str_ - Source encoding.
  If not set, it'll be inferred from `source`.
  
- `compression?` _str_ - Source file compression (zip, ...).
  If not set, it'll be inferred from `source`.
  
- `compression_path?` _str_ - A path within the compressed file.
  It defaults to the first file in the archive.
  
- `control?` _dict|Control_ - File control.
  For more infromation, please check the Control documentation.
  
- `query?` _dict|Query_ - Table query.
  For more infromation, please check the Query documentation.
  
- `dialect?` _dict|Dialect_ - Table dialect.
  For more infromation, please check the Dialect documentation.
  
- `schema?` _dict|Schema_ - Table schema.
  For more infromation, please check the Schema documentation.
  
- `sync_schema?` _bool_ - Whether to sync the schema.
  If it sets to `True` the provided schema will be mapped to
  the inferred schema. It means that, for example, you can
  provide a subset of fileds to be applied on top of the inferred
  fields or the provided schema can have different order of fields.
  
- `patch_schema?` _dict_ - A dictionary to be used as an inferred schema patch.
  The form of this dictionary should follow the Schema descriptor form
  except for the `fields` property which should be a mapping with the
  key named after a field name and the values being a field patch.
  For more information, please check "Extracting Data" guide.
  
- `infer_type?` _str_ - Enforce all the inferred types to be this type.
  For more information, please check "Describing  Data" guide.
  
- `infer_names?` _str[]_ - Enforce all the inferred fields to have provided names.
  For more information, please check "Describing  Data" guide.
  
- `infer_volume?` _int_ - The amount of rows to be extracted as a samle.
  For more information, please check "Describing  Data" guide.
  It defaults to 100
  
- `infer_confidence?` _float_ - A number from 0 to 1 setting the infer confidence.
  If  1 the data is guaranteed to be valid against the inferred schema.
  For more information, please check "Describing  Data" guide.
  It defaults to 0.9
  
- `infer_missing_values?` _str[]_ - String to be considered as missing values.
  For more information, please check "Describing  Data" guide.
  It defaults to `['']`
  
- `lookup?` _dict_ - The lookup is a special object providing relational information.
  For more information, please check "Extracting  Data" guide.

<a name="frictionless.table.Table.path"></a>
#### <big>path</big>

```python
 | @property
 | path()
```

**Returns**:

- `str` - file path

<a name="frictionless.table.Table.source"></a>
#### <big>source</big>

```python
 | @property
 | source()
```

**Returns**:

- `any` - file source

<a name="frictionless.table.Table.scheme"></a>
#### <big>scheme</big>

```python
 | @property
 | scheme()
```

**Returns**:

- `str?` - file scheme

<a name="frictionless.table.Table.format"></a>
#### <big>format</big>

```python
 | @property
 | format()
```

**Returns**:

- `str?` - file format

<a name="frictionless.table.Table.hashing"></a>
#### <big>hashing</big>

```python
 | @property
 | hashing()
```

**Returns**:

- `str?` - file hashing

<a name="frictionless.table.Table.encoding"></a>
#### <big>encoding</big>

```python
 | @property
 | encoding()
```

**Returns**:

- `str?` - file encoding

<a name="frictionless.table.Table.compression"></a>
#### <big>compression</big>

```python
 | @property
 | compression()
```

**Returns**:

- `str?` - file compression

<a name="frictionless.table.Table.compression_path"></a>
#### <big>compression\_path</big>

```python
 | @property
 | compression_path()
```

**Returns**:

- `str?` - file compression path

<a name="frictionless.table.Table.control"></a>
#### <big>control</big>

```python
 | @property
 | control()
```

**Returns**:

- `Control?` - file control

<a name="frictionless.table.Table.query"></a>
#### <big>query</big>

```python
 | @property
 | query()
```

**Returns**:

- `Query?` - table query

<a name="frictionless.table.Table.dialect"></a>
#### <big>dialect</big>

```python
 | @property
 | dialect()
```

**Returns**:

- `Dialect?` - table dialect

<a name="frictionless.table.Table.schema"></a>
#### <big>schema</big>

```python
 | @property
 | schema()
```

**Returns**:

- `Schema?` - table schema

<a name="frictionless.table.Table.headers"></a>
#### <big>headers</big>

```python
 | @property
 | headers()
```

**Returns**:

- `str[]?` - table headers

<a name="frictionless.table.Table.sample"></a>
#### <big>sample</big>

```python
 | @property
 | sample()
```

Tables's rows used as sample.

These sample rows are used internally to infer characteristics of the
source file (e.g. schema, ...).

**Returns**:

- `list[]?` - table sample

<a name="frictionless.table.Table.stats"></a>
#### <big>stats</big>

```python
 | @property
 | stats()
```

Table stats

The stats object has:
- hash: str - hashing sum
- bytes: int - number of bytes
- rows: int - number of rows

**Returns**:

- `dict?` - table stats

<a name="frictionless.table.Table.data_stream"></a>
#### <big>data\_stream</big>

```python
 | @property
 | data_stream()
```

Data stream in form of a generator of data arrays

**Yields**:

- `any[][]?` - data stream

<a name="frictionless.table.Table.row_stream"></a>
#### <big>row\_stream</big>

```python
 | @property
 | row_stream()
```

Row stream in form of a generator of Row objects

**Yields**:

- `Row[][]?` - row stream

<a name="frictionless.table.Table.open"></a>
#### <big>open</big>

```python
 | open()
```

Open the table as "io.open" does

**Raises**:

- `FrictionlessException` - any exception that occurs

<a name="frictionless.table.Table.close"></a>
#### <big>close</big>

```python
 | close()
```

Close the table as "filelike.close" does

<a name="frictionless.table.Table.closed"></a>
#### <big>closed</big>

```python
 | @property
 | closed()
```

Whether the table is closed

**Returns**:

- `bool` - if closed

<a name="frictionless.table.Table.read_data"></a>
#### <big>read\_data</big>

```python
 | read_data()
```

Read data stream into memory

**Returns**:

- `any[][]` - table data

<a name="frictionless.table.Table.read_rows"></a>
#### <big>read\_rows</big>

```python
 | read_rows()
```

Read row stream into memory

**Returns**:

- `Row[][]` - table rows

<a name="frictionless.table.Table.write"></a>
#### <big>write</big>

```python
 | write(target, *, scheme=None, format=None, hashing=None, encoding=None, compression=None, compression_path=None, control=None, dialect=None)
```

Write the table to the target

**Arguments**:

- `target` _str_ - target path
- `**options` - subset of Table's constructor options

<a name="frictionless.row"></a>
## frictionless.row

<a name="frictionless.row.Row"></a>
### Row

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
## frictionless.checks

<a name="frictionless.checks.checksum"></a>
## frictionless.checks.checksum

<a name="frictionless.checks.regulation"></a>
## frictionless.checks.regulation

<a name="frictionless.checks.baseline"></a>
## frictionless.checks.baseline

<a name="frictionless.checks.heuristic"></a>
## frictionless.checks.heuristic

<a name="frictionless.package"></a>
## frictionless.package

<a name="frictionless.package.Package"></a>
### Package

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
#### <big>resources</big>

```python
 | @Metadata.property
 | resources()
```

Package's resources

__Returns__

`Resource[]`: an array of resource instances

<a name="frictionless.package.Package.resource_names"></a>
#### <big>resource\_names</big>

```python
 | @Metadata.property(write=False)
 | resource_names()
```

Schema's resource names

__Returns__

`str[]`: an array of resource names

<a name="frictionless.package.Package.add_resource"></a>
#### <big>add\_resource</big>

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
#### <big>get\_resource</big>

```python
 | get_resource(name)
```

Get resource by name.

__Arguments__

- __name__ (`str`): resource name

__Returns__

`Resource/None`: `Resource` instance or `None` if not found

<a name="frictionless.package.Package.has_resource"></a>
#### <big>has\_resource</big>

```python
 | has_resource(name)
```

Check if a resource is present

__Arguments__

- __name__ (`str`): schema resource name

__Returns__

`bool`: whether there is the resource

<a name="frictionless.package.Package.remove_resource"></a>
#### <big>remove\_resource</big>

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
#### <big>expand</big>

```python
 | expand()
```

Expand the package

It will add default values to the package.

<a name="frictionless.plugin"></a>
## frictionless.plugin

<a name="frictionless.plugin.Plugin"></a>
### Plugin

```python
class Plugin()
```

Plugin representation

<a name="frictionless.program"></a>
## frictionless.program

<a name="frictionless.program.main"></a>
## frictionless.program.main

<a name="frictionless.program.validate"></a>
## frictionless.program.validate

<a name="frictionless.program.transform"></a>
## frictionless.program.transform

<a name="frictionless.program.describe"></a>
## frictionless.program.describe

<a name="frictionless.program.api"></a>
## frictionless.program.api

<a name="frictionless.program.extract"></a>
## frictionless.program.extract

<a name="frictionless.type"></a>
## frictionless.type

<a name="frictionless.metadata"></a>
## frictionless.metadata

<a name="frictionless.metadata.Metadata"></a>
### Metadata

```python
class Metadata(helpers.ControlledDict)
```

Metadata representation

__Arguments__

- __descriptor? (str|dict)__: schema descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.field"></a>
## frictionless.field

<a name="frictionless.field.Field"></a>
### Field

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
#### <big>read\_cell</big>

```python
 | read_cell(cell)
```

Read cell (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`(any, OrderedDict)`: processed cell and dict of notes

<a name="frictionless.field.Field.read_cell_cast"></a>
#### <big>read\_cell\_cast</big>

```python
 | read_cell_cast(cell)
```

Read cell low-level (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`any/None`: processed cell or None if an error

<a name="frictionless.field.Field.read_cell_checks"></a>
#### <big>read\_cell\_checks</big>

```python
 | @Metadata.property(write=False)
 | read_cell_checks()
```

Read cell low-level (cast)

__Returns__

`OrderedDict`: dictionlary of check function by a constraint name

<a name="frictionless.field.Field.write_cell"></a>
#### <big>write\_cell</big>

```python
 | write_cell(cell)
```

Write cell (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`(any, OrderedDict)`: processed cell and dict of notes

<a name="frictionless.field.Field.write_cell_cast"></a>
#### <big>write\_cell\_cast</big>

```python
 | write_cell_cast(cell)
```

Write cell low-level (cast)

__Arguments__

- __cell__ (`any`): cell

__Returns__

`any/None`: processed cell or None if an error

<a name="frictionless.headers"></a>
## frictionless.headers

<a name="frictionless.headers.Headers"></a>
### Headers

```python
class Headers(list)
```

Headers representation

__Arguments__

    cells
    fields
    field_positions

<a name="frictionless.query"></a>
## frictionless.query

<a name="frictionless.query.Query"></a>
### Query

```python
class Query(Metadata)
```

Query representation

<a name="frictionless.system"></a>
## frictionless.system

<a name="frictionless.system.System"></a>
### System

```python
class System()
```

System representation

<a name="frictionless.helpers"></a>
## frictionless.helpers

<a name="frictionless.inquiry"></a>
## frictionless.inquiry

<a name="frictionless.inquiry.Inquiry"></a>
### Inquiry

```python
class Inquiry(Metadata)
```

Inquiry representation.

**Arguments**:

- `descriptor?` _str|dict_ - schema descriptor
  

**Raises**:

- `FrictionlessException` - raise any error that occurs during the process

<a name="frictionless.config"></a>
## frictionless.config

<a name="frictionless.plugins"></a>
## frictionless.plugins

<a name="frictionless.plugins.gsheet"></a>
## frictionless.plugins.gsheet

<a name="frictionless.plugins.gsheet.GsheetDialect"></a>
### GsheetDialect

```python
class GsheetDialect(Dialect)
```

Gsheet dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.dataflows"></a>
## frictionless.plugins.dataflows

<a name="frictionless.plugins.pandas"></a>
## frictionless.plugins.pandas

<a name="frictionless.plugins.spss"></a>
## frictionless.plugins.spss

<a name="frictionless.plugins.elastic"></a>
## frictionless.plugins.elastic

<a name="frictionless.plugins.bigquery"></a>
## frictionless.plugins.bigquery

<a name="frictionless.plugins.ods"></a>
## frictionless.plugins.ods

<a name="frictionless.plugins.ods.OdsDialect"></a>
### OdsDialect

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
## frictionless.plugins.html

<a name="frictionless.plugins.html.HtmlDialect"></a>
### HtmlDialect

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
## frictionless.plugins.tsv

<a name="frictionless.plugins.tsv.TsvDialect"></a>
### TsvDialect

```python
class TsvDialect(Dialect)
```

Tsv dialect representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.plugins.sql"></a>
## frictionless.plugins.sql

<a name="frictionless.plugins.sql.SqlDialect"></a>
### SqlDialect

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
## frictionless.plugins.ckan

<a name="frictionless.plugins.server"></a>
## frictionless.plugins.server

<a name="frictionless.plugins.aws"></a>
## frictionless.plugins.aws

<a name="frictionless.plugins.aws.S3Control"></a>
### S3Control

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
## frictionless.parsers

<a name="frictionless.parsers.excel"></a>
## frictionless.parsers.excel

<a name="frictionless.parsers.excel.convert_excel_date_format_string"></a>
#### <big>convert\_excel\_date\_format\_string</big>

```python
convert_excel_date_format_string(excel_date)
```

Created using documentation here:
https://support.office.com/en-us/article/review-guidelines-for-customizing-a-number-format-c0a1d1fa-d3f4-4018-96b7-9c9354dd99f5

<a name="frictionless.parsers.excel.convert_excel_number_format_string"></a>
#### <big>convert\_excel\_number\_format\_string</big>

```python
convert_excel_number_format_string(excel_number, value)
```

A basic attempt to convert excel number_format to a number string

The important goal here is to get proper amount of rounding

<a name="frictionless.parsers.csv"></a>
## frictionless.parsers.csv

<a name="frictionless.parsers.json"></a>
## frictionless.parsers.json

<a name="frictionless.parsers.inline"></a>
## frictionless.parsers.inline

<a name="frictionless.extract"></a>
## frictionless.extract

<a name="frictionless.extract.main"></a>
## frictionless.extract.main

<a name="frictionless.extract.table"></a>
## frictionless.extract.table

<a name="frictionless.extract.package"></a>
## frictionless.extract.package

<a name="frictionless.extract.resource"></a>
## frictionless.extract.resource

<a name="frictionless.errors"></a>
## frictionless.errors

<a name="frictionless.errors.Error"></a>
### Error

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
### HeaderError

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
### RowError

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
### CellError

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
## frictionless.describe

<a name="frictionless.describe.main"></a>
## frictionless.describe.main

<a name="frictionless.describe.package"></a>
## frictionless.describe.package

<a name="frictionless.describe.schema"></a>
## frictionless.describe.schema

<a name="frictionless.describe.resource"></a>
## frictionless.describe.resource

<a name="frictionless.schema"></a>
## frictionless.schema

<a name="frictionless.schema.Schema"></a>
### Schema

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
#### <big>fields</big>

```python
 | @Metadata.property
 | fields()
```

Schema's fields

__Returns__

`Field[]`: an array of field instances

<a name="frictionless.schema.Schema.field_names"></a>
#### <big>field\_names</big>

```python
 | @Metadata.property(write=False)
 | field_names()
```

Schema's field names

__Returns__

`str[]`: an array of field names

<a name="frictionless.schema.Schema.add_field"></a>
#### <big>add\_field</big>

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
#### <big>get\_field</big>

```python
 | get_field(name)
```

Get schema's field by name.

__Arguments__

- __name__ (`str`): schema field name

__Returns__

`Field/None`: `Field` instance or `None` if not found

<a name="frictionless.schema.Schema.has_field"></a>
#### <big>has\_field</big>

```python
 | has_field(name)
```

Check if a field is present

__Arguments__

- __name__ (`str`): schema field name

__Returns__

`bool`: whether there is the field

<a name="frictionless.schema.Schema.remove_field"></a>
#### <big>remove\_field</big>

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
#### <big>expand</big>

```python
 | expand()
```

Expand the schema

It will add default values to the schema.

<a name="frictionless.schema.Schema.infer"></a>
#### <big>infer</big>

```python
 | infer(sample, *, type=None, names=None, confidence=config.DEFAULT_INFER_CONFIDENCE, missing_values=config.DEFAULT_MISSING_VALUES)
```

Infer schema

__Arguments__

    sample
    names
    confidence

<a name="frictionless.schema.Schema.read_data"></a>
#### <big>read\_data</big>

```python
 | read_data(cells)
```

Read a list of cells (normalize/cast)

__Arguments__

- __cells__ (`any[]`): list of cells

__Returns__

`any[]`: list of processed cells

<a name="frictionless.schema.Schema.write_data"></a>
#### <big>write\_data</big>

```python
 | write_data(cells, *, native_types=[])
```

Write a list of cells (normalize/uncast)

__Arguments__

- __cells__ (`any[]`): list of cells

__Returns__

`any[]`: list of processed cells

<a name="frictionless.schema.Schema.from_sample"></a>
#### <big>from\_sample</big>

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
## frictionless.check

<a name="frictionless.check.Check"></a>
### Check

```python
class Check(Metadata)
```

Check representation.

__Arguments__

- __descriptor? (str|dict)__: schema descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.resource"></a>
## frictionless.resource

<a name="frictionless.exceptions"></a>
## frictionless.exceptions

<a name="frictionless.exceptions.FrictionlessException"></a>
### FrictionlessException

```python
class FrictionlessException(Exception)
```

Main Frictionless exception

__Arguments__

    error

<a name="frictionless.transform"></a>
## frictionless.transform

<a name="frictionless.transform.main"></a>
## frictionless.transform.main

<a name="frictionless.transform.package"></a>
## frictionless.transform.package

<a name="frictionless.transform.resource"></a>
## frictionless.transform.resource

<a name="frictionless.parser"></a>
## frictionless.parser

<a name="frictionless.parser.Parser"></a>
### Parser

```python
class Parser()
```

Parser representation

__Arguments__

- __file__ (`File`): file

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.report"></a>
## frictionless.report

<a name="frictionless.report.Report"></a>
### Report

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
#### <big>flatten</big>

```python
 | flatten(spec)
```

Flatten the report

__Arguments__

    spec

<a name="frictionless.report.ReportTable"></a>
### ReportTable

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
#### <big>flatten</big>

```python
 | flatten(spec)
```

Flatten the report table

__Arguments__

    spec

<a name="frictionless.dialects"></a>
## frictionless.dialects

<a name="frictionless.dialects.Dialect"></a>
### Dialect

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
### CsvDialect

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
### ExcelDialect

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
### InlineDialect

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
### JsonDialect

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
## frictionless.\_\_main\_\_

<a name="frictionless.server"></a>
## frictionless.server

<a name="frictionless.server.Server"></a>
### Server

```python
class Server()
```

Server representation

<a name="frictionless.types"></a>
## frictionless.types

<a name="frictionless.types.yearmonth"></a>
## frictionless.types.yearmonth

<a name="frictionless.types.datetime"></a>
## frictionless.types.datetime

<a name="frictionless.types.date"></a>
## frictionless.types.date

<a name="frictionless.types.string"></a>
## frictionless.types.string

<a name="frictionless.types.object"></a>
## frictionless.types.object

<a name="frictionless.types.geojson"></a>
## frictionless.types.geojson

<a name="frictionless.types.year"></a>
## frictionless.types.year

<a name="frictionless.types.integer"></a>
## frictionless.types.integer

<a name="frictionless.types.time"></a>
## frictionless.types.time

<a name="frictionless.types.geopoint"></a>
## frictionless.types.geopoint

<a name="frictionless.types.array"></a>
## frictionless.types.array

<a name="frictionless.types.boolean"></a>
## frictionless.types.boolean

<a name="frictionless.types.any"></a>
## frictionless.types.any

<a name="frictionless.types.duration"></a>
## frictionless.types.duration

<a name="frictionless.types.number"></a>
## frictionless.types.number

<a name="frictionless.loader"></a>
## frictionless.loader

<a name="frictionless.loader.Loader"></a>
### Loader

```python
class Loader()
```

Loader representation

__Arguments__

- __file__ (`File`): file

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.loader.Loader.read_byte_stream"></a>
#### <big>read\_byte\_stream</big>

```python
 | read_byte_stream()
```

Create bytes stream

__Returns__

`BinaryIO`: I/O stream

<a name="frictionless.loader.Loader.read_text_stream"></a>
#### <big>read\_text\_stream</big>

```python
 | read_text_stream()
```

Create texts stream

__Returns__

`TextIO`: I/O stream

<a name="frictionless.validate"></a>
## frictionless.validate

<a name="frictionless.validate.main"></a>
## frictionless.validate.main

<a name="frictionless.validate.table"></a>
## frictionless.validate.table

<a name="frictionless.validate.table.validate_table"></a>
#### <big>validate\_table</big>

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
## frictionless.validate.package

<a name="frictionless.validate.package.validate_package"></a>
#### <big>validate\_package</big>

```python
@Report.from_validate
validate_package(source, basepath=None, noinfer=False, **options)
```

Validate package

<a name="frictionless.validate.inquiry"></a>
## frictionless.validate.inquiry

<a name="frictionless.validate.inquiry.validate_inquiry"></a>
#### <big>validate\_inquiry</big>

```python
@Report.from_validate
validate_inquiry(source)
```

Validate inquiry

<a name="frictionless.validate.schema"></a>
## frictionless.validate.schema

<a name="frictionless.validate.schema.validate_schema"></a>
#### <big>validate\_schema</big>

```python
@Report.from_validate
validate_schema(source)
```

Validate schema

<a name="frictionless.validate.resource"></a>
## frictionless.validate.resource

<a name="frictionless.validate.resource.validate_resource"></a>
#### <big>validate\_resource</big>

```python
@Report.from_validate
validate_resource(source, basepath=None, noinfer=False, lookup=None, **options)
```

Validate resource

<a name="frictionless.controls"></a>
## frictionless.controls

<a name="frictionless.controls.Control"></a>
### Control

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
### LocalControl

```python
class LocalControl(Control)
```

Local control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.controls.RemoteControl"></a>
### RemoteControl

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
### StreamControl

```python
class StreamControl(Control)
```

Stream control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.controls.TextControl"></a>
### TextControl

```python
class TextControl(Control)
```

Text control representation

__Arguments__

- __descriptor? (str|dict)__: descriptor

__Raises__

- `FrictionlessException`: raise any error that occurs during the process

<a name="frictionless.loaders"></a>
## frictionless.loaders

<a name="frictionless.loaders.remote"></a>
## frictionless.loaders.remote

<a name="frictionless.loaders.stream"></a>
## frictionless.loaders.stream

<a name="frictionless.loaders.text"></a>
## frictionless.loaders.text

<a name="frictionless.loaders.local"></a>
## frictionless.loaders.local

