# Extracting Data

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1is_PcpzFl42aWI2B2tHaBGj3jxsKZ_eZ)



Extracting data means reading tabular data from some source. We can use various customization for this process as though providing a file format, table schema, limiting fields or rows amount, and many more. Let's see on real files:


```
! pip install frictionless
```


```
! wget -q -O country-3.csv https://raw.githubusercontent.com/frictionlessdata/frictionless-py/master/data/country-3.csv
! cat country-3.csv
```

    id,capital_id,name,population
    1,1,Britain,67
    2,3,France,67
    3,2,Germany,83
    4,5,Italy,60
    5,4,Spain,47



```
! wget -q -O capital-3.csv https://raw.githubusercontent.com/frictionlessdata/frictionless-py/master/data/capital-3.csv
! cat capital-3.csv
```

    id,name
    1,London
    2,Berlin
    3,Paris
    4,Madrid
    5,Rome


For a starter, we will use the command-line interface:


```
! frictionless extract country-3.csv
```

    [data] country-3.csv

      id    capital_id  name       population
    ----  ------------  -------  ------------
       1             1  Britain            67
       2             3  France             67
       3             2  Germany            83
       4             5  Italy              60
       5             4  Spain              47


The same can be done in Python:


```
from pprint import pprint
from frictionless import extract

rows = extract('country-3.csv')
pprint(rows)
```

    [Row([('id', 1), ('capital_id', 1), ('name', 'Britain'), ('population', 67)]),
     Row([('id', 2), ('capital_id', 3), ('name', 'France'), ('population', 67)]),
     Row([('id', 3), ('capital_id', 2), ('name', 'Germany'), ('population', 83)]),
     Row([('id', 4), ('capital_id', 5), ('name', 'Italy'), ('population', 60)]),
     Row([('id', 5), ('capital_id', 4), ('name', 'Spain'), ('population', 47)])]



## Extract functions

The high-level interface for extracting data provided by Frictionless is a set of `extract` functions:
- `extract`: it will detect the source type and extract data accordingly
- `extract_package`: it accepts a package descriptor and returns a map of the package's tables
- `extract_resource`: it accepts a resource descriptor and returns a table data
- `extract_table`: it accepts various tabular options and returns a table data

In command-line, there is only 1 command but there is a flag to adjust the behavior:

```bash
$ frictionless extract
$ frictionless extract --source-type package
$ frictionless extract --source-type resource
$ frictionless extract --source-type table
```


The `extract` functions always read data in a form of rows (see the object description below) into memory. The lower-level interfaces will allow you to stream data and various output forms.


### Extracting Package

The easiest way is to use a command line-interface. We're going to provide two files to the `extract` command which will be enough to detect that it's a dataset:


```
! frictionless extract *-3.csv
```

    [data] capital-3.csv

      id  name
    ----  ------
       1  London
       2  Berlin
       3  Paris
       4  Madrid
       5  Rome

    [data] country-3.csv

      id    capital_id  name       population
    ----  ------------  -------  ------------
       1             1  Britain            67
       2             3  France             67
       3             2  Germany            83
       4             5  Italy              60
       5             4  Spain              47


In Python we have can do the same providing a glob for the `extract` function but instead we will use `extract_package` providing a package descriptor:


```
from frictionless import extract_package

data = extract_package({'resources':[{'path': 'country-3.csv'}, {'path': 'capital-3.csv'}]})
for path, rows in data.items():
  pprint(path)
  pprint(rows)
```

    'country-3.csv'
    [Row([('id', 1), ('capital_id', 1), ('name', 'Britain'), ('population', 67)]),
     Row([('id', 2), ('capital_id', 3), ('name', 'France'), ('population', 67)]),
     Row([('id', 3), ('capital_id', 2), ('name', 'Germany'), ('population', 83)]),
     Row([('id', 4), ('capital_id', 5), ('name', 'Italy'), ('population', 60)]),
     Row([('id', 5), ('capital_id', 4), ('name', 'Spain'), ('population', 47)])]
    'capital-3.csv'
    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


### Extracting Resource

A resource contains only one file and for extracting a resource we can use the same approach we used above but providing only one file. We will extract data using a metadata descriptor:


```
from frictionless import extract_resource

rows = extract_resource({'path': 'capital-3.csv'})
pprint(rows)
```

    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


Usually, the code above doesn't really make sense as we can just provide a path to the high-level `extract` function instead of a descriptor to the `extract_resource` function but the power of the descriptor is that it can contain different metadata and be stored on the disc. Let's extend our example:


```
from frictionless import Resource

resource = Resource(path='capital-3.csv')
resource.schema.missing_values.append('3')
resource.to_yaml('capital.resource.yaml')
```


```
! cat capital.resource.yaml
```

    path: capital-3.csv
    schema:
      missingValues:
        - ''
        - '3'



```
! frictionless extract capital.resource.yaml
```

    [data] capital.resource.yaml

      id  name
    ----  ------
       1  London
       2  Berlin
          Paris
       4  Madrid
       5  Rome


So what's happened? We set textual representation of the number "3" to be a missing value. It was done only for the presentational purpose because it's difenitely not a missing value. On the ohter hand, it demostrated how metadata can be used.

### Extracting Table

While the package and resource concepts contain both data and metadata, a table is solely data. Because of this fact we can provide much more options to the `extract_table` function. Most of this options are incapsulated into the resource descriptor as we saw with the `missingValues` example above. We will reproduce it:


```
from frictionless import extract_table

rows = extract_table('capital-3.csv', patch_schema={'missingValues': ['', '3']})
pprint(rows)
```

    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', None), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


We got in idential result but it's important to understand that on the table level we need to provide all the metadata options separately while a resource incapsulate all these metadata. Please check the `extract_table` API Referenec as it has a lot of options. We're going to discuss some of the below.

## Using Package

The Package class is a metadata class which provides an ability to read its contents. First of all, let's create a package descriptor:


```
! frictionless describe *-3.csv --json > country.package.json
```

Now, we can open the created descriptor and read the package's resources:


```
from frictionless import Package

package = Package('country.package.json')
pprint(package.get_resource('country-3').read_rows())
pprint(package.get_resource('capital-3').read_rows())
```

    [Row([('id', 1), ('capital_id', 1), ('name', 'Britain'), ('population', 67)]),
     Row([('id', 2), ('capital_id', 3), ('name', 'France'), ('population', 67)]),
     Row([('id', 3), ('capital_id', 2), ('name', 'Germany'), ('population', 83)]),
     Row([('id', 4), ('capital_id', 5), ('name', 'Italy'), ('population', 60)]),
     Row([('id', 5), ('capital_id', 4), ('name', 'Spain'), ('population', 47)])]
    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


The package by itself doesn't provide any read functions directly as it's a role of its resources. So all the written below for the Resource class can be used withing a package.


## Using Resource

The Resource class is also a metadata class which provides various read and stream functions. Let's create a resource descriptor:


```
! frictionless describe country-3.csv --json > country.resource.json
```

**Exploring Data**

There are various function helping to explore your resource as checking headers, and other attributes as stats:


```
from frictionless import Resource

resource = Resource('country.resource.json')
pprint(resource.read_headers())
pprint(resource.read_sample())
pprint(resource.read_stats())
```

    ['id', 'capital_id', 'name', 'population']
    [['1', '1', 'Britain', '67'],
     ['2', '3', 'France', '67'],
     ['3', '2', 'Germany', '83'],
     ['4', '5', 'Italy', '60'],
     ['5', '4', 'Spain', '47']]
    {'bytes': 100, 'hash': 'c0558b91523683483f86f63346d06d81', 'rows': 5}


**Reading Data**

The `extract` functions always read rows into memory; Resource can do the same but it also gives a choice regarding ouput data. It can be `rows`, `data`, `text`, or `bytes`. Let's try reading all of them:


```
from frictionless import Resource

resource = Resource('country.resource.json')
pprint(resource.read_bytes())
pprint(resource.read_text())
pprint(resource.read_data())
pprint(resource.read_rows())
```

    (b'id,capital_id,name,population\n1,1,Britain,67\n2,3,France,67\n3,2,Germany,8'
     b'3\n4,5,Italy,60\n5,4,Spain,47\n')
    ('id,capital_id,name,population\n'
     '1,1,Britain,67\n'
     '2,3,France,67\n'
     '3,2,Germany,83\n'
     '4,5,Italy,60\n'
     '5,4,Spain,47\n')
    [['1', '1', 'Britain', '67'],
     ['2', '3', 'France', '67'],
     ['3', '2', 'Germany', '83'],
     ['4', '5', 'Italy', '60'],
     ['5', '4', 'Spain', '47']]
    [Row([('id', 1), ('capital_id', 1), ('name', 'Britain'), ('population', 67)]),
     Row([('id', 2), ('capital_id', 3), ('name', 'France'), ('population', 67)]),
     Row([('id', 3), ('capital_id', 2), ('name', 'Germany'), ('population', 83)]),
     Row([('id', 4), ('capital_id', 5), ('name', 'Italy'), ('population', 60)]),
     Row([('id', 5), ('capital_id', 4), ('name', 'Spain'), ('population', 47)])]


**Streaming Data**

It's really handy to read all your data into memory but it's not always possible as a file can be really big. For such cases, Frictionless provides streaming functions:


```
from frictionless import Resource

resource = Resource('country.resource.json')
pprint(resource.read_byte_stream())
pprint(resource.read_text_stream())
pprint(resource.read_data_stream())
pprint(resource.read_row_stream())
for row in resource.read_row_stream():
  print(row)
```

    <frictionless.loader.ByteStreamWithStatsHandling object at 0x7fbee43266a0>
    <_io.TextIOWrapper name='country-3.csv' encoding='utf-8'>
    <generator object Resource.read_data_stream at 0x7fbee4299150>
    <generator object Resource.read_row_stream at 0x7fbee4299150>
    Row([('id', 1), ('capital_id', 1), ('name', 'Britain'), ('population', 67)])
    Row([('id', 2), ('capital_id', 3), ('name', 'France'), ('population', 67)])
    Row([('id', 3), ('capital_id', 2), ('name', 'Germany'), ('population', 83)])
    Row([('id', 4), ('capital_id', 5), ('name', 'Italy'), ('population', 60)])
    Row([('id', 5), ('capital_id', 4), ('name', 'Spain'), ('population', 47)])


## Using Table

The Table class is at heart of all the tabular capabilities of Frictionless. It's used by all the higher-level class and provides a comprehensive user interface by itself. The main difference with, for example, Resource class that Table has a state of a lower-level file descriptor and needs to be opened and closed. Usually we use a context manager (the `with` keyword) to work with Table. In-general, Table is a streaming interface that needs to be re-opened if data is already read.


**Exploring Data**

First of all, let's take a look at the file details information:



```
from frictionless import Table

with Table('capital-3.csv') as table:
  print(f'Source: "{table.source}"')
  print(f'Scheme: "{table.scheme}"')
  print(f'Format: "{table.format}"')
  print(f'Hashing: "{table.hashing}"')
  print(f'Encoding: "{table.encoding}"')
  print(f'Compression: "{table.compression}"')
  print(f'Compression Path: "{table.compression_path}"')
```

    Source: "capital-3.csv"
    Scheme: "file"
    Format: "csv"
    Hashing: "md5"
    Encoding: "utf-8"
    Compression: "no"
    Compression Path: ""


There are much more information available; we will explain some of it later in the the sections below:


```
from frictionless import Table

with Table('capital-3.csv') as table:
  print(f'Control: "{table.control}"')
  print(f'Dialect: "{table.dialect}"')
  print(f'Query: "{table.query}"')
  print(f'Schema: "{table.schema}"')
  print(f'Headers: "{table.headers}"')
  print(f'Sample: "{table.sample}"')
  print(f'Stats: "{table.stats}"')
```

    Control: "{}"
    Dialect: "{}"
    Query: "{}"
    Schema: "{'fields': [{'name': 'id', 'type': 'integer'}, {'name': 'name', 'type': 'string'}]}"
    Headers: "['id', 'name']"
    Sample: "[['1', 'London'], ['2', 'Berlin'], ['3', 'Paris'], ['4', 'Madrid'], ['5', 'Rome']]"
    Stats: "{'hash': 'e7b6592a0a4356ba834e4bf1c8e8c7f8', 'bytes': 50, 'rows': 0}"


Many of the properties above not only can be read from the existent Table but also can be provided as an option to alter the Table behaviour, for example:


```
from frictionless import Table

with Table('capital-3.csv', scheme='file', format='csv') as table:
  print(table.source)
  print(table.scheme)
  print(table.format)
```

    capital-3.csv
    file
    csv


**Reading Data**

There are 2 different types of ouput that Table can produce:


```
from frictionless import Table

with Table('capital-3.csv') as table:
  pprint(table.read_data())
with Table('capital-3.csv') as table:
  pprint(table.read_rows())
```

    [['1', 'London'],
     ['2', 'Berlin'],
     ['3', 'Paris'],
     ['4', 'Madrid'],
     ['5', 'Rome']]
    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


The `data` format is just a raw array of arrays similiar to JSON while the `row` format is rich object with all the cells normalized and converted to proper types. We will explore the Row class later.

**Streaming Data**

It was mentioned for Resource and it's the same for Table, we can stream our tabular data. The core difference is that Table is stateful so we use properties instead of the read functions:


```
from frictionless import Table

with Table('capital-3.csv') as table:
  pprint(table.data_stream)
  for cells in table.data_stream:
    print(cells)
with Table('capital-3.csv') as table:
  pprint(table.row_stream)
  for row in table.row_stream:
    print(row)
```

    <generator object Table.__read_data_stream_create at 0x7fbee429ffc0>
    ['1', 'London']
    ['2', 'Berlin']
    ['3', 'Paris']
    ['4', 'Madrid']
    ['5', 'Rome']
    <generator object Table.__read_row_stream_create at 0x7fbee4123c50>
    Row([('id', 1), ('name', 'London')])
    Row([('id', 2), ('name', 'Berlin')])
    Row([('id', 3), ('name', 'Paris')])
    Row([('id', 4), ('name', 'Madrid')])
    Row([('id', 5), ('name', 'Rome')])


**Table's Lifecycle**

You might probably have noticed that we had to duplicate the `with Table(...)` statement in some example. The reason is that Table is a streaming interface. Once it's read you need to open it again. Let's show it on example:


```
from frictionless import Table

table = Table('capital-3.csv')
table.open()
pprint(table.read_rows())
pprint(table.read_rows())
# We need to re-open: there is no data left
table.open()
pprint(table.read_rows())
# We need to close manually: not context manager is used
table.close()
```

    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]
    []
    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


## File Details

Let's overview the details we can specify for a file. Usually you don't need to provide those details as Frictionless is capable to infer it on its own. Although, there are situation when you need to specify it manually. The following example will use the `Table` class but the same options can be used for the `extract` and `extract_table` functions.

**Scheme**

The scheme also know as protocol indicates which loader Frictionless should use to read or write data. It can be `file` (default), `text`, `http`, `https`, `s3`, and others.


```
from frictionless import Table

with Table('header1,header2\nvalue1,value2.csv', scheme='text') as table:
  print(table.scheme)
  print(table.read_rows())
```

    text
    [Row([('header1', 'value1'), ('header2', 'value2.csv')])]


**Format**

The format or as it's also called extension helps Frictionless to choose a proper parser to handle the file. Popular formats are `csv`, `xlsx`, `json` and others


```
from frictionless import Table

with Table('text://header1,header2\nvalue1,value2.csv', format='csv') as table:
  print(table.format)
  print(table.read_rows())
```

    csv
    [Row([('header1', 'value1'), ('header2', 'value2.csv')])]


**Hashing**

The hashing option controls which hashing algorithm should be used for generating the `hash` property. It doesn't affect the `extract` function but can be used with the `Table` class:


```
from frictionless import Table

with Table('country-3.csv', hashing='sha256') as table:
  table.read_rows()
  print(table.hashing)
  print(table.stats['hash'])
```

    sha256
    408b5058f961915c1e1f3bc318ab01d7d094a4daccdf03ad6022cfc7b8ea4e3e


**Encoding**

Frictionless automatically detects encoding of files but sometimes it can be innacurate. It's possible to provide an encoding manually:


```
from frictionless import Table

with Table('country-3.csv', encoding='utf-8') as table:
  print(table.encoding)
  print(table.source)
```

    country-3.csv
    utf-8


**Compression**

It's possible to adjust compression detection by providing the algorithm explicetely. For the example below it's not required as it would be detected anyway:


```
! wget -q -O table.csv.zip https://raw.githubusercontent.com/frictionlessdata/frictionless-py/master/data/table.csv.zip
```


```
from frictionless import Table

with Table('table.csv.zip', compression='zip') as table:
  print(table.compression)
  print(table.read_rows())
```

    zip
    [Row([('id', 1), ('name', 'english')]), Row([('id', 2), ('name', '中国人')])]


**Compression Path**

By default, Frictionless uses the first file found in a zip archive. It's possible to adjust this behaviour:


```
! wget -q -O table-multiple-files.zip https://raw.githubusercontent.com/frictionlessdata/frictionless-py/master/data/table-multiple-files.zip
```


```
from frictionless import Table

with Table('table-multiple-files.zip', compression_path='table-reverse.csv') as table:
  print(table.compression)
  print(table.compression_path)
  print(table.read_rows())
```

    zip
    table-reverse.csv
    [Row([('id', 1), ('name', '中国人')]), Row([('id', 2), ('name', 'english')])]


Further reading:
- Schemes Reference
- Formats Reference

## File Control

The Control object allows you to manage the loader used by the Table class. In most cases, you don't need to provide any Control settings but sometimes it can be useful:


```
from frictionless import Table, controls

source = 'https://raw.githubusercontent.com/frictionlessdata/frictionless-py/master/data/table.csv'
control = controls.RemoteControl(http_timeout=1)
with Table(source, control=control) as table:
  print(table.control)
  print(table.read_rows())
```

    {'httpTimeout': 1}
    [Row([('id', 1), ('name', 'english')]), Row([('id', 2), ('name', '中国人')])]


Exact parameters depend on schemes and can be found in the Schemes Reference. For example, the Remote Control provides `http_timeout`, `http_session`, and others but there is only one option available for all controls:

**Detect Encoding**

It's a function that can be provided to adjust the encoding detection:


```
from frictionless import Table, controls

control = controls.Control(detect_encoding=lambda sample: "utf-8")
with Table("capital-3.csv", control=control) as table:
  print(table.source)
  print(table.encoding)
```

    capital-3.csv
    utf-8


Further reading:
- Schemes Reference

## Table Dialect

The Dialect is similiar to Control but it affects the way the parser works. Let's use the CSV Dialect to adjust the delimiter configuration:


```
from frictionless import Table, dialects

source = 'header1;header2\nvalue1;value2'
dialect = dialects.CsvDialect(delimiter=';')
with Table(source, scheme='text', format='csv', dialect=dialect) as table:
  print(table.dialect)
  print(table.read_rows())
```

    {'delimiter': ';'}
    [Row([('header1', 'value1'), ('header2', 'value2')])]


There are a great deal of options available for different dialect that can be found in "Formats Reference". We will list the properties that can be used with every dialect:

**Header**

**Header Rows**

**Header Join**

Further reading:
- Formats Reference

## Table Query

## Table Schema

## Headers Object


## Row Object