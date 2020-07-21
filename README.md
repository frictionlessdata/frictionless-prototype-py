# frictionless-py

[![Travis](https://img.shields.io/travis/frictionlessdata/frictionless-py/master.svg)](https://travis-ci.org/frictionlessdata/frictionless-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/frictionless-py.svg?branch=master)](https://coveralls.io/r/frictionlessdata/frictionless-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/frictionless.svg)](https://pypi.python.org/pypi/frictionless)
[![Github](https://img.shields.io/badge/github-master-brightgreen)](https://github.com/frictionlessdata/frictionless-py)
[![Discord](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://discord.com/channels/695635777199145130/695635777199145133)

Frictionless is a framework to describe, extract, validate, and transform tabular data. It supports a great deal of data sources and formats, as well as provides popular platforms integrations. The framework is powered by the lightweight yet comprehensive [Frictionless Data Specifications](https://specs.frictionlessdata.io/).

## Features

- **Describe your data**: You can infer, edit and save metadata of your data tables. It's a first step for ensuring data quality and usability. Frictionless metadata includes general information about your data like textual description, as well as, field types and other tabular data details.
- **Extract your data**: You can read your data using a unified tabular interface. Data quality and consistency are guaranteed by a schema. Frictionless supports various file protocols like HTTP, FTP, and S3 and data formats like CSV, XLS, JSON, SQL, and others.
- **Validate your data**: You can validate data tables, resources, and datasets. Frictionless generates a unified validation report, as well as supports a lot of options to customize the validation process.
- **Transform your data**: You can clean, reshape, and transfer your data tables and datasets. Frictionless provides a pipeline capability and a lower-level interface to work with the data.

---

- powerful command-line interface
- low memory consumption for data of any size
- reasonable performance on big data
- support for compressed files
- custom checks and formats
- fully pluggable architecture
- the included API server
- more than 1000+ tests

## Installation

> Versioning follows the [SemVer Standard](https://semver.org/)

```bash
$ pip install frictionless
$ pip install frictionless[sql]  # to install a core plugin
```

By default, the framework comes with the support of CSV, Excel, and JSON formats. Please use the command above to add support for SQL, Pandas, Html, and others. Usually, you don't need to think about it in advance - frictionless will show a useful error on a missing plugin with installation instruction.

## Usage

The framework can be used:
- as a Python library
- as a command-line interface
- as a restful API server

For example, all the examples below do the same thing:

```python
from frictionless import extract

extract('data/table.csv')
# CLI: $ frictionless extract data/table.csv
# API: [POST] /extract {"source': 'data/table.csv"}
```

All these interfaces are close as much as possible regarding naming and the way you interact with them. Usually, it's straightforward to translate e.g., Python code to a command-line call. Frictionless provides code completion for Python and command-line, which should help to get useful hints in real-time.

Arguments follow this naming rule:
- for Python interfaces, they are lowercased, e.g. `missing_values`
- within dictionaries or JSON objects they are camel-cased, e.g. `missingValues`
- in a command line they use dashes, e.g. `--missing-values`

To get documentation for a command-line interface just use the `--help` flag:

```bash
$ frictionless --help
$ frictionless describe --help
$ frictionless extract --help
$ frictionless validate --help
$ frictionless transform --help
```

## Example

> All the examples use the data folder from this repository

We will take a very dirty data file:


```bash
$ cat data/invalid.csv
id,name,,name
1,english
1,english

2,german,1,2,3
```

Firt of all, let's infer the metadata. We can save and edit it to provide useful information about the table:

```bash
$ frictionless describe data/invalid.csv
[metadata] data/invalid.csv

bytes: 50
compression: 'no'
compressionPath: ''
dialect: {}
encoding: utf-8
format: csv
hash: 8c73c3d9d59088dcb2508e0b348bf8a8
hashing: md5
name: invalid
path: data/invalid.csv
profile: tabular-data-resource
rows: 4
schema:
  fields:
    - name: id
      type: integer
    - name: name
      type: string
    - name: field3
      type: integer
    - name: name2
      type: integer
scheme: file
```

Secondly, we can extract the cleaned data. It conforms to the inferred schema from above e.g., the dimension is fixed, and bad cells are omitted:

```bash
$ frictionless extract data/invalid.csv
[data] data/invalid.csv

  id  name       field3    name2
----  -------  --------  -------
   1  english
   1  english

   2  german          1        2
```

Last but not least, let's get a validation report. This report will help us to fix all these errors as comprehensive information is provided for every tabular problem:

```bash
$ frictionless validate data/invalid.csv
[invalid] data/invalid.csv

row    field    code              message
-----  -------  ----------------  ------------------------------------------------------------------------------------------------
-      3        blank-header      Header in field at position "3" is blank
-      4        duplicate-header  Header "name" in field at position "4" is duplicated to header in another field: at position "2"
2      3        missing-cell      Row at position "2" has a missing cell in field "field3" at position "3"
2      4        missing-cell      Row at position "2" has a missing cell in field "name2" at position "4"
3      3        missing-cell      Row at position "3" has a missing cell in field "field3" at position "3"
3      4        missing-cell      Row at position "3" has a missing cell in field "name2" at position "4"
4      -        blank-row         Row at position "4" is completely blank
5      5        extra-cell        Row at position "5" has an extra value in field at position "5"
```

Now having all this information:
- we can clean up the table to ensure the data quality
- we can use the metadata to describe and share the dataset
- we can include the validation into our workflow to guarantee validity
- and much more: don't hesitate and read the documentation below!

## Documentation

This readme gives a high-level overview of the framework. A detailed documentation is avialable and here is a table of contents:
- [Introductory Guide](docs/introductory-guide.md)
- [Advanced Guide](docs/advanced-guide.md)
- [Extension Guide](docs/extension-guide.md)
- [Contribution Guide](docs/contribution-guide.md)
- [Using with SQL](docs/using-with-sql.md)
- [Using with Pandas](docs/using-with-pandas.md)
- [Formats Reference](docs/formats-reference.md)
- [Errors Reference](docs/errors-reference.md)
- [API Reference](docs/api-reference.md)
- [Contributors](docs/contributors.md)
- [Changelog](docs/changelog.md)
