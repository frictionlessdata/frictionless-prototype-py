# Describing Data

What does "describing data" mean?

Frictionless is a project based on the [Frictionless Data Specifications](https://specs.frictionlessdata.io/). It's a set of patterns for creating metadata including Data Package (for datasets), Data Resource (for files) and Table Schema (for tables).

So in other words, "describing data" means creating metadata for your data files. The reason for having metadata is simple: usually data files themselves are not capable to provide enough information. For example, if you have a data table in a csv format it misses a few critical pieces of imformation:
- meaning of the fields e.g. what the `size` field mean; is it clothes size or file size
- data types information e.g. is this field a string or an integer
- data constraints e.g. the minimal temperature for your measurments
- data relations e.g. identifiers connection
- and others

For a dataset there are even more information can be provided like general dataset purpose, information about data sources, list of authors, and many more. Of course, when there are many tabular files relational rules can be very important. Usually, there are foreign keys ensuring integrity of the dataset, for example, there is some reference table containing country names and other tables using it as a reference. Data in this form is called "normalized data" and it occurs very often in scientific and other kind of research.

Having general understanding of what is "data describing", we can now articulate why it's important:
- data validation; metadata helps to reveal problems in your data on the early stages of your workflow
- data publication; metadata provides additional information that your data can't include

There are not the only two pros of having metadata but they are two the most important. Please continue reading to learn how Frictionless helps to achieve these advantages describing you data.

## Table Schema

Table Schema is a specification for providing a "schema" (similar to a database schema) for tabular data. This information includes the expected type of each value in a column ("string", "number", "date", etc.), constraints on the value ("this string can only be at most 10 characters long"), and the expected format of the data ("this field should only contain strings that look like email addresses"). Table Schema can also specify relations between tables.

We're going to use this file for this section examples. For this guide we use solely csv files because of their demonstrativeness but in-general Frictionless can handle Excel, JSON, SQL, and many others formats:

> data/country-1.csv

```csv
id,neighbor_id,name,population
1,,Britain,67
2,3,France,67
3,2,Germany,83
4,5,Italy,60
5,4,Spain,47
```

Let's get Table Schema using Frictionless framework:

```python
from frictionless import describe

resource = describe("data/country.csv")
schema = resource.schema
schema.to_yaml("country.schema.yaml")
```

The high-level functions of Frictionless operate on dataset and resource levels so we have to use Python a little of Python programming to get schema information. Below we will show how to use command-line interface for similiar tasks.

```yaml
$ cat country.schema.yaml
fields:
  - name: id
    type: integer
  - name: neighbor_id
    type: integer
  - name: name
    type: string
  - name: population
    type: integer
```

As we can see, we were able to get infer basic metadata of our data file but describing data doesn't end here, we can  provide additional information we discussed earlier:

```python
from frictionless import describe

resource = describe("data/country.csv")
schema = resource.schema
schema.get_field("id").title = "Identifier"
schema.get_field("neighbor_id").title = "Identifier of the neighbor"
schema.get_field("name").title = "Name of the country"
schema.get_field("population").title = "Population"
schema.get_field("population").description = "According to the year 2020's data"
schema.get_field("population").constraints["minimum"] = 0
schema.foreign_keys.append(
    {"fields": ["neighbor_id"], "reference": {"resource": "", "fields": ["id"]}}
)
schema.to_yaml("country.schema.yaml")
```

Let's break it down:
- we added title for all the fields
- we added a description to the "Population" field; the year information can be critical to interpret the data
- we set a constraint to the "Population" field because it can't be less than 0
- we added a foreign key saying that "Identifier of the neigbor" should present in the "Identifier" field

```yaml
$ cat country.schema.yaml
fields:
  - name: id
    title: Identifier
    type: integer
  - name: neighbor_id
    title: Identifier of the neighbor
    type: integer
  - name: name
    title: Name of the country
    type: string
  - constraints:
      minimum: 0
    description: According to the year 2020's data
    name: population
    title: Population
    type: integer
foreignKeys:
  - fields:
      - neighbor_id
    reference:
      fields:
        - id
      resource: ''
```

Later we're going to show how to use the schema we created to ensure validity of your data; in the next few sections we will focus on Data Resource and Data Package metadata.

To continue learning about table schemas please read:
- [Table Schema Spec](https://specs.frictionlessdata.io/table-schema/)
- API Reference: Schema


## Data Resource

The Data Resource format describes a data resource such as an individual file or table.
The essence of a Data Resource is a locator for the data it describes.
A range of other properties can be declared to provide a richer set of metadata.

For this section, we will use the file that is slightly more complex to handle. For some reason cells are separated by the ";" char and there is a comment on the top:

> data/country-2.csv

```csv
# Author: the scientist
id;neighbor_id;name;population
1;;Britain;67
2;3;France;67
3;2;Germany;83
4;5;Italy;60
5;4;Spain;47
```

Let's describe it this time using command-line interface:

```yaml
frictionless describe data/country-2.csv
[metadata] data/country-2.csv

bytes: 124
compression: 'no'
compressionPath: ''
dialect: {}
encoding: utf-8
format: csv
hash: 88e1901235a8cf35da4d28a1cdf415e5
hashing: md5
name: country-2
path: data/country-2.csv
profile: tabular-data-resource
rows: 6
schema:
  fields:
    - name: '# Author: the scientist'
      type: string
scheme: file
```

OK, that's clearly wrong. As we have seen in the "Introductory Guide" Frictionless is capable of inferring some complicated cases' metadata but our table is too weird for it. We need to program it:

```python
from frictionless import describe, Schema

resource = describe("data/country-2.csv")
resource.dialect.header_rows = [2]
resource.dialect.delimiter = ";"
resource.schema = Schema("country.schema.yaml")
resource.to_yaml("country.resource.yaml")
```

So what we are doing here:
- we set header rows to be row number 2; as humans we can easaly see it
- we set CSV Delimiter to be ";"; this file in not really usual csv for some reason
- we reuse the schema we created earlier as the data has the same structure and meaning

```yaml
bytes: 124
compression: 'no'
compressionPath: ''
dialect:
  delimiter: ;
  headerRows:
    - 2
encoding: utf-8
format: csv
hash: 88e1901235a8cf35da4d28a1cdf415e5
hashing: md5
name: country-2
path: data/country-2.csv
profile: tabular-data-resource
rows: 6
schema:
  fields:
    - name: id
      title: Identifier
      type: integer
    - name: neighbor_id
      title: Identifier of the neighbor
      type: integer
    - name: name
      title: Name of the country
      type: string
    - constraints:
        minimum: 0
      description: According to the year 2020's data
      name: population
      title: Population
      type: integer
  foreignKeys:
    - fields:
        - neighbor_id
      reference:
        fields:
          - id
        resource: ''
scheme: file
```

Our resource metadata includes the schema metadata we created earlier but also it has:
- general inmormation about file's schema, format and compression
- information about CSV Dialect helping software understand how to read it
- checksum information as though hash, bytes and rows

But the most important difference is that resource metadata contains the `path` property. It conceptually distincts Data Resource specification from Table Schema specification because while a Table Schema descriptor can describe a class of data files, a Data Resoure descriptor describes the only one exact data file, `data/country-2.csv` in our case.

Using programming terminolody we could say that:
- Table Schema descriptor is abstract (for a class of files)
- Data Resource descriptor is concrete (for an individual file)

We will show the practical difference in the "Using Metadata" section but in the next section we will overview the Data Package specification.

To continue learning about data resources please read:
- [Data Resource Spec](https://specs.frictionlessdata.io/data-resource/)
- API Reference: Resource

## Data Package

A Data Package consists of:
- Metadata that describes the structure and contents of the package
- Resources such as data files that form the contents of the package
The Data Package metadata is stored in a “descriptor”. This descriptor is what makes a collection of data a Data Package. The structure of this descriptor is the main content of the specification below.

In addition to this descriptor a data package will include other resources such as data files. The Data Package specification does NOT impose any requirements on their form or structure and can therefore be used for packaging any kind of data.

The data included in the package may be provided as:
- Files bundled locally with the package descriptor
- Remote resources, referenced by URL
- "Inline" data (see below) which is included directly in the descriptor

For this section, we will use following files:

> data/country-3.csv

```csv
id,capital_id,name,population
1,1,Britain,67
2,3,France,67
3,2,Germany,83
4,5,Italy,60
5,4,Spain,47
```

> data/capital-3.csv

```csv
id,name
1,London
2,Berlin
3,Paris
4,Madrid
5,Rome
```

First of all, let's describe our package using command-line interface. We did it before for a resource but now we're going to use a glob pattern to indicate that there are multiple files:

```yaml
$ frictionless describe data/*-3.csv
[metadata] data/capital-3.csv data/country-3.csv

profile: data-package
resources:
  - bytes: 50
    compression: 'no'
    compressionPath: ''
    dialect: {}
    encoding: utf-8
    format: csv
    hash: e7b6592a0a4356ba834e4bf1c8e8c7f8
    hashing: md5
    name: capital-3
    path: data/capital-3.csv
    profile: tabular-data-resource
    rows: 5
    schema:
      fields:
        - name: id
          type: integer
        - name: name
          type: string
    scheme: file
  - bytes: 100
    compression: 'no'
    compressionPath: ''
    dialect: {}
    encoding: utf-8
    format: csv
    hash: c0558b91523683483f86f63346d06d81
    hashing: md5
    name: country-3
    path: data/country-3.csv
    profile: tabular-data-resource
    rows: 5
    schema:
      fields:
        - name: id
          type: integer
        - name: capital_id
          type: integer
        - name: name
          type: string
        - name: population
          type: integer
    scheme: file
```

We have already learned about many concepts that are reflected in this metadata. We can see resources, schemas, fields, and others familiar entities. The difference is that this descriptor has information about multiple files which is the most popular way of sharing data - in datasets. Very often you have not only one data file but also additional data files, some textual documents e.g. PDF, and others. To package all of these files with corresponding metadat we use data packges.

Following the already familiar to the guide's reader pattern, we add some additional metadata:

```python
from frictionless import describe

package = describe("data/*-3.csv")
package.title = "Countries and their capitals"
package.description = "The data was collected as a research project"
package.get_resource("country-3").name = "country"
package.get_resource("capital-3").name = "capital"
package.get_resource("country").schema.foreign_keys.append(
    {"fields": ["capital_id"], "reference": {"resource": "capital", "fields": ["id"]}}
)
package.to_yaml("country.package.yaml")
```

In this case, we add a relation between to different files connecting `id` and `capital_id`. Also, we provide dataset-level metadata to share to purpose of this dataset. We haven't added individual fields' titles and description but it can be done as it was shown in the "Table Schema" section.

```yaml
$ cat country.package.json
title: Countries and their capitals
description: The data was collected as a research project
profile: data-package
resources:
  - bytes: 100
    compression: 'no'
    compressionPath: ''
    dialect: {}
    encoding: utf-8
    format: csv
    hash: c0558b91523683483f86f63346d06d81
    hashing: md5
    name: country
    path: data/country-3.csv
    profile: tabular-data-resource
    rows: 5
    schema:
      fields:
        - name: id
          type: integer
        - name: capital_id
          type: integer
        - name: name
          type: string
        - name: population
          type: integer
      foreignKeys:
        - fields:
            - capital_id
          reference:
            fields:
              - id
            resource: capital
    scheme: file
  - bytes: 50
    compression: 'no'
    compressionPath: ''
    dialect: {}
    encoding: utf-8
    format: csv
    hash: e7b6592a0a4356ba834e4bf1c8e8c7f8
    hashing: md5
    name: capital
    path: data/capital-3.csv
    profile: tabular-data-resource
    rows: 5
    schema:
      fields:
        - name: id
          type: integer
        - name: name
          type: string
    scheme: file
```

The main role of Data Package descriptor is describing a dataset; as we can see, it includes previously shown descriptors as though `schema`, `dialect` and `resource`. But it's a mistake to think then that Data Package is the least important specification; actually, it completes the Frictionless Data suite making possible sharing and validating not only individual fiels but complete datasets.

To continue learning about data resources please read:
- [Data Package Spec](https://specs.frictionlessdata.io/data-package/)
- API Reference: Package

## Using Metadata

## Inferring Metadata

## Expanding Metadata

## Transforming Metadata

## The describe functions

Frictionless framework provides 3 different `describe` functions in Python:
- `describe`: it will detect the source type and return Data Resource or Data Package metadata
- `describe_resoure`: it will always return Data Resource metadata
- `describe_package`: it will always return Data Package metadata

In command-line there is only 1 command but there is a flag to adjust the behaviour:

```bash
$ frictionless describe
$ frictionless describe --source-type resource
$ frictionless describe --source-type package
```

For example, if we want a Data Package descriptor for a single file:

```yaml
$ frictionless describe data/country-1.csv --source-type package
[metadata] data/country-1.csv

profile: data-package
resources:
  - bytes: 100
    compression: 'no'
    compressionPath: ''
    dialect: {}
    encoding: utf-8
    format: csv
    hash: 4204f087f328b70c854c03403ab448c4
    hashing: md5
    name: country-1
    path: data/country-1.csv
    profile: tabular-data-resource
    rows: 5
    schema:
      fields:
        - name: id
          type: integer
        - name: neighbor_id
          type: integer
        - name: name
          type: string
        - name: population
          type: integer
    scheme: file
```

To continue learning about the describe functions please read:
- API Reference: describe
- API Reference: describe_resource
- API Reference: describe_package
