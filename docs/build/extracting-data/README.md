# Extracting Data

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1is_PcpzFl42aWI2B2tHaBGj3jxsKZ_eZ)



Extracting data means reading tabular data from some source. We can use various customization for this process as though providing a file format, table schema, limiting fields or rows amount, and many more. Let's see on a real file:


```
! pip install frictionless
```


```
! cat data/country-3.csv
```

    id,capital_id,name,population
    1,1,Britain,67
    2,3,France,67
    3,2,Germany,83
    4,5,Italy,60
    5,4,Spain,47


For a starter, we will use the command-line interface:


```
! frictionless extract data/country-3.csv
```

    [data] data/country-3.csv

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

rows = extract('data/country-3.csv')
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



### Extracting Package

The easiest way is to use a command line-interface. We're going to provide two files to the `extract` command which will be enough to detect that it's a dataset:


```
! frictionless extract data/*-3.csv
```

    [data] data/capital-3.csv

      id  name
    ----  ------
       1  London
       2  Berlin
       3  Paris
       4  Madrid
       5  Rome

    [data] data/country-3.csv

      id    capital_id  name       population
    ----  ------------  -------  ------------
       1             1  Britain            67
       2             3  France             67
       3             2  Germany            83
       4             5  Italy              60
       5             4  Spain              47


In Python the same operation will return a dictionary indexed by a path:


```
from frictionless import extract_package

data = extract_package('data/*-3.csv')
for path, rows in data.items():
  pprint(path)
  pprint(rows)
```

    'data/capital-3.csv'
    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]
    'data/country-3.csv'
    [Row([('id', 1), ('capital_id', 1), ('name', 'Britain'), ('population', 67)]),
     Row([('id', 2), ('capital_id', 3), ('name', 'France'), ('population', 67)]),
     Row([('id', 3), ('capital_id', 2), ('name', 'Germany'), ('population', 83)]),
     Row([('id', 4), ('capital_id', 5), ('name', 'Italy'), ('population', 60)]),
     Row([('id', 5), ('capital_id', 4), ('name', 'Spain'), ('population', 47)])]


### Extracting Resource

A resource contains only one file and for extracting a resource we can use the same approach we used above but providing only one file. It would be boring to do the same thing againg so we will use a different method: extracring data using a metadata descriptor:


```
from frictionless import extract_resource

descriptor = {'path': 'data/capital-3.csv'}
rows = extract_resource(descriptor)
pprint(rows)
```

    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', 3), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


Usually, the code above doesn't really make sense as we can just provide a path instead of a descriptor but the power of the descriptor is that it can contain different metadata and be stored on the disc. Let's extend our example:


```
from frictionless import Resource

resource = Resource(path='data/capital-3.csv')
resource.schema.missing_values.append('3')
resource.to_yaml('capital.resource.yaml')
```


```
! cat capital.resource.yaml
```

    path: data/capital-3.csv
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

rows = extract_table('data/capital-3.csv', patch_schema={'missingValues': ['', '3']})
pprint(rows)
```

    [Row([('id', 1), ('name', 'London')]),
     Row([('id', 2), ('name', 'Berlin')]),
     Row([('id', None), ('name', 'Paris')]),
     Row([('id', 4), ('name', 'Madrid')]),
     Row([('id', 5), ('name', 'Rome')])]


We got in idential result but it's important to understand that on the table level we need to provide all the metadata options separately while a resource incapsulate all these metadata. Please check the `extract_table` API Referenec as it has a lot of options. We're going to discuss some of the below.

## File details

Let's overview the details we can specify for a file

#### scheme

#### format

## Table Dialect

## Table Query

## Table Schema

## Headers


## Row Object

## Using Metadata