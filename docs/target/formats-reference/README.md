# Formats Reference

It's a formats reference supported by the main Frictionless package. If you have installed external plugins there can be more formats available. Below we're listing a format group name (or a parser name) like Excel which is used, for example, for `xlsx`, `xls` etc formats. Options can be used for creating dialects, for example, `dialect = ExcelDialect(sheet=1)`.


## Csv


### Options

#### Delimiter

> Type: str

Csv delimiter

#### Line Terminator

> Type: str

Line_terminator

#### Quote Char

> Type: str

Quote_char

#### Double Quote

> Type: bool

Double_quote

#### Escape Char

> Type: str

Escape_char

#### Null Sequence

> Type: str

Null_sequence

#### Skip Initial Space

> Type: bool

Skip_initial_space

#### Comment Char

> Type: str

Comment_char

#### Case Sensitive Header

> Type: bool

Case_sensitive_header



## Excel


### Options

#### Sheet

> Type: int|str

Sheet

#### Workbook Cache

> Type: dict

Workbook_cache

#### Fill Merged Cells

> Type: bool

Fill_merged_cells

#### Preserve Formatting

> Type: bool

Preserve_formatting

#### Adjust Floating Point Error

> Type: bool

Adjust_floating_point_error



## Inline


### Options

#### Keyed

> Type: bool

Keyed



## Json


### Options

#### Keyed

> Type: bool

Keyed

#### Property

> Type: str

Property



## Gsheet


There are no options available.


## Html


### Options

#### Selector

> Type: str

Selector



## Ods


### Options

#### Sheet

> Type: str

Sheet



## Sql


### Options

#### Table

> Type: str

Table

#### Order By

> Type: str

Order_by



## Tsv


There are no options available.

