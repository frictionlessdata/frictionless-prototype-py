# Errors Reference

> it's a work in progress

This document provides a full reference of the Frictionless errors.

## Error

> `error`

Tags: `` <br>
Template: {note} <br>
Description: Error.


## Header Error

> `header-error`

Tags: `#head` <br>
Template: Cell Error <br>
Description: Cell Error


## Row Error

> `row-error`

Tags: `#body` <br>
Template: Row Error <br>
Description: Row Error


## Cell Error

> `cell-error`

Tags: `#body` <br>
Template: Cell Error <br>
Description: Cell Error


## Package Error

> `package-error`

Tags: `#general` <br>
Template: The data package has an error: {note} <br>
Description: A validation cannot be processed.


## Resource Error

> `resource-error`

Tags: `#general` <br>
Template: The data resource has an error: {note} <br>
Description: A validation cannot be processed.


## Inquiry Error

> `inquiry-error`

Tags: `#general` <br>
Template: The inquiry is not valid: {note} <br>
Description: Provided inquiry is not valid.


## Report Error

> `report-error`

Tags: `#general` <br>
Template: The validation report has an error: {note} <br>
Description: A validation cannot be presented.


## Pipeline Error

> `pipeline-error`

Tags: `#general` <br>
Template: The pipeline is not valid: {note} <br>
Description: Provided pipeline is not valid.


## Task Error

> `task-error`

Tags: `#general` <br>
Template: The validation task has an error: {note} <br>
Description: General task-level error.


## Check Error

> `check-error`

Tags: `#general` <br>
Template: The validation check has an error: {note} <br>
Description: A validation check cannot be created


## Source Error

> `source-error`

Tags: `#table` <br>
Template: The data source has not supported or has inconsistent contents: {note} <br>
Description: Data reading error because of not supported or inconsistent contents.


## Scheme Error

> `scheme-error`

Tags: `#table` <br>
Template: The data source could not be successfully loaded: {note} <br>
Description: Data reading error because of incorrect scheme.


## Format Error

> `format-error`

Tags: `#table` <br>
Template: The data source could not be successfully parsed: {note} <br>
Description: Data reading error because of incorrect format.


## Encoding Error

> `encoding-error`

Tags: `#table` <br>
Template: The data source could not be successfully decoded: {note} <br>
Description: Data reading error because of an encoding problem.


## Hashing Error

> `hashing-error`

Tags: `#table` <br>
Template: The data source could not be successfully hashed: {note} <br>
Description: Data reading error because of a hashing problem.


## Compression Error

> `compression-error`

Tags: `#table` <br>
Template: The data source could not be successfully decompressed: {note} <br>
Description: Data reading error because of a decompression problem.


## Control Error

> `control-error`

Tags: `#table #control` <br>
Template: Control object is not valid: {note} <br>
Description: Provided control is not valid.


## Dialect Error

> `dialect-error`

Tags: `#table #dialect` <br>
Template: Dialect object is not valid: {note} <br>
Description: Provided dialect is not valid.


## Schema Error

> `schema-error`

Tags: `#table #schema` <br>
Template: The data source could not be successfully described by the invalid Table Schema: {note} <br>
Description: Provided schema is not valid.


## Field Error

> `field-error`

Tags: `#table schema #field` <br>
Template: The data source could not be successfully described by the invalid Table Schema: {note} <br>
Description: Provided field is not valid.


## Query Error

> `query-error`

Tags: `#table #query` <br>
Template: The data source could not be successfully described by the invalid Table Query: {note} <br>
Description: Provided query is not valid.


## Checksum Error

> `checksum-error`

Tags: `#table #checksum` <br>
Template: The data source does not match the expected checksum: {note} <br>
Description: This error can happen if the data is corrupted.


## Extra Header

> `extra-header`

Tags: `#head #structure` <br>
Template: There is an extra header "{cell}" in field at position "{fieldPosition}" <br>
Description: The first row of the data source contains header that does not exist in the schema.


## Missing Header

> `missing-header`

Tags: `#head #structure` <br>
Template: There is a missing header in the field "{fieldName}" at position "{fieldPosition}" <br>
Description: Based on the schema there should be a header that is missing in the first row of the data source.


## Blank Header

> `blank-header`

Tags: `#head #structure` <br>
Template: Header in field at position "{fieldPosition}" is blank <br>
Description: A column in the header row is missing a value. Headers should be provided and not be blank.


## Duplicate Header

> `duplicate-header`

Tags: `#head #structure` <br>
Template: Header "{cell}" in field at position "{fieldPosition}" is duplicated to header in another field: {note} <br>
Description: Two columns in the header row have the same value. Column names should be unique.


## Non-matching Header

> `non-matching-header`

Tags: `#head #schema` <br>
Template: Header "{cell}" in field {fieldName} at position "{fieldPosition}" does not match the field name in the schema <br>
Description: One of the data source headers does not match the field name defined in the schema.


## Extra Cell

> `extra-cell`

Tags: `#body #structure` <br>
Template: Row at position "{rowPosition}" has an extra value in field at position "{fieldPosition}" <br>
Description: This row has more values compared to the header row (the first row in the data source). A key concept is that all the rows in tabular data must have the same number of columns.


## Missing Cell

> `missing-cell`

Tags: `#body #structure` <br>
Template: Row at position "{rowPosition}" has a missing cell in field "{fieldName}" at position "{fieldPosition}" <br>
Description: This row has less values compared to the header row (the first row in the data source). A key concept is that all the rows in tabular data must have the same number of columns.


## Blank Row

> `blank-row`

Tags: `#body #structure` <br>
Template: Row at position "{rowPosition}" is completely blank <br>
Description: This row is empty. A row should contain at least one value.


## Missing Cell

> `type-error`

Tags: `#body #schema` <br>
Template: The cell "{cell}" in row at position "{rowPosition}" and field "{fieldName}" at position "{fieldPosition}" has incompatible type: {note} <br>
Description: The value does not match the schema type and format for this field.


## Constraint Error

> `constraint-error`

Tags: `#body #schema` <br>
Template: The cell "{cell}" in row at position "{rowPosition}" and field "{fieldName}" at position "{fieldPosition}" does not conform to a constraint: {note} <br>
Description: A field value does not conform to a constraint.


## Unique Error

> `unique-error`

Tags: `#body #schema #integrity` <br>
Template: Row at position "{rowPosition}" has unique constraint violation in field "{fieldName}" at position "{fieldPosition}": {note} <br>
Description: This field is a unique field but it contains a value that has been used in another row.


## PrimaryKey Error

> `primary-key-error`

Tags: `#body #schema #integrity` <br>
Template: The row at position "{rowPosition}" does not conform to the primary key constraint: {note} <br>
Description: Values in the primary key fields should be unique for every row


## ForeignKey Error

> `foreign-key-error`

Tags: `#body #schema #integrity` <br>
Template: The row at position "{rowPosition}" does not conform to the foreign key constraint: {note} <br>
Description: Values in the foreign key fields should exist in the reference table


## Duplicate Row

> `duplicate-row`

Tags: `#body #heuristic` <br>
Template: Row at position {rowPosition} is duplicated: {note} <br>
Description: The row is duplicated.


## Deviated Value

> `deviated-value`

Tags: `#body #heuristic` <br>
Template: There is a possible error because the value is deviated: {note} <br>
Description: The value is deviated.


## Truncated Value

> `truncated-value`

Tags: `#body #heuristic` <br>
Template: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note} <br>
Description: The value is possible truncated.


## Blacklisted Value

> `blacklisted-value`

Tags: `#body #regulation` <br>
Template: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note} <br>
Description: The value is blacklisted.


## Sequential Value

> `sequential-value`

Tags: `#body #regulation` <br>
Template: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note} <br>
Description: The value is not sequential.


## Row Constraint

> `row-constraint`

Tags: `#body #regulation` <br>
Template: The row at position {rowPosition} has an error: {note} <br>
Description: The value does not conform to the row constraint.


