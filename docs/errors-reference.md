# Errors Reference

> it's a work in progress

This document provides a full reference of the Frictionless errors.

## Error

> `error`

Template: {note}
Description: Error.
Tags: ``

## Header Error

> `header-error`

Template: Cell Error
Description: Cell Error
Tags: `#head`

## Row Error

> `row-error`

Template: Row Error
Description: Row Error
Tags: `#body`

## Cell Error

> `cell-error`

Template: Cell Error
Description: Cell Error
Tags: `#body`

## Package Error

> `package-error`

Template: The data package has an error: {note}
Description: A validation cannot be processed.
Tags: `#general`

## Resource Error

> `resource-error`

Template: The data resource has an error: {note}
Description: A validation cannot be processed.
Tags: `#general`

## Inquiry Error

> `inquiry-error`

Template: The inquiry is not valid: {note}
Description: Provided inquiry is not valid.
Tags: `#general`

## Report Error

> `report-error`

Template: The validation report has an error: {note}
Description: A validation cannot be presented.
Tags: `#general`

## Pipeline Error

> `pipeline-error`

Template: The pipeline is not valid: {note}
Description: Provided pipeline is not valid.
Tags: `#general`

## Task Error

> `task-error`

Template: The validation task has an error: {note}
Description: General task-level error.
Tags: `#general`

## Check Error

> `check-error`

Template: The validation check has an error: {note}
Description: A validation check cannot be created
Tags: `#general`

## Source Error

> `source-error`

Template: The data source has not supported or has inconsistent contents: {note}
Description: Data reading error because of not supported or inconsistent contents.
Tags: `#table`

## Scheme Error

> `scheme-error`

Template: The data source could not be successfully loaded: {note}
Description: Data reading error because of incorrect scheme.
Tags: `#table`

## Format Error

> `format-error`

Template: The data source could not be successfully parsed: {note}
Description: Data reading error because of incorrect format.
Tags: `#table`

## Encoding Error

> `encoding-error`

Template: The data source could not be successfully decoded: {note}
Description: Data reading error because of an encoding problem.
Tags: `#table`

## Hashing Error

> `hashing-error`

Template: The data source could not be successfully hashed: {note}
Description: Data reading error because of a hashing problem.
Tags: `#table`

## Compression Error

> `compression-error`

Template: The data source could not be successfully decompressed: {note}
Description: Data reading error because of a decompression problem.
Tags: `#table`

## Control Error

> `control-error`

Template: Control object is not valid: {note}
Description: Provided control is not valid.
Tags: `#table #control`

## Dialect Error

> `dialect-error`

Template: Dialect object is not valid: {note}
Description: Provided dialect is not valid.
Tags: `#table #dialect`

## Schema Error

> `schema-error`

Template: The data source could not be successfully described by the invalid Table Schema: {note}
Description: Provided schema is not valid.
Tags: `#table #schema`

## Field Error

> `field-error`

Template: The data source could not be successfully described by the invalid Table Schema: {note}
Description: Provided field is not valid.
Tags: `#table schema #field`

## Query Error

> `query-error`

Template: The data source could not be successfully described by the invalid Table Query: {note}
Description: Provided query is not valid.
Tags: `#table #query`

## Checksum Error

> `checksum-error`

Template: The data source does not match the expected checksum: {note}
Description: This error can happen if the data is corrupted.
Tags: `#table #checksum`

## Extra Header

> `extra-header`

Template: There is an extra header "{cell}" in field at position "{fieldPosition}"
Description: The first row of the data source contains header that does not exist in the schema.
Tags: `#head #structure`

## Missing Header

> `missing-header`

Template: There is a missing header in the field "{fieldName}" at position "{fieldPosition}"
Description: Based on the schema there should be a header that is missing in the first row of the data source.
Tags: `#head #structure`

## Blank Header

> `blank-header`

Template: Header in field at position "{fieldPosition}" is blank
Description: A column in the header row is missing a value. Headers should be provided and not be blank.
Tags: `#head #structure`

## Duplicate Header

> `duplicate-header`

Template: Header "{cell}" in field at position "{fieldPosition}" is duplicated to header in another field: {note}
Description: Two columns in the header row have the same value. Column names should be unique.
Tags: `#head #structure`

## Non-matching Header

> `non-matching-header`

Template: Header "{cell}" in field {fieldName} at position "{fieldPosition}" does not match the field name in the schema
Description: One of the data source headers does not match the field name defined in the schema.
Tags: `#head #schema`

## Extra Cell

> `extra-cell`

Template: Row at position "{rowPosition}" has an extra value in field at position "{fieldPosition}"
Description: This row has more values compared to the header row (the first row in the data source). A key concept is that all the rows in tabular data must have the same number of columns.
Tags: `#body #structure`

## Missing Cell

> `missing-cell`

Template: Row at position "{rowPosition}" has a missing cell in field "{fieldName}" at position "{fieldPosition}"
Description: This row has less values compared to the header row (the first row in the data source). A key concept is that all the rows in tabular data must have the same number of columns.
Tags: `#body #structure`

## Blank Row

> `blank-row`

Template: Row at position "{rowPosition}" is completely blank
Description: This row is empty. A row should contain at least one value.
Tags: `#body #structure`

## Missing Cell

> `type-error`

Template: The cell "{cell}" in row at position "{rowPosition}" and field "{fieldName}" at position "{fieldPosition}" has incompatible type: {note}
Description: The value does not match the schema type and format for this field.
Tags: `#body #schema`

## Constraint Error

> `constraint-error`

Template: The cell "{cell}" in row at position "{rowPosition}" and field "{fieldName}" at position "{fieldPosition}" does not conform to a constraint: {note}
Description: A field value does not conform to a constraint.
Tags: `#body #schema`

## Unique Error

> `unique-error`

Template: Row at position "{rowPosition}" has unique constraint violation in field "{fieldName}" at position "{fieldPosition}": {note}
Description: This field is a unique field but it contains a value that has been used in another row.
Tags: `#body #schema #integrity`

## PrimaryKey Error

> `primary-key-error`

Template: The row at position "{rowPosition}" does not conform to the primary key constraint: {note}
Description: Values in the primary key fields should be unique for every row
Tags: `#body #schema #integrity`

## ForeignKey Error

> `foreign-key-error`

Template: The row at position "{rowPosition}" does not conform to the foreign key constraint: {note}
Description: Values in the foreign key fields should exist in the reference table
Tags: `#body #schema #integrity`

## Duplicate Row

> `duplicate-row`

Template: Row at position {rowPosition} is duplicated: {note}
Description: The row is duplicated.
Tags: `#body #heuristic`

## Deviated Value

> `deviated-value`

Template: There is a possible error because the value is deviated: {note}
Description: The value is deviated.
Tags: `#body #heuristic`

## Truncated Value

> `truncated-value`

Template: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note}
Description: The value is possible truncated.
Tags: `#body #heuristic`

## Blacklisted Value

> `blacklisted-value`

Template: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note}
Description: The value is blacklisted.
Tags: `#body #regulation`

## Sequential Value

> `sequential-value`

Template: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note}
Description: The value is not sequential.
Tags: `#body #regulation`

## Row Constraint

> `row-constraint`

Template: The row at position {rowPosition} has an error: {note}
Description: The value does not conform to the row constraint.
Tags: `#body #regulation`

