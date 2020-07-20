# Errors Reference

> it's a work in progress

This document provides a full reference of the Frictionless errors.

## Error

> `error` [``]

Template
: {note}

Description
: Error.

## Header Error

> `header-error` [`#head`]

Template
: Cell Error

Description
: Cell Error

## Row Error

> `row-error` [`#body`]

Template
: Row Error

Description
: Row Error

## Cell Error

> `cell-error` [`#body`]

Template
: Cell Error

Description
: Cell Error

## Package Error

> `package-error` [`#general`]

Template
: The data package has an error: {note}

Description
: A validation cannot be processed.

## Resource Error

> `resource-error` [`#general`]

Template
: The data resource has an error: {note}

Description
: A validation cannot be processed.

## Inquiry Error

> `inquiry-error` [`#general`]

Template
: The inquiry is not valid: {note}

Description
: Provided inquiry is not valid.

## Report Error

> `report-error` [`#general`]

Template
: The validation report has an error: {note}

Description
: A validation cannot be presented.

## Pipeline Error

> `pipeline-error` [`#general`]

Template
: The pipeline is not valid: {note}

Description
: Provided pipeline is not valid.

## Task Error

> `task-error` [`#general`]

Template
: The validation task has an error: {note}

Description
: General task-level error.

## Check Error

> `check-error` [`#general`]

Template
: The validation check has an error: {note}

Description
: A validation check cannot be created

## Source Error

> `source-error` [`#table`]

Template
: The data source has not supported or has inconsistent contents: {note}

Description
: Data reading error because of not supported or inconsistent contents.

## Scheme Error

> `scheme-error` [`#table`]

Template
: The data source could not be successfully loaded: {note}

Description
: Data reading error because of incorrect scheme.

## Format Error

> `format-error` [`#table`]

Template
: The data source could not be successfully parsed: {note}

Description
: Data reading error because of incorrect format.

## Encoding Error

> `encoding-error` [`#table`]

Template
: The data source could not be successfully decoded: {note}

Description
: Data reading error because of an encoding problem.

## Hashing Error

> `hashing-error` [`#table`]

Template
: The data source could not be successfully hashed: {note}

Description
: Data reading error because of a hashing problem.

## Compression Error

> `compression-error` [`#table`]

Template
: The data source could not be successfully decompressed: {note}

Description
: Data reading error because of a decompression problem.

## Control Error

> `control-error` [`#table #control`]

Template
: Control object is not valid: {note}

Description
: Provided control is not valid.

## Dialect Error

> `dialect-error` [`#table #dialect`]

Template
: Dialect object is not valid: {note}

Description
: Provided dialect is not valid.

## Schema Error

> `schema-error` [`#table #schema`]

Template
: The data source could not be successfully described by the invalid Table Schema: {note}

Description
: Provided schema is not valid.

## Field Error

> `field-error` [`#table schema #field`]

Template
: The data source could not be successfully described by the invalid Table Schema: {note}

Description
: Provided field is not valid.

## Query Error

> `query-error` [`#table #query`]

Template
: The data source could not be successfully described by the invalid Table Query: {note}

Description
: Provided query is not valid.

## Checksum Error

> `checksum-error` [`#table #checksum`]

Template
: The data source does not match the expected checksum: {note}

Description
: This error can happen if the data is corrupted.

## Extra Header

> `extra-header` [`#head #structure`]

Template
: There is an extra header "{cell}" in field at position "{fieldPosition}"

Description
: The first row of the data source contains header that does not exist in the schema.

## Missing Header

> `missing-header` [`#head #structure`]

Template
: There is a missing header in the field "{fieldName}" at position "{fieldPosition}"

Description
: Based on the schema there should be a header that is missing in the first row of the data source.

## Blank Header

> `blank-header` [`#head #structure`]

Template
: Header in field at position "{fieldPosition}" is blank

Description
: A column in the header row is missing a value. Headers should be provided and not be blank.

## Duplicate Header

> `duplicate-header` [`#head #structure`]

Template
: Header "{cell}" in field at position "{fieldPosition}" is duplicated to header in another field: {note}

Description
: Two columns in the header row have the same value. Column names should be unique.

## Non-matching Header

> `non-matching-header` [`#head #schema`]

Template
: Header "{cell}" in field {fieldName} at position "{fieldPosition}" does not match the field name in the schema

Description
: One of the data source headers does not match the field name defined in the schema.

## Extra Cell

> `extra-cell` [`#body #structure`]

Template
: Row at position "{rowPosition}" has an extra value in field at position "{fieldPosition}"

Description
: This row has more values compared to the header row (the first row in the data source). A key concept is that all the rows in tabular data must have the same number of columns.

## Missing Cell

> `missing-cell` [`#body #structure`]

Template
: Row at position "{rowPosition}" has a missing cell in field "{fieldName}" at position "{fieldPosition}"

Description
: This row has less values compared to the header row (the first row in the data source). A key concept is that all the rows in tabular data must have the same number of columns.

## Blank Row

> `blank-row` [`#body #structure`]

Template
: Row at position "{rowPosition}" is completely blank

Description
: This row is empty. A row should contain at least one value.

## Missing Cell

> `type-error` [`#body #schema`]

Template
: The cell "{cell}" in row at position "{rowPosition}" and field "{fieldName}" at position "{fieldPosition}" has incompatible type: {note}

Description
: The value does not match the schema type and format for this field.

## Constraint Error

> `constraint-error` [`#body #schema`]

Template
: The cell "{cell}" in row at position "{rowPosition}" and field "{fieldName}" at position "{fieldPosition}" does not conform to a constraint: {note}

Description
: A field value does not conform to a constraint.

## Unique Error

> `unique-error` [`#body #schema #integrity`]

Template
: Row at position "{rowPosition}" has unique constraint violation in field "{fieldName}" at position "{fieldPosition}": {note}

Description
: This field is a unique field but it contains a value that has been used in another row.

## PrimaryKey Error

> `primary-key-error` [`#body #schema #integrity`]

Template
: The row at position "{rowPosition}" does not conform to the primary key constraint: {note}

Description
: Values in the primary key fields should be unique for every row

## ForeignKey Error

> `foreign-key-error` [`#body #schema #integrity`]

Template
: The row at position "{rowPosition}" does not conform to the foreign key constraint: {note}

Description
: Values in the foreign key fields should exist in the reference table

## Duplicate Row

> `duplicate-row` [`#body #heuristic`]

Template
: Row at position {rowPosition} is duplicated: {note}

Description
: The row is duplicated.

## Deviated Value

> `deviated-value` [`#body #heuristic`]

Template
: There is a possible error because the value is deviated: {note}

Description
: The value is deviated.

## Truncated Value

> `truncated-value` [`#body #heuristic`]

Template
: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note}

Description
: The value is possible truncated.

## Blacklisted Value

> `blacklisted-value` [`#body #regulation`]

Template
: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note}

Description
: The value is blacklisted.

## Sequential Value

> `sequential-value` [`#body #regulation`]

Template
: The cell {cell} in row at position {rowPosition} and field {fieldName} at position {fieldPosition} has an error: {note}

Description
: The value is not sequential.

## Row Constraint

> `row-constraint` [`#body #regulation`]

Template
: The row at position {rowPosition} has an error: {note}

Description
: The value does not conform to the row constraint.

