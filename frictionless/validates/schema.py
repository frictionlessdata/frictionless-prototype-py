from .. import helpers
from ..report import Report
from ..schema import Schema
from ..errors import SchemaError
from .. import exceptions


@Report.from_validate
def validate_schema(source):
    """Validate schema
    """

    # Create state
    timer = helpers.Timer()

    # Create schema
    try:
        schema = Schema(source)
    except exceptions.FrictionlessException as exception:
        error = SchemaError(note=str(exception))
        return Report(time=timer.time, errors=[error], tables=[])

    # Validate schema
    errors = []
    for error in schema.metadata_errors:
        errors.append(error)

    # Return report
    return Report(time=timer.time, errors=errors, tables=[])
