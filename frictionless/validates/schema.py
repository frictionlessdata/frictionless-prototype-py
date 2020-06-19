from .. import helpers
from ..report import Report
from ..schema import Schema
from ..errors import SchemaError
from .. import exceptions


@Report.from_validate
def validate_schema(source):
    """Validate schema
    """

    # Prepare state
    timer = helpers.Timer()

    # Create schema
    try:
        schema = Schema(source)
    except exceptions.FrictionlessException as exception:
        time = timer.get_time()
        error = SchemaError(note=str(exception))
        return Report(time=time, errors=[error], tables=[])

    # Validate schema
    errors = []
    for error in schema.metadata_errors:
        errors.append(error)

    # Return report
    time = timer.get_time()
    return Report(time=time, errors=errors, tables=[])
