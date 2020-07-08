from .. import helpers
from ..report import Report
from ..package import Package
from ..inquiry import Inquiry
from .inquiry import validate_inquiry
from .. import exceptions


@Report.from_validate
def validate_package(source, basepath=None, noinfer=False, **options):
    """Validate package
    """

    # Create state
    timer = helpers.Timer()

    # Create package
    try:
        package = Package(source, basepath=basepath)
    except exceptions.FrictionlessException as exception:
        return Report(time=timer.time, errors=[exception.error], tables=[])

    # Prepare package
    if not noinfer:
        package.infer()
    if package.metadata_errors:
        return Report(time=timer.time, errors=package.metadata_errors, tables=[])

    # Prepare inquiry
    descriptor = {"tasks": []}
    for resource in package.resources:
        lookup = helpers.create_lookup(resource, package=package)
        descriptor["tasks"].append(
            helpers.create_descriptor(
                **options,
                source=resource,
                basepath=resource.basepath,
                noinfer=noinfer,
                lookup=lookup,
            )
        )

    # Validate inquiry
    inquiry = Inquiry(descriptor)
    report = validate_inquiry(inquiry)

    # Return report
    return Report(time=timer.time, errors=report["errors"], tables=report["tables"])
