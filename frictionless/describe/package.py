from ..package import Package


def describe_package(source, *, expand=False):
    """Describe the given source as a package

    API      | Usage
    -------- | --------
    Public   | `from frictionless import describe_package`

    Parameters:
        source (any): data source
        expand? (bool): if `True` it will expand the metadata

    Returns:
        Package: data package

    """

    # Infer package
    package = Package()
    package.infer(source)

    # Expand package
    if expand:
        package.expand()

    return package
