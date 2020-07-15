from ..package import Package


def describe_package(source, *, expand=False):

    # Infer package
    package = Package()
    package.infer(source)

    # Expand package
    if expand:
        package.expand()

    return package
