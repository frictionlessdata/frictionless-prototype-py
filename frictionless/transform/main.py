from .package import transform_package


def transform(source):
    """Transform resource

    Parameters:
        source (any): data source
    """
    return transform_package(source)
