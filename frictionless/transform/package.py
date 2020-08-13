from ..pipeline import Pipeline


def transform_package(source):
    """Transform package

    Parameters:
        source (any): a pipeline descriptor

    """

    # Run pipeline
    pipeline = Pipeline(source)
    pipeline.run()
