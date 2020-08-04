from ..pipeline import Pipeline


def transform_package(source):

    # Run pipeline
    pipeline = Pipeline(source)
    pipeline.run()
