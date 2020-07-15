from ..pipeline import Pipeline


def transform(source):

    # Run pipeline
    pipeline = Pipeline(source)
    pipeline.run()
