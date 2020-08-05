from .resource import describe_resource


def describe_schema(source, **options):
    resource = describe_resource(source, **options)
    schema = resource.schema
    return schema
