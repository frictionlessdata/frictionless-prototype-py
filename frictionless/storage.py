class Storage:
    def __init__(self, **options):
        pass

    # Read

    def read_package(self, **options):
        pass

    def read_resource(self, name, **options):
        pass

    # Write

    def write_package(self, package, *, force=False, **options):
        pass

    def write_resource(self, resource, *, force=False, **options):
        pass

    # Delete

    def delete_package(self, names, *, ignore=False, **options):
        pass

    def delete_resource(self, name, *, ignore=False, **options):
        pass


# TODO: remove after the migration to new storage API
class StorageTable:
    def __init__(self, name, *, schema):
        self.__name = name
        self.__schema = schema

    def __repr__(self):
        template = "StorageTable <{name}>"
        text = template.format(name=self.__name)
        return text

    @property
    def name(self):
        return self.__name

    @property
    def schema(self):
        return self.__schema
