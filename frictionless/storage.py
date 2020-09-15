class Storage:
    def __init__(self, **options):
        pass

    # Delete

    def delete_resource(self, name, *, ignore=False, **options):
        pass

    def delete_package(self, names, *, ignore=False, **options):
        pass

    # Read

    def read_resource(self, name, **options):
        pass

    def read_package(self, **options):
        pass

    # Write

    def write_resource(self, resource, *, force=False, **options):
        pass

    def write_package(self, package, *, force=False, **options):
        pass


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
