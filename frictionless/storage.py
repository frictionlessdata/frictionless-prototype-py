class Storage:
    def __init__(self, **options):
        pass

    @property
    def tables(self):
        pass

    @property
    def table_names(self):
        pass

    def add_table(self, *tables, **options):
        pass

    def get_table(self, name):
        pass

    def has_table(self, name):
        pass

    def remove_table(self, *names, **options):
        pass


class StorageTable:
    def __init__(self, name, *, schema):
        self.__name = name
        self.__schema = schema

    @property
    def name(self):
        return self.__name

    @property
    def schema(self):
        return self.__schema
