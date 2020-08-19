class Storage:
    def __init__(self, **options):
        pass

    def add_table(self, *tables, force=False, **options):
        pass

    def remove_table(self, *names, ignore=False, **options):
        pass

    def get_table(self, name):
        pass

    def has_table(self, name):
        pass

    def list_tables(self):
        pass

    def list_table_names(self):
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
