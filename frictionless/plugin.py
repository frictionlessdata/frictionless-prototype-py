class Plugin:
    """Plugin representation
    """

    def create_check(self, name, *, descriptor=None):
        pass

    def create_loader(self, location, *, control=None):
        pass

    def create_parser(self, location, *, control=None, dialect=None):
        pass

    def create_server(self, name):
        pass
