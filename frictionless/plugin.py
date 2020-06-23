class Plugin:
    """Plugin representation
    """

    def create_check(self, name, *, descriptor=None):
        pass

    def create_loader(self, source, *, control=None):
        pass

    def create_server(self, name):
        pass
