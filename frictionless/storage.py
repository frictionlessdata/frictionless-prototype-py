class Storage:
    @property
    def resources(self):
        pass

    @property
    def resource_names(self):
        pass

    def add_resource(self, *descriptors, **options):
        pass

    def get_resource(self, name):
        pass

    def has_resource(self, name):
        pass

    def remove_resource(self, *names, **options):
        pass
