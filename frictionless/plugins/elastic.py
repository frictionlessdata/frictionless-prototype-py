from ..plugin import Plugin


# Plugin


class ElasticPlugin(Plugin):
    """Plugin for ElasticSearch

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.elastic import ElasticPlugin`

    """

    def create_storage(self, source):
        pass
