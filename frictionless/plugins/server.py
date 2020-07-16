from ..plugin import Plugin
from ..server import Server
from .. import helpers


# Plugin


class ServerPlugin(Plugin):
    def create_server(self, name):
        if name == "api":
            return ApiServer()


# Servers


class ApiServer(Server):
    def listen(self, port):
        gunicorn = helpers.import_from_plugin("gunicorn", plugin="server")
        gunicorn
