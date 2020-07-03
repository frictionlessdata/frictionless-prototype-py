import gunicorn
from ..plugin import Plugin
from ..server import Server


# Plugin


class ServerPlugin(Plugin):
    def create_server(self, name):
        if name == "api":
            return ApiServer()


# Servers


class ApiServer(Server):
    def listen(self, port):
        gunicorn
