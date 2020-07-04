from importlib import import_module
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
        gunicorn = import_module("gunicorn")
        gunicorn
