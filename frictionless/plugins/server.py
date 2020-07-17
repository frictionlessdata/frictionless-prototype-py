import multiprocessing
from ..plugin import Plugin
from ..server import Server
from .. import helpers
from .. import config


# Plugin


class ServerPlugin(Plugin):
    def create_server(self, name):
        if name == "api":
            return ApiServer()


# Servers


class ApiServer(Server):
    def listen(self, *, port):
        app = create_api()
        server = create_server(app, port=port)
        server.run()


# Internal


def create_api():
    flask = helpers.import_from_plugin("flask", plugin="server")

    # Create api
    app = flask.Flask("app")

    @app.route("/")
    def main():
        return flask.jsonify(
            {
                "version": config.VERSION,
                "options": ["/describe", "/extract", "/validate", "/transform"],
            }
        )

    return app


def create_server(app, *, port):
    # https://docs.gunicorn.org/en/latest/custom.html
    base = helpers.import_from_plugin("gunicorn.app.base", plugin="server")

    # Define server
    class Server(base.BaseApplication):
        def __init__(self, app, options=None):
            self.options = options or {}
            self.application = app
            super().__init__()

        def load_config(self):
            config = {
                key: value
                for key, value in self.options.items()
                if key in self.cfg.settings and value is not None
            }
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application

    # Define options
    options = {
        "bind": "%s:%s" % ("127.0.0.1", str(port)),
        "workers": (multiprocessing.cpu_count() * 2) + 1,
        "accesslog": "-",
    }

    # Return server
    server = Server(app, options)
    return server
