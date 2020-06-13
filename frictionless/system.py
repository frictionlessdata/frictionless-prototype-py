import importlib
from . import exceptions


class System:
    def __init__(self):
        self.__plugins = {}

    # Checks

    def create_check(self, name, *, descriptor=None):
        if name.count('/') != 1:
            message = f'Check name "{name}" should be in "plugin/element" form'
            raise exceptions.FrictionlessException(message)
        plugin_name, *rest = name.split('/', 1)
        plugin = self.load_plugin(plugin_name)
        check = plugin.create_check(name, descriptor=descriptor)
        if check is None:
            message = f'Plugin "{plugin_name}" does not support check "{name}"'
            raise exceptions.FrictionlessException(message)
        return check

    # Servers

    def create_server(self, name):
        if name.count('/') != 1:
            message = f'Server name "{name}" should be in "plugin/element" form'
            raise exceptions.FrictionlessException(message)
        plugin_name, *rest = name.split('/', 1)
        plugin = self.load_plugin(plugin_name)
        server = plugin.create_server(name)
        if server is None:
            message = f'Plugin "server" does not support server "{name}"'
            raise exceptions.FrictionlessException(message)
        return server

    # Plugins

    def load_plugin(self, name):
        if name not in self.__plugins:
            module = None
            internal = f'frictionless.plugins.{name}'
            external = f'frictionless-{name}'
            for module_name in [internal, external]:
                try:
                    module = importlib.import_module(module_name)
                    break
                except ImportError as exception:
                    # Plugin is available but its dependencies are not
                    if module_name == getattr(exception, 'name', None):
                        command = f'pip install frictionless[{name}]'
                        message = f'Plugin "{name}" is not installed. Run: "{command}"'
                        raise exceptions.FrictionlessException(message)
                    pass
            if not module:
                command = f'pip install frictionless-{name}'
                message = f'Plugin "{name}" is not installed. Run: "{command}"'
                raise exceptions.FrictionlessException()
            self.__plugins[name] = getattr(module, f'{name.capitalize()}Plugin')()
        return self.__plugins[name]


system = System()
