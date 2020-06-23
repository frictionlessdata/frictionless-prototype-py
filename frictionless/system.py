import os
import pkgutil
from collections import OrderedDict
from importlib import import_module
from .errors import Error
from . import exceptions
from . import plugins


class System:
    """System representation
    """

    def __init__(self):
        self.__plugins = OrderedDict()
        self.__actions = {
            'create_check': OrderedDict(),
            'create_server': OrderedDict(),
        }

    # Checks

    def create_check(self, name, *, descriptor=None):
        check = None
        for func in self.__actions['create_check'].values():
            check = func(name, descriptor=descriptor)
            if check is not None:
                break
        if check is None:
            note = f'Cannot create check "{name}". Try installing "frictionless-{name}"'
            raise exceptions.FrictionlessException(Error(note=note))
        return check

    # Servers

    def create_server(self, name):
        server = None
        for func in self.__actions['create_server'].values():
            server = func(name)
            if server is not None:
                break
        if server is None:
            note = f'Cannot create server "{name}". Try installing "frictionless-{name}"'
            raise exceptions.FrictionlessException(Error(note=note))
        return server

    # Plugins

    def load_plugins(self):
        modules = OrderedDict()
        # External
        for item in pkgutil.iter_modules():
            if item.name.startswith('frictionless-'):
                module = import_module(item.name)
                modules[item.name] = module
        # Internal
        for _, name, _ in pkgutil.iter_modules([os.path.dirname(plugins.__file__)]):
            module = import_module(f'frictionless.plugins.{name}')
            modules[name] = module
        # Plugins
        for name, module in modules.items():
            Plugin = getattr(module, f'{name.capitalize()}Plugin', None)
            if Plugin:
                plugin = Plugin()
                self.__plugins[name] = plugin
                for code in self.__actions.keys():
                    if code in vars(Plugin):
                        func = getattr(plugin, code, None)
                        self.__actions[code][name] = func


system = System()
system.load_plugins()
