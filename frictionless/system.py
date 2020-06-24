import os
import pkgutil
from collections import OrderedDict
from importlib import import_module
from .errors import Error
from . import exceptions
from . import plugins
from . import loaders
from . import parsers
from . import checks
from . import config


class System:
    """System representation
    """

    def __init__(self):
        self.__plugins = OrderedDict()
        self.__actions = {
            'create_check': OrderedDict(),
            'create_loader': OrderedDict(),
            'create_parser': OrderedDict(),
            'create_server': OrderedDict(),
        }

    # Create

    def create_check(self, name, *, descriptor=None):
        check = None
        for func in self.__actions['create_check'].values():
            check = func(name, descriptor=descriptor)
            if check is not None:
                break
        if check is None:
            if name == 'baseline':
                return checks.BaselineCheck(descriptor)
            elif name == 'integrity':
                return checks.IntegrityCheck(descriptor)
            elif name == 'duplicate-row':
                return checks.DuplicateRowCheck(descriptor)
            elif name == 'deviated-value':
                return checks.DeviatedValueCheck(descriptor)
            elif name == 'truncated-value':
                return checks.TruncatedValueCheck(descriptor)
            elif name == 'blacklisted-value':
                return checks.BlacklistedValueCheck(descriptor)
            elif name == 'sequential-value':
                return checks.SequentialValueCheck(descriptor)
            elif name == 'row-constraint':
                return checks.RowConstraintCheck(descriptor)
        if check is None:
            note = f'Cannot create check "{name}". Try installing "frictionless-{name}"'
            raise exceptions.FrictionlessException(Error(note=note))
        return check

    def create_loader(self, location, *, control=None):
        loader = None
        name = location.scheme
        for func in self.__actions['create_loader'].values():
            loader = func(location, control=control)
            if loader is not None:
                break
        if loader is None:
            if name == 'file':
                return loaders.LocalLoader(location, control=control)
            elif name in config.REMOTE_SCHEMES:
                return loaders.RemoteLoader(location, control=control)
            elif name == 'stream':
                return loaders.StreamLoader(location, control=control)
            elif name == 'text':
                return loaders.TextLoader(location, control=control)
        if loader is None:
            note = f'Cannot create loader "{name}". Try installing "frictionless-{name}"'
            raise exceptions.FrictionlessException(Error(note=note))
        return loader

    def create_parser(self, location, *, dialect=None):
        parser = None
        name = location.format
        for func in self.__actions['create_parser'].values():
            parser = func(location, dialect=dialect)
            if parser is not None:
                break
        if parser is None:
            if name == 'inline':
                return parsers.InlineParser(location, dialect=dialect)
        if parser is None:
            note = f'Cannot create parser "{name}". Try installing "frictionless-{name}"'
            raise exceptions.FrictionlessException(Error(note=note))
        return parser

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
            if item.name.startswith('frictionless_'):
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
