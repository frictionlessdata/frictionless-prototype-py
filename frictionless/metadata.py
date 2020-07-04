import io
import json
import requests
import jsonschema
from copy import deepcopy
from operator import setitem
from functools import partial
from urllib.parse import urlparse
from importlib import import_module
from .helpers import cached_property
from . import exceptions
from . import helpers
from . import config


class Metadata(helpers.ControlledDict):
    """Metadata representation

    # Arguments
        descriptor? (str|dict): schema descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = None
    metadata_profile = None
    metadata_relaxed = False
    metadata_setters = {}

    def __init__(self, descriptor=None):
        self.__Error = self.metadata_Error or import_module("frictionless.errors").Error
        metadata = self.metadata_extract(descriptor)
        for key, value in metadata.items():
            dict.setdefault(self, key, value)
        self.metadata_process()
        if not self.metadata_relaxed:
            for error in self.metadata_errors:
                raise exceptions.FrictionlessException(error)

    def __setattr__(self, name, value):
        setter = self.metadata_setters.get(name)
        if isinstance(setter, str):
            return setitem(self, setter, value)
        elif callable(setter):
            return callable(self, value)
        return super().__setattr__(name, value)

    def __onchange__(self):
        helpers.reset_cached_properties(self)
        self.metadata_process()

    @cached_property
    def metadata_valid(self):
        return not len(self.metadata_errors)

    @cached_property
    def metadata_errors(self):
        return list(self.metadata_validate())

    def setinitial(self, key, value):
        if value is not None:
            dict.__setitem__(self, key, value)

    # Import/Export

    def to_dict(self):
        return self.copy()

    # Attach

    def metadata_attach(self, name, value):
        if self.get(name) != value:
            value = deepcopy(value)
            onchange = partial(metadata_attach, self, name)
            if isinstance(value, dict):
                value = helpers.ControlledDict(value, onchange=onchange)
            elif isinstance(value, list):
                value = helpers.ControlledList(value, onchange=onchange)
        return value

    # Extract

    def metadata_extract(self, descriptor, *, duplicate=False):
        try:
            if descriptor is None:
                return {}
            if isinstance(descriptor, dict):
                return deepcopy(descriptor) if duplicate else descriptor
            if isinstance(descriptor, str):
                if urlparse(descriptor).scheme in config.REMOTE_SCHEMES:
                    return requests.get(descriptor).json()
                with io.open(descriptor, encoding="utf-8") as file:
                    return json.load(file)
            return json.load(descriptor)
        except Exception as exception:
            note = f'canot extract metadata "{descriptor}" because "{exception}"'
            raise exceptions.FrictionlessException(self.__Error(note=note)) from exception

    # Process

    def metadata_process(self):
        pass

    # Validate

    def metadata_validate(self):
        if self.metadata_profile:
            validator_class = jsonschema.validators.validator_for(self.metadata_profile)
            validator = validator_class(self.metadata_profile)
            for error in validator.iter_errors(self):
                metadata_path = "/".join(map(str, error.path))
                profile_path = "/".join(map(str, error.schema_path))
                note = '"%s" at "%s" in metadata and at "%s" in profile'
                note = note % (error.message, metadata_path, profile_path)
                yield self.__Error(note=note)
        yield from []

    # Save

    def metadata_save(self, target, ensure_ascii=True):
        try:
            helpers.ensure_dir(target)
            with io.open(target, mode="w", encoding="utf-8") as file:
                json.dump(self, file, indent=2, ensure_ascii=ensure_ascii)
        except Exception as exc:
            raise exceptions.FrictionlessException(self.__Error(note=str(exc))) from exc


# Internal


def metadata_attach(self, name, value):
    copy = dict if isinstance(value, dict) else list
    setitem(self, name, copy(value))
