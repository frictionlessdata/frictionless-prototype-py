import io
import json
import requests
import jsonschema
from copy import deepcopy
from operator import setitem
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
        self.metadata_validate()

    def __setattr__(self, name, value):
        setter = self.metadata_setters.get(name)
        if isinstance(setter, str):
            return setitem(self, setter, value)
        elif callable(setter):
            return callable(self, value)
        return super().__setattr__(name, value)

    def __onchange__(self):
        super().__onchange__()
        self.metadata_process()

    @cached_property
    def metadata_valid(self):
        return not len(self.metadata_errors)

    @cached_property
    def metadata_errors(self):
        return list(self.metadata_validate())

    def setnotnull(self, key, value):
        if value is not None:
            self[key] = value

    # Attach

    def metadata_attach(self, name, value):
        if self.get(name) != value:
            if isinstance(value, dict):
                value = deepcopy(value)
                onchange = lambda: setitem(self, name, dict(value))
                value = helpers.ControlledDict(value, onchange=onchange)
            elif isinstance(value, list):
                value = deepcopy(value)
                onchange = lambda: setitem(self, name, list(value))
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
                error = self.__Error(note=note)
                if not self.metadata_relaxed:
                    raise exceptions.FrictionlessException(error)
                yield error

    # Write

    def metadata_write(self, target, ensure_ascii=True):
        try:
            helpers.ensure_dir(target)
            with io.open(target, mode="w", encoding="utf-8") as file:
                json.dump(self, file, indent=2, ensure_ascii=ensure_ascii)
        except Exception as exc:
            raise exceptions.FrictionlessException(self.__Error(note=str(exc))) from exc
