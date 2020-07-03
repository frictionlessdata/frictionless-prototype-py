import io
import json
import requests
import jsonschema
from copy import deepcopy
from urllib.parse import urlparse
from importlib import import_module
from .helpers import cached_property
from . import exceptions
from . import helpers
from . import config


class Metadata(dict):
    """Metadata representation

    # Arguments
        descriptor? (str|dict): schema descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = None
    metadata_profile = None
    metadata_strict = False

    def __init__(self, descriptor=None):
        self.__errors = []
        self.__Error = self.metadata_Error or import_module("frictionless.errors").Error
        metadata = self.metadata_extract(descriptor)
        for key, value in metadata.items():
            self.setdefault(key, value)

    @cached_property
    def metadata_valid(self):
        return not len(self.__errors)

    @cached_property
    def metadata_errors(self):
        return self.__errors

    def setnotnull(self, key, value):
        if value is not None:
            self[key] = value

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

    # Validate

    def metadata_validate(self):
        self.metadata_errors.clear()
        if self.metadata_profile:
            validator_class = jsonschema.validators.validator_for(self.metadata_profile)
            validator = validator_class(self.metadata_profile)
            for error in validator.iter_errors(self):
                metadata_path = "/".join(map(str, error.path))
                profile_path = "/".join(map(str, error.schema_path))
                note = '"%s" at "%s" in metadata and at "%s" in profile'
                note = note % (error.message, metadata_path, profile_path)
                error = self.__Error(note=note)
                if self.metadata_strict:
                    raise exceptions.FrictionlessException(error)
                self.metadata_errors.append(error)

    # Write

    def metadata_write(self, target, ensure_ascii=True):
        try:
            helpers.ensure_dir(target)
            with io.open(target, mode="w", encoding="utf-8") as file:
                json.dump(self, file, indent=2, ensure_ascii=ensure_ascii)
        except Exception as exc:
            raise exceptions.FrictionlessException(self.__Error(note=str(exc))) from exc
