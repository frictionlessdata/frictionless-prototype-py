import io
import json
import requests
import jsonschema
from copy import deepcopy
from operator import setitem
from functools import partial
from urllib.parse import urlparse
from .helpers import cached_property
from . import exceptions
from . import helpers
from . import config


class Metadata(dict):
    """Metadata representation

    # Arguments
        descriptor? (str|dict): schema descriptor
        metadata_root? (Metadata): root metadata object
        metadata_strict? (bool): if True it will fail on the first metadata error

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = None
    metadata_profile = None

    def __init__(self, descriptor=None, *, metadata_root=None, metadata_strict=False):
        self.__errors = []
        self.__root = metadata_root or self
        self.__raise = metadata_strict or not self.metadata_Error
        metadata = self.metadata_extract(descriptor)
        for key, value in metadata.items():
            dict.setdefault(self, key, value)
        if not metadata_root:
            self.metadata_process()
            self.metadata_validate()

    @cached_property
    def metadata_root(self):
        return self.__root

    @cached_property
    def metadata_strict(self):
        return self.__raise

    @cached_property
    def metadata_valid(self):
        return not len(self.__errors)

    @cached_property
    def metadata_errors(self):
        return self.__errors

    # Duplicate

    def metadata_duplicate(self):
        result = {}
        for key, value in self.items():
            if hasattr(value, 'metadata_duplicate'):
                value = value.copy()
            result[key] = value
        return result

    def copy(self):
        return self.metadata_duplicate()

    def __copy__(self, *args, **kwargs):
        return self.copy()

    def __deepcopy__(self, *args, **kwargs):
        return self.copy()

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
                with io.open(descriptor, encoding='utf-8') as file:
                    return json.load(file)
            return json.load(descriptor)
        except Exception as exception:
            message = 'canot extract metadata "%s" because "%s"' % (descriptor, exception)
            raise exceptions.FrictionlessException(message) from exception

    # Process

    def metadata_process(self):
        pass

    # Save

    def metadata_save(self, target, ensure_ascii=True):
        try:
            helpers.ensure_dir(target)
            with io.open(target, mode='w', encoding='utf-8') as file:
                json.dump(self, file, indent=2, ensure_ascii=ensure_ascii)
        except Exception as exception:
            raise exceptions.FrictionlessException(str(exception)) from exception

    # Transform

    def metadata_transform(self):
        pass

    def setdefined(self, key, value):
        if value is not None:
            self[key] = value

    # Validate

    def metadata_validate(self):
        self.metadata_errors.clear()
        if self.metadata_profile:
            validator_class = jsonschema.validators.validator_for(self.metadata_profile)
            validator = validator_class(self.metadata_profile)
            for error in validator.iter_errors(self):
                metadata_path = '/'.join(map(str, error.path))
                profile_path = '/'.join(map(str, error.schema_path))
                note = '"%s" at "%s" in metadata and at "%s" in profile'
                note = note % (error.message, metadata_path, profile_path)
                if self.metadata_strict:
                    raise exceptions.FrictionlessException(note)
                error = self.metadata_Error(note=note)
                self.metadata_errors.append(error)


class ControlledMetadata(Metadata):
    """Metadata representation (controlled)

    # Arguments
        descriptor? (str|dict): schema descriptor
        metadata_root? (Metadata): root metadata object
        metadata_strict? (bool): if True it will fail on the first metadata error
        metadata_attach? (bool): callback for the attach mechanism

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    def __init__(
        self,
        descriptor=None,
        *,
        metadata_root=None,
        metadata_strict=False,
        metadata_attach=None
    ):
        self.__attach = metadata_attach
        super().__init__(
            descriptor, metadata_root=metadata_root, metadata_strict=metadata_strict
        )

    # Attach

    def metadata_attach(self, name, value):
        if self.get(name) != value:
            value = deepcopy(value)
            attach = partial(metadata_attach, self, name, value)
            if isinstance(value, dict):
                value = ControlledMetadata(value, metadata_attach=attach)
            if isinstance(value, list):
                value = ControlledMetadataList(value, metadata_attach=attach)
        return value

    # Extract

    def metadata_extract(self, descriptor, *, duplicate=False):
        return super().metadata_extract(descriptor, duplicate=True)

    # Process

    def metadata_process(self):
        helpers.reset_cached_properties(self)
        for key, value in self.items():
            if isinstance(value, dict):
                if getattr(value, 'metadata_root', None) != self.metadata_root:
                    value = ControlledMetadata(
                        value,
                        metadata_root=self.metadata_root,
                        metadata_strict=self.metadata_strict,
                    )
                    dict.__setitem__(self, key, value)
                value.metadata_process()
            if isinstance(value, list):
                if getattr(value, 'metadata_root', None) != self.metadata_root:
                    value = ControlledMetadataList(
                        value,
                        metadata_root=self.metadata_root,
                        metadata_strict=self.metadata_strict,
                    )
                    dict.__setitem__(self, key, value)
                value.metadata_process()

    # Transform

    def metadata_transform(self):
        try:
            # It can be not initiated
            root = self.metadata_root
        except AttributeError:
            return
        if self.__attach:
            attach = self.__attach
            self.__attach = None
            attach()
        if root is not self:
            return self.metadata_root.metadata_transform()
        self.metadata_process()
        self.metadata_validate()

    def __setitem__(self, *args, **kwargs):
        result = super().__setitem__(*args, **kwargs)
        self.metadata_transform()
        return result

    def __delitem__(self, *args, **kwargs):
        result = super().__delitem__(*args, **kwargs)
        self.metadata_transform()
        return result

    def clear(self, *args, **kwargs):
        result = super().clear(*args, **kwargs)
        self.metadata_transform()
        return result

    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        self.metadata_transform()
        return result

    def popitem(self, *args, **kwargs):
        result = super().popitem(*args, **kwargs)
        self.metadata_transform()
        return result

    def setdefault(self, *args, **kwargs):
        result = super().setdefault(*args, **kwargs)
        self.metadata_transform()
        return result

    def setdefined(self, *args, **kwargs):
        result = super().setdefined(*args, **kwargs)
        self.metadata_transform()
        return result

    def update(self, *args, **kwargs):
        result = super().update(*args, **kwargs)
        self.metadata_transform()
        return result

    # Validate

    def metadata_validate(self):
        super().metadata_validate()
        for key, value in self.items():
            if hasattr(value, 'metadata_validate'):
                value.metadata_validate()
                self.metadata_errors.extend(value.metadata_errors)


class ControlledMetadataList(list):
    """Metadata representation (controlled list)

    # Arguments
        values (str|dict): values
        metadata_root? (Metadata): root metadata object
        metadata_strict? (bool): if True it will fail on the first metadata error
        metadata_attach? (bool): callback for the attach mechanism

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    def __init__(
        self, values, *, metadata_root=None, metadata_strict=False, metadata_attach=None
    ):
        list.extend(self, values)
        self.__errors = []
        self.__root = metadata_root or self
        self.__raise = metadata_strict
        self.__attach = metadata_attach

    @cached_property
    def metadata_root(self):
        return self.__root

    @cached_property
    def metadata_strict(self):
        return self.__raise

    @cached_property
    def metadata_errors(self):
        return self.__errors

    # Duplicate

    def metadata_duplicate(self):
        result = []
        for value in self:
            if hasattr(value, 'metadata_duplicate'):
                value = value.copy()
            result.append(value)
        return result

    def copy(self):
        return self.metadata_duplicate()

    def __copy__(self, *args, **kwargs):
        return self.copy()

    def __deepcopy__(self, *args, **kwargs):
        return self.copy()

    # Process

    def metadata_process(self):
        for index, value in list(enumerate(self)):
            if isinstance(value, dict):
                if getattr(value, 'metadata_root', None) != self.metadata_root:
                    value = ControlledMetadata(
                        value,
                        metadata_root=self.metadata_root,
                        metadata_strict=self.metadata_strict,
                    )
                    list.__setitem__(self, index, value)
                value.metadata_process()
            if isinstance(value, list):
                if getattr(value, 'metadata_root', None) != self.metadata_root:
                    value = ControlledMetadataList(
                        value,
                        metadata_root=self.metadata_root,
                        metadata_strict=self.metadata_strict,
                    )
                    list.__setitem__(self, index, value)
                value.metadata_process()

    # Transform

    def metadata_transform(self):
        try:
            # It can be not initiated
            root = self.metadata_root
        except AttributeError:
            return
        if self.__attach:
            attach = self.__attach
            self.__attach = None
            attach()
        if root is not self:
            self.metadata_root.metadata_transform()

    def __setitem__(self, *args, **kwargs):
        result = super().__setitem__(*args, **kwargs)
        self.metadata_transform()
        return result

    def __delitem__(self, *args, **kwargs):
        result = super().__delitem__(*args, **kwargs)
        self.metadata_transform()
        return result

    def append(self, *args, **kwargs):
        result = super().append(*args, **kwargs)
        self.metadata_transform()
        return result

    def clear(self, *args, **kwargs):
        result = super().clear(*args, **kwargs)
        self.metadata_transform()
        return result

    def extend(self, *args, **kwargs):
        result = super().extend(*args, **kwargs)
        self.metadata_transform()
        return result

    def insert(self, *args, **kwargs):
        result = super().insert(*args, **kwargs)
        self.metadata_transform()
        return result

    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        self.metadata_transform()
        return result

    def remove(self, *args, **kwargs):
        result = super().remove(*args, **kwargs)
        self.metadata_transform()
        return result

    # Validate

    def metadata_validate(self):
        for value in self:
            if hasattr(value, 'metadata_validate'):
                value.metadata_validate()
                self.metadata_errors.extend(value.metadata_errors)


# Internal


def metadata_attach(self, name, value):
    setitem(self, name, value)
