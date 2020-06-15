from .metadata import ControlledMetadata
from . import config


class Schema(ControlledMetadata):
    metadata_profile = config.SCHEMA_PROFILE
