from .metadata import Metadata
from . import config


class Inquiry(Metadata):
    """Inquiry representation.

    # Arguments
        descriptor? (str|dict): schema descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_strict = True
    metadata_profile = config.INQUIRY_PROFILE

    def __init__(self, descriptor):
        super().__init__(descriptor)

    @property
    def tasks(self):
        return self["tasks"]

    # Save

    def save(self, target):
        self.metadata_save(target)
