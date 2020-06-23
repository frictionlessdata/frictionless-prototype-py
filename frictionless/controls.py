import requests
from .metadata import Metadata
from . import config


class Control(Metadata):
    """Control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    def __init__(self, descriptor=None):
        super().__init__(descriptor)

    # Expand

    def expand(self):
        pass


# TODO: move to plugins


class LocalControl(Control):
    """Local control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'additionalProperties': False,
        'properties': {},
    }


class RemoteControl(Control):
    """Remote control representation

    # Arguments
        descriptor? (str|dict): descriptor
        http_session? (any): http_session
        http_preload? (bool): http_preload
        http_timeout? (int): http_timeout

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {
            'httpSession': {},
            'httpPreload': {'type': 'boolean'},
            'httpTimeout': {'type': 'number'},
        },
    }

    def __init__(
        self, descriptor=None, http_session=None, http_preload=None, http_timeout=None,
    ):
        self.setdefined('httpSession', http_session)
        self.setdefined('httpPreload', http_preload)
        self.setdefined('httpTimeout', http_timeout)
        super().__init(descriptor)

    @property
    def http_session(self):
        http_session = self.get('httpSession')
        if not http_session:
            http_session = requests.Session()
            http_session.headers.update(config.HTTP_HEADERS)
        return http_session

    @property
    def http_preload(self):
        return self.get('httpPreload', False)

    @property
    def http_timeout(self):
        return self.get('httpTimeout', config.HTTP_TIMEOUT)

    # Expand

    def expand(self):
        self.setdetault('httpPreload', self.http_preload)
        self.setdetault('httpTimeout', self.http_timeout)


class StreamControl(Control):
    """Stream control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'additionalProperties': False,
        'properties': {},
    }


class TextControl(Metadata):
    """Text control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'additionalProperties': False,
        'properties': {},
    }
