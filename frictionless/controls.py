from .metadata import Metadata


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


class LocalControl(Metadata):
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


class RemoteControl(Metadata):
    """Remote control representation

    # Arguments
        descriptor? (str|dict): descriptor
        http_session? (any): http_session
        http_stream? (bool): http_stream
        http_timeout? (int): http_timeout

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {
            'httpSession': {},
            'httpStream': {'type': 'boolean'},
            'httpTimeout': {'type': 'number'},
        },
    }

    def __init__(
        self, descriptor=None, http_session=None, http_stream=None, http_timeout=None,
    ):
        self.setdefined('httpSession', http_session)
        self.setdefined('httpStream', http_stream)
        self.setdefined('httpTimeout', http_timeout)
        super().__init(descriptor)

    # Expand

    def expand(self):
        self.setdetault('httpStream', True)


class StreamControl(Metadata):
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
