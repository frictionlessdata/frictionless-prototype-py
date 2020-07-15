import requests
from .metadata import Metadata
from . import helpers
from . import errors
from . import config


class Control(Metadata):
    """Control representation

    # Arguments
        descriptor? (str|dict): descriptor
        detectEncoding? (func): detectEncoding

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = errors.ControlError
    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {"detectEncoding": {}},
    }

    def __init__(self, descriptor=None, *, detect_encoding=None):
        self.setinitial("detectEncoding", detect_encoding)
        super().__init__(descriptor)

    @Metadata.property
    def detect_encoding(self):
        return self.get("detectEncoding", helpers.detect_encoding)

    # Expand

    def expand(self):
        pass

    # Import/Export

    def to_dict(self, expand=False):
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result


class LocalControl(Control):
    """Local control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {"detectEncoding": {}},
    }


class RemoteControl(Control):
    """Remote control representation

    # Arguments
        descriptor? (str|dict): descriptor
        http_session? (any): http_session
        http_preload? (bool): http_preload
        http_timeout? (int): http_timeout
        detectEncoding? (func): detectEncoding

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "httpSession": {},
            "httpPreload": {"type": "boolean"},
            "httpTimeout": {"type": "number"},
            "detectEncoding": {},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        http_session=None,
        http_preload=None,
        http_timeout=None,
        detect_encoding=None,
    ):
        self.setinitial("httpSession", http_session)
        self.setinitial("httpPreload", http_preload)
        self.setinitial("httpTimeout", http_timeout)
        super().__init__(descriptor, detect_encoding=detect_encoding)

    @Metadata.property
    def http_session(self):
        http_session = self.get("httpSession")
        if not http_session:
            http_session = requests.Session()
            http_session.headers.update(config.DEFAULT_HTTP_HEADERS)
        return http_session

    @Metadata.property
    def http_preload(self):
        return self.get("httpPreload", False)

    @Metadata.property
    def http_timeout(self):
        return self.get("httpTimeout", config.DEFAULT_HTTP_TIMEOUT)

    # Expand

    def expand(self):
        super().expand()
        self.setdefault("httpPreload", self.http_preload)
        self.setdefault("httpTimeout", self.http_timeout)


class StreamControl(Control):
    """Stream control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {"detectEncoding": {}},
    }


class TextControl(Control):
    """Text control representation

    # Arguments
        descriptor? (str|dict): descriptor

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {"detectEncoding": {}},
    }
