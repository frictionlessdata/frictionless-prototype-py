from ..loader import Loader
from .. import exceptions
from .. import controls
from .. import errors


class StreamLoader(Loader):
    Control = controls.LocalControl

    # Read

    def read_byte_stream_create(self):
        source = self.file.source
        if hasattr(source, "encoding"):
            error = errors.SchemeError(note="only byte streams are supported")
            raise exceptions.FrictionlessException(error)
        return source
