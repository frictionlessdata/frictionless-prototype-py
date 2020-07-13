import io
from ..loader import Loader


class LocalLoader(Loader):

    # Read

    def read_byte_stream_create(self):
        scheme = "file://"
        source = self.file.source
        if source.startswith(scheme):
            source = source.replace(scheme, "", 1)
        byte_stream = io.open(source, "rb")
        return byte_stream
