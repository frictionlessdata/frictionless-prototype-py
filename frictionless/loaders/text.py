import io
from ..loader import Loader
from .. import helpers
from .. import config


class TextLoader(Loader):
    options = []  # type: ignore

    def __init__(self):
        self.__stats = None

    def attach_stats(self, stats):
        self.__stats = stats

    def load(self, source, mode='t', encoding=None):

        # Prepare source
        scheme = 'text://'
        if source.startswith(scheme):
            source = source.replace(scheme, '', 1)

        # Prepare bytes
        bytes = io.BufferedRandom(io.BytesIO())
        bytes.write(source.encode(encoding or config.DEFAULT_ENCODING))
        bytes.seek(0)
        if self.__stats:
            bytes = helpers.BytesStatsWrapper(bytes, self.__stats)

        # Return bytes
        if mode == 'b':
            return bytes

        # Prepare chars
        chars = io.TextIOWrapper(bytes, encoding)

        return chars
