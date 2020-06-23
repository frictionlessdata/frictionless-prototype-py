import io
from ..loader import Loader
from .. import exceptions
from .. import helpers


class LocalLoader(Loader):
    options = []  # type: ignore

    def __init__(self):
        self.__stats = None

    def attach_stats(self, stats):
        self.__stats = stats

    def load(self, source, mode='t', encoding=None):

        # Prepare source
        scheme = 'file://'
        if source.startswith(scheme):
            source = source.replace(scheme, '', 1)

        # Prepare bytes
        try:
            bytes = io.open(source, 'rb')
            if self.__stats:
                bytes = helpers.BytesStatsWrapper(bytes, self.__stats)
        except IOError as exception:
            raise exceptions.LoadingError(str(exception))

        # Return bytes
        if mode == 'b':
            return bytes

        # Detect encoding
        # TODO: rebase on infer_volume/sampling
        if True:
            sample = bytes.read(10000)
            bytes.seek(0)
            encoding = helpers.detect_encoding(sample, encoding)

        # Prepare chars
        chars = io.TextIOWrapper(bytes, encoding)

        return chars
