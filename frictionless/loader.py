class Loader:
    """Abstract class implemented by the data loaders

    The loaders inherit and implement this class' methods to add support for a
    new scheme (e.g. ssh).

    # Arguments
        **options (dict): Loader options

    """

    # Public

    options = []  # type: ignore

    def __init__(self, **options):
        pass

    def load(self, source, mode='t', encoding=None):
        """Load source file.

        # Arguments
            source (str): Path to tabular source file.
            mode (str, optional):
                Text stream mode, `t` (text) or `b` (binary).  Defaults to `t`.
            encoding (str, optional):
                Source encoding. Auto-detect by default.

        # Returns
            Union[TextIO, BinaryIO]: I/O stream opened either as text or binary.

        """
        raise NotImplementedError
