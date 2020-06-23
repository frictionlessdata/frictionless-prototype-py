class Parser:
    """Abstract class implemented by the data parsers.

    The parsers inherit and implement this class' methods to add support for a
    new file type.

    # Arguments
        loader (tabulator.Loader): Loader instance to read the file.
        **options (dict): Loader options

    """

    # Public

    options = []  # type: ignore

    def __init__(self, loader, **options):
        pass

    @property
    def closed(self):
        """Flag telling if the parser is closed.

        # Returns
            bool: whether closed

        """
        raise NotImplementedError

    def open(self, source, encoding=None):
        """Open underlying file stream in the beginning of the file.

        The parser gets a byte or text stream from the `tabulator.Loader`
        instance and start emitting items.

        # Arguments
            source (str): Path to source table.
            encoding (str, optional): Source encoding. Auto-detect by default.

        # Returns
            None

        """
        raise NotImplementedError

    def close(self):
        """Closes underlying file stream.
        """
        raise NotImplementedError

    def reset(self):
        """Resets underlying stream and current items list.

        After `reset()` is called, iterating over the items will start from the beginning.
        """
        raise NotImplementedError

    @property
    def encoding(self):
        """Encoding

        # Returns
            str: encoding

        """
        raise NotImplementedError

    @property
    def extended_rows(self):
        """Returns extended rows iterator.

        The extended rows are tuples containing `(row_number, headers, row)`,

        # Raises
            SourceError:
                If a row can't be parsed, this exception will be raised.

        Returns:
            Iterator[Tuple[int, List[str], List[Any]]]:
                Extended rows containing
                `(row_number, headers, row)`, where `headers` is a list of the
                header names (can be `None`), and `row` is a list of row
                values.

        """
        raise NotImplementedError
