import io
import csv
import unicodecsv
from itertools import chain
from ..plugin import Plugin
from ..parser import Parser
from ..system import system
from .. import helpers
from .. import config


# Plugin


class CsvPlugin(Plugin):
    def create_parser(self, source, *, dialect=None):
        pass


# Parsers


class CsvParser(Parser):
    options = [
        'delimiter',
        'doublequote',
        'escapechar',
        'quotechar',
        'quoting',
        'skipinitialspace',
        'lineterminator',
    ]

    def __init__(self, loader, **options):
        self.__loader = loader
        self.__options = options
        self.__extended_rows = None
        self.__encoding = None
        self.__dialect = None
        self.__text_stream = None

    @property
    def closed(self):
        return self.__text_stream is None or self.__text_stream.closed

    def open(self, source, encoding=None):
        self.close()
        self.__text_stream = self.__loader.read_text_stream()
        self.__encoding = getattr(self.__text_stream, 'encoding', encoding)
        if self.__encoding:
            self.__encoding.lower()
        self.reset()

    def close(self):
        if not self.closed:
            self.__text_stream.close()

    def reset(self):
        helpers.reset_stream(self.__text_stream)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def dialect(self):
        if self.__dialect:
            dialect = {
                'delimiter': self.__dialect.delimiter,
                'doubleQuote': self.__dialect.doublequote,
                'lineTerminator': self.__dialect.lineterminator,
                'quoteChar': self.__dialect.quotechar,
                'skipInitialSpace': self.__dialect.skipinitialspace,
            }
            if self.__dialect.escapechar is not None:
                dialect['escapeChar'] = self.__dialect.escapechar
            return dialect

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        sample, dialect = self.__prepare_dialect(self.__text_stream)
        items = csv.reader(chain(sample, self.__text_stream), dialect=dialect)
        for row_number, item in enumerate(items, start=1):
            yield (row_number, None, list(item))

    def __prepare_dialect(self, stream):

        # Get sample
        sample = []
        while True:
            try:
                sample.append(next(stream))
            except StopIteration:
                break
            if len(sample) >= config.CSV_SAMPLE_LINES:
                break

        # Get dialect
        try:
            separator = ''
            delimiter = self.__options.get('delimiter', ',\t;|')
            dialect = csv.Sniffer().sniff(separator.join(sample), delimiter)
            if not dialect.escapechar:
                dialect.doublequote = True
        except csv.Error:

            class dialect(csv.excel):
                pass

        for key, value in self.__options.items():
            setattr(dialect, key, value)
        # https://github.com/frictionlessdata/FrictionlessDarwinCore/issues/1
        if getattr(dialect, 'quotechar', None) == '':
            setattr(dialect, 'quoting', csv.QUOTE_NONE)

        self.__dialect = dialect
        return sample, dialect


class CsvWriter:
    options = [
        'delimiter',
    ]

    def __init__(self, **options):
        self.__options = options

    def write(self, source, target, headers, encoding=None):
        helpers.ensure_dir(target)
        count = 0
        with io.open(target, 'wb') as file:
            writer = unicodecsv.writer(file, encoding=encoding, **self.__options)
            if headers:
                writer.writerow(headers)
            for row in source:
                count += 1
                writer.writerow(row)
        return count
