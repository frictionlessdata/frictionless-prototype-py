import io
import csv
import unicodecsv
from itertools import chain
from ..parser import Parser
from .. import dialects
from .. import helpers
from .. import config


class CsvParser(Parser):
    Dialect = dialects.CsvDialect
    newline = ''

    # Read

    def read_data_stream_create(self):
        sample = self.read_data_stream_infer_dialect()
        source = chain(sample, self.loader.text_stream)
        data = csv.reader(source, dialect=self.file.dialect)
        yield from data

    def read_data_stream_infer_dialect(self):
        sample = extract_samle(self.loader.text_stream)
        delimiter = self.file.dialect.get('delimiter', ',\t;|')
        try:
            dialect = csv.Sniffer().sniff(''.join(sample), delimiter)
        except csv.Error:
            dialect = csv.excel()
        if not dialect.escapechar:
            dialect.doublequote = True
        if getattr(dialect, 'quotechar', None) == '':
            setattr(dialect, 'quoting', csv.QUOTE_NONE)
        for name in INFER_NAMES:
            value = getattr(dialect, name.lower())
            if value is not None:
                self.file.dialect.setdefault(name, value)
        return sample

    # Write

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


# Internal

INFER_NAMES = [
    'delimiter',
    'lineTerminator',
    'doubleQuote',
    'escapeChar',
    'quoteChar',
    'skipInitialSpace',
]


def extract_samle(text_stream):
    sample = []
    while True:
        try:
            sample.append(next(text_stream))
        except StopIteration:
            break
        if len(sample) >= config.INFER_DIALECT_VOLUME:
            break
    return sample
