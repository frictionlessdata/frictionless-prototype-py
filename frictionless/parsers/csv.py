import io
import csv
import unicodecsv
from itertools import chain
from ..parser import Parser
from .. import dialects
from .. import helpers


class CsvParser(Parser):
    Dialect = dialects.CsvDialect
    newline = ""

    # Read

    def read_data_stream_create(self):
        sample = self.read_data_stream_infer_dialect()
        source = chain(sample, self.loader.text_stream)
        data = csv.reader(source, dialect=self.file.dialect)
        yield from data

    def read_data_stream_infer_dialect(self):
        sample = extract_samle(self.loader.text_stream)
        delimiter = self.file.dialect.get("delimiter", ",\t;|")
        try:
            dialect = csv.Sniffer().sniff("".join(sample), delimiter)
        except csv.Error:
            dialect = csv.excel()
        if not dialect.escapechar:
            dialect.doublequote = True
        if getattr(dialect, "quotechar", None) == "":
            setattr(dialect, "quoting", csv.QUOTE_NONE)
        for name in INFER_DIALECT_NAMES:
            value = getattr(dialect, name.lower())
            if value is not None:
                self.file.dialect.setdefault(name, value)
        return sample

    # Write

    def write(self, data_stream):
        options = {}
        dialect = self.file.dialect
        for name in INFER_DIALECT_NAMES + ["quoting"]:
            name = name.lower()
            value = getattr(dialect, name, None)
            if value is not None:
                options[name] = value
        helpers.ensure_dir(self.file.source)
        with io.open(self.file.source, "wb") as file:
            writer = unicodecsv.writer(file, encoding=self.file.encoding, **options)
            for cells in data_stream:
                writer.writerow(cells)


# Internal

INFER_DIALECT_VOLUME = 100
INFER_DIALECT_NAMES = [
    "delimiter",
    "lineTerminator",
    "doubleQuote",
    "escapeChar",
    "quoteChar",
    "skipInitialSpace",
]


def extract_samle(text_stream):
    sample = []
    while True:
        try:
            sample.append(next(text_stream))
        except StopIteration:
            break
        if len(sample) >= INFER_DIALECT_VOLUME:
            break
    return sample
