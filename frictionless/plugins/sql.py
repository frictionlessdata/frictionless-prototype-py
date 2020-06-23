from sqlalchemy import create_engine, sql, MetaData, Table, Column, String
from ..plugin import Plugin
from ..parser import Parser
from .. import exceptions


# Plugin


class SqlPlugin(Plugin):
    def create_parser(self, source, *, dialect=None):
        pass


# Parsers


class SqlParser(Parser):
    options = [
        'table',
        'order_by',
    ]

    def __init__(self, loader, table=None, order_by=None):

        # Ensure table
        if table is None:
            raise exceptions.TabulatorException('Format `sql` requires `table` option.')

        # Set attributes
        self.__loader = loader
        self.__table = table
        self.__order_by = order_by
        self.__engine = None
        self.__extended_rows = None
        self.__encoding = None

    @property
    def closed(self):
        return self.__engine is None

    def open(self, source, encoding=None):
        self.close()
        self.__engine = create_engine(source)
        self.__engine.update_execution_options(stream_results=True)
        self.__encoding = encoding
        self.reset()

    def close(self):
        if not self.closed:
            self.__engine.dispose()
            self.__engine = None

    def reset(self):
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        table = sql.table(self.__table)
        order = sql.text(self.__order_by) if self.__order_by else None
        query = sql.select(['*']).select_from(table).order_by(order)
        result = self.__engine.execute(query)
        for row_number, row in enumerate(iter(result), start=1):
            yield (row_number, row.keys(), list(row))


class SqlWriter:
    options = [
        'table',
    ]

    def __init__(self, table=None, **options):

        # Ensure table
        if table is None:
            raise exceptions.TabulatorException('Format `sql` requires `table` option.')

        self.__table = table

    def write(self, source, target, headers, encoding=None):
        engine = create_engine(target)
        count = 0
        buffer = []
        buffer_size = 1000
        with engine.begin() as conn:
            meta = MetaData()
            columns = [Column(header, String()) for header in headers]
            table = Table(self.__table, meta, *columns)
            meta.create_all(conn)
            for row in source:
                count += 1
                buffer.append(row)
                if len(buffer) > buffer_size:
                    conn.execute(table.insert().values(buffer))
                    buffer = []
            if len(buffer):
                conn.execute(table.insert().values(buffer))
        return count
