from sqlalchemy import create_engine, sql, MetaData, Table, Column, String
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser
from .. import exceptions


# Plugin


class SqlPlugin(Plugin):
    def create_parser(self, file):
        if file.scheme in SQL_SCHEMES:
            return SqlParser(file)


# Parser


class SqlParser(Parser):
    Dialect = property(lambda self: SqlDialect)
    loading = False

    # Read

    def read_data_stream_create(self):
        dialect = self.file.dialect

        # Ensure table
        if not dialect.table:
            raise exceptions.FrictionlessException(
                'Format "sql" requires "table" option.'
            )

        # Stream data
        engine = create_engine(self.file.source)
        engine.update_execution_options(stream_results=True)
        table = sql.table(dialect.table)
        order = sql.text(dialect.order_by) if dialect.order_by else None
        query = sql.select(["*"]).select_from(table).order_by(order)
        data = iter(engine.execute(query))
        item = next(data)
        yield list(item.keys())
        yield list(item)
        for item in iter(data):
            yield list(item)

    # Write

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


# Dialect


class SqlDialect(Dialect):
    """Sql dialect representation

    # Arguments
        descriptor? (str|dict): descriptor
        table (str): table
        order_by? (str): order_by

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_profile = {  # type: ignore
        "type": "object",
        "required": ["table"],
        "additionalProperties": False,
        "properties": {"table": {"type": "string"}, "order_by": {"type": "string"}},
    }

    def __init__(self, descriptor=None, *, table=None, order_by=None, metadata_root=None):
        self.setdefined("table", table)
        self.setdefined("order_by", order_by)
        super().__init__(descriptor, metadata_root=metadata_root)

    @property
    def table(self):
        return self.get("table")

    @property
    def order_by(self):
        return self.get("order_by")


# Internal


SQL_SCHEMES = ["firebird", "mssql", "mysql", "oracle", "postgresql", "sqlite", "sybase"]
