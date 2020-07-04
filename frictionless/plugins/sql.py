from importlib import import_module
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser


# Plugin


class SqlPlugin(Plugin):
    def create_parser(self, file):
        if file.scheme in SQL_SCHEMES:
            return SqlParser(file)


# Parser


# TODO: implement write
class SqlParser(Parser):
    Dialect = property(lambda self: SqlDialect)
    loading = False

    # Read

    def read_data_stream_create(self):
        sa = import_module("sqlalchemy")
        dialect = self.file.dialect
        engine = sa.create_engine(self.file.source)
        engine.update_execution_options(stream_results=True)
        table = sa.sql.table(dialect.table)
        order = sa.sql.text(dialect.order_by) if dialect.order_by else None
        query = sa.sql.select(["*"]).select_from(table).order_by(order)
        data = iter(engine.execute(query))
        item = next(data)
        yield list(item.keys())
        yield list(item)
        for item in iter(data):
            yield list(item)

    # Write

    def write(self, source, target, headers, encoding=None):
        sa = import_module("sqlalchemy")
        engine = sa.create_engine(target)
        count = 0
        buffer = []
        buffer_size = 1000
        with engine.begin() as conn:
            meta = sa.MetaData()
            columns = [sa.Column(header, sa.String()) for header in headers]
            table = sa.Table(self.__table, meta, *columns)
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
        "properties": {
            "table": {"type": "string"},
            "order_by": {"type": "string"},
            "headers": {"type": ["object", "array", "number", "null"]},
        },
    }

    def __init__(
        self, descriptor=None, *, table=None, order_by=None, headers=None,
    ):
        self.setinitial("table", table)
        self.setinitial("order_by", order_by)
        super().__init__(descriptor, headers=headers)

    @property
    def table(self):
        return self.get("table")

    @property
    def order_by(self):
        return self.get("order_by")


# Internal


SQL_SCHEMES = ["firebird", "mssql", "mysql", "oracle", "postgresql", "sqlite", "sybase"]
