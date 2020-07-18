from ..metadata import Metadata
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser
from .. import helpers


# Plugin


class SqlPlugin(Plugin):
    def create_dialect(self, file, *, descriptor):
        if file.scheme in SQL_SCHEMES:
            return SqlDialect(descriptor)

    def create_parser(self, file):
        if file.scheme in SQL_SCHEMES:
            return SqlParser(file)


# Parser


# NOTE: extend native types (make property as it depends on the database engine)
class SqlParser(Parser):
    loading = False
    native_types = [
        "string",
    ]

    # Read

    def read_data_stream_create(self):
        sa = helpers.import_from_plugin("sqlalchemy", plugin="sql")
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

    # NOTE: rewrite this method
    # NOTE: create columns using extended native types
    def write(self, row_stream):
        sa = helpers.import_from_plugin("sqlalchemy", plugin="sql")
        engine = sa.create_engine(self.file.source)
        dialect = self.file.dialect
        buffer = []
        buffer_size = 1000
        with engine.begin() as conn:
            for row in row_stream:
                schema = row.schema
                if row.row_number == 1:
                    meta = sa.MetaData()
                    columns = [sa.Column(nm, sa.String()) for nm in schema.field_names]
                    table = sa.Table(dialect.table, meta, *columns)
                    meta.create_all(conn)
                cells = list(row.values())
                cells, notes = schema.write_data(cells, native_types=self.native_types)
                buffer.append(cells)
                if len(buffer) > buffer_size:
                    conn.execute(table.insert().values(buffer))
                    buffer = []
            if len(buffer):
                conn.execute(table.insert().values(buffer))


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
            "header": {"type": "boolean"},
            "headerRows": {"type": "array", "items": {"type": "number"}},
            "headerJoin": {"type": "string"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        table=None,
        order_by=None,
        header=None,
        header_rows=None,
        header_join=None,
    ):
        self.setinitial("table", table)
        self.setinitial("order_by", order_by)
        super().__init__(
            descriptor=descriptor,
            header=header,
            header_rows=header_rows,
            header_join=header_join,
        )

    @Metadata.property
    def table(self):
        return self.get("table")

    @Metadata.property
    def order_by(self):
        return self.get("order_by")


# Internal


SQL_SCHEMES = ["firebird", "mssql", "mysql", "oracle", "postgresql", "sqlite", "sybase"]
