import re
import sqlalchemy as sa
from functools import partial
import sqlalchemy.dialects.postgresql as sapg
from ..storage import Storage, StorageTable
from ..metadata import Metadata
from ..dialects import Dialect
from ..plugin import Plugin
from ..parser import Parser
from ..schema import Schema
from ..field import Field
from .. import exceptions
from .. import helpers
from .. import errors


# Plugin


class SqlPlugin(Plugin):
    """Plugin for SQL

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.sql import SqlPlugin`

    """

    def create_dialect(self, file, *, descriptor):
        if file.scheme in SQL_SCHEMES:
            return SqlDialect(descriptor)

    def create_parser(self, file):
        if file.scheme in SQL_SCHEMES:
            return SqlParser(file)


# Parser


# NOTE: extend native types (make property as it depends on the database engine)
class SqlParser(Parser):
    """SQL parser implementation.

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.sql import SqlParser`

    """

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
    """SQL dialect representation

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.sql import SqlDialect`

    Parameters:
        descriptor? (str|dict): descriptor
        table (str): table
        order_by? (str): order_by

    Raises:
        FrictionlessException: raise any error that occurs during the process

    """

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

    # Metadata

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


# Storage


class SqlStorage(Storage):
    """SQL storage representation

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.sql import SqlStorage`

    Parameters:
        engine (object): `sqlalchemy` engine
        prefix (str): prefix for all buckets

    """

    # Public

    def __init__(self, *, engine, prefix=""):

        # Set attributes
        self.__prefix = prefix
        self.__connection = engine.connect()
        self.__dialect = engine.dialect.name
        self.__tables = {}

        # Create metadata and reflect
        self.__add_regex_support()
        self.__metadata = sa.MetaData(bind=self.__connection)
        self.__metadata.reflect()

    def __repr__(self):
        template = "Storage <{engine}>"
        text = template.format(engine=self.__connection.engine)
        return text

    # Tables

    def add_table(self, *tables, force=False):

        # Check existent
        for table in tables:
            if self.has_table(table.name):
                if not force:
                    note = f'Bucket "{table.name}" already exists'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                self.delete(table.name)

        # Convert tables
        sql_tables = []
        for table in tables:
            sql_table = self.write_table(table)
            sql_tables.append(sql_table)

        # Create tables
        try:
            self.__metadata.create_all(tables=sql_tables)
            for table in tables:
                self.__tables[table.name] = table
        except sa.exc.ProgrammingError as exception:
            if "there is no unique constraint matching given keys" in str(exception):
                note = "Foreign keys can only reference primary key or unique fields\n%s"
                error = errors.StorageError(note=note % exception)
                raise exceptions.FrictionlessException(error) from exception

    def remove_table(self, *names, ignore=False):

        # Iterate
        sql_tables = []
        for name in names:

            # Check existent
            if not self.has_table(name):
                if not ignore:
                    note = f'Bucket "{name}" does not exist'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                return

            # Remove from tables
            if name in self.__tables:
                del self.__tables[name]

            # Add table to tables
            sql_table = self.__metadata.tables[name]
            sql_tables.append(sql_table)

        # Drop tables, update metadata
        self.__metadata.drop_all(tables=sql_tables)
        self.__metadata.clear()
        self.__metadata.reflect()

    def get_table(self, name):
        table = self.__tables.get(name)
        if table is None:
            table = self.read_table(name)
        return table

    def has_table(self, name):
        return name in self.list_table_names()

    def list_tables(self):
        tables = []
        for name in self.list_table_names():
            table = self.get_table(name)
            tables.append(table)
        return table

    def list_table_names(self):
        names = []
        for table in self.__metadata.sorted_tables:
            name = self.read_table_name(table.name)
            if name is not None:
                names.append(name)
        return names

    # Read

    def read_table(self, name):

        # Prepare
        schema = Schema()
        sql_name = self.read_table_name(name)
        sql_table = self.__metadata[sql_name]

        # Fields
        for column in sql_table.columns:
            field_type = self.read_field_type(column.type)
            field = Field(name=column.name, type=field_type)
            if not column.nullable:
                field.required = True
            schema.fields.append(field)

        # Primary key
        for constraint in sql_table.constraints:
            if not isinstance(constraint, sa.PrimaryKeyConstraint):
                for column in constraint.columns:
                    schema.primary_key.append(column.name)

        # Foreign keys
        if self.__dialect in ["postgresql", "sqlite"]:
            for constraint in sql_table.constraints:
                if isinstance(constraint, sa.ForeignKeyConstraint):
                    resource = ""
                    own_fields = []
                    foreign_fields = []
                    for element in constraint.elements:
                        own_fields.append(element.parent.name)
                        if element.column.table.name != sql_name:
                            resource = self.restore_bucket(element.column.table.name)
                        foreign_fields.append(element.column.name)
                    if len(own_fields) == len(foreign_fields) == 1:
                        own_fields = own_fields.pop()
                        foreign_fields = foreign_fields.pop()
                    ref = {"resource": resource, "fields": foreign_fields}
                    schema.foreign_keys.append({"fields": own_fields, "reference": ref})

        # Create table
        table = StorageTable(name, schema=schema)
        return table

    def read_table_name(self, name):
        if name.startswith(self.__prefix):
            return name.replace(self.__prefix, "", 1)
        return None

    def read_field_type(self, name):

        # All dialects
        mapping = {
            sapg.ARRAY: "array",
            sa.Boolean: "boolean",
            sa.Date: "date",
            sa.DateTime: "datetime",
            sa.Float: "number",
            sa.Integer: "integer",
            sapg.JSONB: "object",
            sapg.JSON: "object",
            sa.Numeric: "number",
            sa.Text: "string",
            sa.Time: "time",
            sa.VARCHAR: "string",
            sapg.UUID: "string",
        }

        # Get field type
        for key, value in mapping.items():
            if isinstance(name, key):
                return value

        # Not supported type
        note = f'Column type "{name}" is not supported'
        raise exceptions.FrictionlessException(errors.StorageError(note=note))

    def read_row_stream(self, table):

        # Open and close transaction
        with self.__connection.begin():
            # Streaming could be not working for some backends:
            # http://docs.sqlalchemy.org/en/latest/core/connections.html
            select = table.select().execution_options(stream_results=True)
            result = select.execute()
            for row in result:
                row = self.__mapper.restore_row(row, schema=table.schema)
                yield row

    # Write

    def write_table(self, table):

        # Prepare
        columns = []
        constraints = []
        column_mapping = {}
        name = self.write_table_name(table.name)

        # Fields
        for field in table.schema.fields:
            checks = []
            nullable = not field.required
            column_type = self.write_field_type(field.type)
            unique = field.constraints.get("unique", False)
            for name, value in field.constraints.items():
                if name == "minLength":
                    checks.append(sa.Check('LENGTH("%s") >= %s' % (field.name, value)))
                elif name == "maxLength":
                    checks.append(sa.Check('LENGTH("%s") <= %s' % (field.name, value)))
                elif name == "minimum":
                    checks.append(sa.Check('"%s" >= %s' % (field.name, value)))
                elif name == "maximum":
                    checks.append(sa.Check('"%s" <= %s' % (field.name, value)))
                elif name == "pattern":
                    if self.__dialect in ["postgresql"]:
                        checks.append(sa.Check("\"%s\" ~ '%s'" % (field.name, value)))
                    else:
                        check = sa.Check("\"%s\" REGEXP '%s'" % (field.name, value))
                        checks.append(check)
                elif name == "enum":
                    enum_name = "%s_%s_enum" % (table.name, field.name)
                    column_type = sa.Enum(*value, name=enum_name)
            column_args = [field.name, column_type] + checks
            column = sa.Column(*column_args, nullable=nullable, unique=unique)
            columns.append(column)
            column_mapping[field.name] = column

        # Primary key
        if table.schema.primary_key is not None:
            constraint = sa.PrimaryKeyConstraint(*table.schema.primary_key)
            constraints.append(constraint)

        # Foreign keys
        if self.__dialect in ["postgresql", "sqlite"]:
            for fk in table.schema.foreign_keys:
                fields = fk["fields"]
                resource = fk["reference"]["resource"]
                foreign_fields = fk["reference"]["fields"]
                if resource != "":
                    table_name = self.write_table_name(resource)
                composer = lambda field: ".".join([table_name, field])
                foreign_fields = list(map(composer, foreign_fields))
                constraint = sa.ForeignKeyConstraint(fields, foreign_fields)
                constraints.append(constraint)

        # Convert
        sql_table = sa.Table(name, self.__metadata, *(columns + constraints))
        return sql_table

    def write_table_name(self, name):
        return self.__prefix + name

    def write_field_type(self, name):

        # Default dialect
        mapping = {
            "any": sa.Text,
            "array": None,
            "boolean": sa.Boolean,
            "date": sa.Date,
            "datetime": sa.DateTime,
            "duration": None,
            "geojson": None,
            "geopoint": None,
            "integer": sa.Integer,
            "number": sa.Float,
            "object": None,
            "string": sa.Text,
            "time": sa.Time,
            "year": sa.Integer,
            "yearmonth": None,
        }

        # Postgresql dialect
        if self.__dialect == "postgresql":
            mapping.update(
                {
                    "array": sapg.JSONB,
                    "geojson": sapg.JSONB,
                    "number": sa.Numeric,
                    "object": sapg.JSONB,
                }
            )

        # Return type
        if name in mapping:
            return mapping[name]

        # Not supported type
        note = f'Field type "{name}" is not supported'
        raise exceptions.FrictionlessException(errors.StorageError(note=note))

    def write_row_stream(self, bucket, rows, update_keys=None, buffer_size=1000):
        """Write to bucket

        # Arguments
            keyed (bool):
                accept keyed rows
            as_generator (bool):
                returns generator to provide writing control to the client
            update_keys (str[]):
                update instead of inserting if key values match existent rows
            buffer_size (int=1000):
                maximum number of rows to try and write to the db in one batch
            use_bloom_filter (bool=True):
                should we use a bloom filter to optimize DB update performance
                (in exchange for some setup time)

        """

        # Get table and description
        table = self.__get_table(bucket)

        # Write rows to table
        convert_row = partial(self.__mapper.convert_row, schema=table.schema)
        print(convert_row)
        #  writer = Writer(
        #  table,
        #  table.schema,
        #  # Only PostgreSQL supports "returning" so we don't use autoincrement for all
        #  autoincrement=autoincrement if self.__dialect in ["postgresql"] else None,
        #  update_keys=update_keys,
        #  convert_row=convert_row,
        #  buffer_size=buffer_size,
        #  )
        #  with self.__connection.begin():
        #  gen = writer.write(rows, keyed=keyed)
        #  collections.deque(gen, maxlen=0)

    # Private

    def __add_regex_support(self):
        # It will fail silently if this function already exists
        if self.__dialect == "sqlite":
            self.__connection.connection.create_function("REGEXP", 2, regexp)


# Internal


SQL_SCHEMES = ["firebird", "mssql", "mysql", "oracle", "postgresql", "sqlite", "sybase"]


def regexp(expr, item):
    reg = re.compile(expr)
    return reg.search(item) is not None
