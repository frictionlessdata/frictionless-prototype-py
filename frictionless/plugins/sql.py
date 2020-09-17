import re
import sqlalchemy as sa
from functools import partial
import sqlalchemy.dialects.postgresql as sapg
from ..metadata import Metadata
from ..dialects import Dialect
from ..resource import Resource
from ..storage import Storage
from ..package import Package
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

    def create_storage(self, name, **options):
        if name == "sql":
            return SqlStorage(**options)


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


# Parser


# NOTE: probably we can reuse Storage for read/write here
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


# Storage


# TODO: move dependencies from the top to here
# TODO: for now, read/write are oversimplified
class SqlStorage(Storage):
    """SQL storage representation

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.sql import SqlStorage`

    Parameters:
        engine (object): `sqlalchemy` engine
        prefix (str): prefix for all tables
        namespace (str): SQL scheme

    """

    def __init__(self, *, engine, prefix="", namespace=None):

        # Set attributes
        self.__prefix = prefix
        self.__namespace = namespace
        self.__connection = engine.connect()
        self.__dialect = engine.dialect.name
        self.__resources = {}

        # Add regex support
        # It will fail silently if this function already exists
        if self.__dialect == "sqlite":
            self.__connection.connection.create_function("REGEXP", 2, regexp)

        # Create metadata and reflect
        self.__metadata = sa.MetaData(bind=self.__connection)
        self.__metadata.reflect()

    def __repr__(self):
        template = "Storage <{engine}>"
        text = template.format(engine=self.__connection.engine)
        return text

    # Read

    def read_package(self):
        package = Package()
        for name in self.read_resource_names():
            resource = self.read_resource(name)
            package.resources.append(resource)
        return package

    def read_resource(self, name):
        resource = self.__resources.get(name)
        if resource is None:
            sql_table = self.read_sql_table(name)
            if not sql_table:
                note = f'Resource "{name}" does not exist'
                raise exceptions.FrictionlessException(errors.StorageError(note=note))
            schema = self.read_convert_schema(sql_table)
            data = partial(self.read_data_stream, name)
            resource = Resource(name=name, schema=schema, data=data)
            self.__resources[resource.name] = resource
        return resource

    def read_resource_names(self):
        names = []
        for sql_table in self.__metadata.sorted_tables:
            name = self.read_convert_name(sql_table.name)
            if name is not None:
                names.append(sql_table.name)
        return names

    def read_data_stream(self, name):
        sql_table = self.read_sql_table(name)
        with self.__connection.begin():
            # Streaming could be not working for some backends:
            # http://docs.sqlalchemy.org/en/latest/core/connections.html
            select = sql_table.select().execution_options(stream_results=True)
            result = select.execute()
            for cells in result:
                yield tuple(cells)

    def read_convert_name(self, sql_name):
        if sql_name.startswith(self.__prefix):
            return sql_name.replace(self.__prefix, "", 1)
        return None

    def read_convert_schema(self, sql_table):
        schema = Schema()

        # Fields
        for column in sql_table.columns:
            field_type = self.read_convert_type(column.type)
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
                        # TODO: review this comparision
                        if element.column.table.name != sql_table.name:
                            res_name = element.column.table.name
                            resource = self.read_table_convert_name(res_name)
                        foreign_fields.append(element.column.name)
                    if len(own_fields) == len(foreign_fields) == 1:
                        own_fields = own_fields.pop()
                        foreign_fields = foreign_fields.pop()
                    ref = {"resource": resource, "fields": foreign_fields}
                    schema.foreign_keys.append({"fields": own_fields, "reference": ref})

        # Return schema
        return schema

    def read_convert_type(self, sql_type):
        mapping = self.read_convert_types()

        # Supported type
        for key, value in mapping.items():
            if isinstance(sql_type, key):
                return value

        # Not supported type
        note = f'Column type "{sql_type}" is not supported'
        raise exceptions.FrictionlessException(errors.StorageError(note=note))

    def read_convert_types(self):
        return {
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

    def read_sql_table(self, name):
        sql_name = self.__prefix + name
        if self.__namespace:
            sql_name = ".".join((self.__namespace, sql_name))
        return self.__metadata.tables.get(sql_name)

    # Write

    # TODO: use a transaction
    def write_package(self, package, force=False):
        existent_names = self.read_resource_names()

        # Check existent
        for resource in package.resources:
            if resource.name in existent_names:
                if not force:
                    note = f'Table "{resource.name}" already exists'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                self.delete_resource(resource.name)

        # Convert tables
        sql_tables = []
        for resource in package.resources:
            sql_table = self.write_convert_schema(resource.name, resource.schema)
            sql_tables.append(sql_table)

        # Create tables
        try:
            self.__metadata.create_all(tables=sql_tables)
            for resource in package.resources:
                self.__resources[resource.name] = resource
        except sa.exc.ProgrammingError as exception:
            if "there is no unique constraint matching given keys" in str(exception):
                note = "Foreign keys can only reference primary key or unique fields\n%s"
                error = errors.StorageError(note=note % exception)
                raise exceptions.FrictionlessException(error) from exception

        # Write data
        for resource in package.resources:
            self.write_row_stream(resource.name, resource.read_row_stream())

    def write_resource(self, resource, *, force=False):
        package = Package(resources=[resource])
        return self.write_package(package, force=force)

    def write_row_stream(self, name, row_stream):
        buffer = []
        buffer_size = 1000
        sql_table = self.read_sql_table(name)
        with self.__connection.begin():
            for row in row_stream:
                buffer.append(row)
                if len(buffer) > buffer_size:
                    self.__connection.execute(sql_table.insert().values(buffer))
                    buffer = []
            if len(buffer):
                self.__connection.execute(sql_table.insert().values(buffer))

    def write_convert_name(self, name):
        return self.__prefix + name

    def write_convert_schema(self, name, schema):

        # Prepare
        columns = []
        constraints = []
        column_mapping = {}
        sql_name = self.write_convert_name(name)

        # Fields
        for field in schema.fields:
            checks = []
            nullable = not field.required
            column_type = self.write_convert_type(field.type)
            if not column_type:
                column_type = sa.Text
            unique = field.constraints.get("unique", False)
            for const, value in field.constraints.items():
                if const == "minLength":
                    checks.append(sa.Check('LENGTH("%s") >= %s' % (field.name, value)))
                elif const == "maxLength":
                    checks.append(sa.Check('LENGTH("%s") <= %s' % (field.name, value)))
                elif const == "minimum":
                    checks.append(sa.Check('"%s" >= %s' % (field.name, value)))
                elif const == "maximum":
                    checks.append(sa.Check('"%s" <= %s' % (field.name, value)))
                elif const == "pattern":
                    if self.__dialect in ["postgresql"]:
                        checks.append(sa.Check("\"%s\" ~ '%s'" % (field.name, value)))
                    else:
                        check = sa.Check("\"%s\" REGEXP '%s'" % (field.name, value))
                        checks.append(check)
                elif const == "enum":
                    enum_name = "%s_%s_enum" % (sql_name, field.name)
                    column_type = sa.Enum(*value, name=enum_name)
            column_args = [field.name, column_type] + checks
            column = sa.Column(*column_args, nullable=nullable, unique=unique)
            columns.append(column)
            column_mapping[field.name] = column

        # Primary key
        if schema.primary_key is not None:
            constraint = sa.PrimaryKeyConstraint(*schema.primary_key)
            constraints.append(constraint)

        # Foreign keys
        if self.__dialect in ["postgresql", "sqlite"]:
            for fk in schema.foreign_keys:
                fields = fk["fields"]
                resource = fk["reference"]["resource"]
                foreign_fields = fk["reference"]["fields"]
                table_name = sql_name
                if resource != "":
                    table_name = self.write_convert_name(resource)
                composer = lambda field: ".".join([table_name, field])
                foreign_fields = list(map(composer, foreign_fields))
                constraint = sa.ForeignKeyConstraint(fields, foreign_fields)
                constraints.append(constraint)

        # Create sql table
        sql_table = sa.Table(sql_name, self.__metadata, *(columns + constraints))
        return sql_table

    def write_convert_type(self, type):
        mapping = self.write_convert_types()

        # Supported type
        if type in mapping:
            return mapping[type]

        # Not supported type
        note = f'Field type "{type}" is not supported'
        raise exceptions.FrictionlessException(errors.StorageError(note=note))

    def write_convert_types(self):

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

        return mapping

    # Delete

    def delete_package(self, names, *, ignore=False):
        existent_names = self.read_resource_names()

        # Iterate
        sql_tables = []
        for name in names:

            # Check existent
            if name not in existent_names:
                if not ignore:
                    note = f'Resource "{name}" does not exist'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                continue

            # Remove from resources
            if name in self.__resources:
                del self.__resources[name]

            # Add table for removal
            sql_table = self.read_sql_table(name)
            sql_tables.append(sql_table)

        # Drop tables, update metadata
        self.__metadata.drop_all(tables=sql_tables)
        self.__metadata.clear()
        self.__metadata.reflect()

    def delete_resource(self, name, *, ignore=False):
        return self.delete_package([name], ignore=ignore)


# Internal


SQL_SCHEMES = ["firebird", "mssql", "mysql", "oracle", "postgresql", "sqlite", "sybase"]


def regexp(expr, item):
    reg = re.compile(expr)
    return reg.search(item) is not None
