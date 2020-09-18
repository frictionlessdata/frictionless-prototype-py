import isodate
import datetime
import collections
import numpy as np
import pandas as pd
import pandas.core.dtypes.api as pdc
from functools import partial
from ..resource import Resource
from ..package import Package
from ..plugin import Plugin
from ..storage import Storage
from ..schema import Schema
from ..field import Field
from .. import exceptions
from .. import errors


# Plugin


class PandasPlugin(Plugin):
    """Plugin for Pandas

    API      | Usage
    -------- | --------
    Public   | `from frictionless.plugins.pandas import PandasPlugin`

    """

    def create_storage(self, name, **options):
        if name == "pandas":
            return PandasStorage(**options)


# Storage


# TODO: move dependencies from the top to here
class PandasStorage(Storage):
    """Pandas storage implementation"""

    def __init__(self, *, dataframes=None):
        self.__dataframes = dataframes or collections.OrderedDict()

    def __repr__(self):
        return "Storage <pandas>"

    @property
    def dataframes(self):
        return self.__dataframes

    # Read

    def read_resource(self, name):
        dataframe = self.__read_pandas_dataframe(name)
        if not dataframe:
            note = f'Resource "{name}" does not exist'
            raise exceptions.FrictionlessException(errors.StorageError(note=note))
        schema = self.__read_convert_schema(dataframe)
        data = partial(self.__read_data_stream, name)
        resource = Resource(name=name, schema=schema, data=data)
        return resource

    def read_package(self):
        resources = []
        for name in self.read_names():
            resource = self.read_resource(name)
            resources.append(resource)
        return resources

    def __read_resource_names(self):
        return list(sorted(self.__dataframes.keys()))

    # TODO: fix the logic
    def __read_data_stream(self, name):
        table = self.read_table(name)
        schema = table.schema

        # Yield cells
        for pk, row in self.__dataframes[name].iterrows():
            result = []
            for field in schema.fields:
                if schema.primary_key and schema.primary_key[0] == field.name:
                    if field.type == "number" and np.isnan(pk):
                        pk = None
                    if pk and field.type == "integer":
                        pk = int(pk)
                    result.append(field.cast_value(pk))
                else:
                    value = row[field.name]
                    if field.type == "number" and np.isnan(value):
                        value = None
                    if value and field.type == "integer":
                        value = int(value)
                    elif field.type == "datetime":
                        value = value.to_pydatetime()
                    result.append(field.cast_value(value))
            yield result

    def __read_convert_schema(self, dataframe):
        schema = Schema()

        # Primary key
        if dataframe.index.name:
            type = self.__read_convert_type(dataframe.index.dtype)
            field = Field(name=dataframe.index.name, type=type)
            field.required = True
            schema.fields.append(field)
            schema.primary_key.append(dataframe.index.name)

        # Fields
        for name, dtype in dataframe.dtypes.iteritems():
            sample = dataframe[name].iloc[0] if len(dataframe) else None
            type = self.read_table_convert_field_type(dtype, sample=sample)
            field = Field(name=name, type=type)
            schema.fields.append(field)

        # Return schema
        return schema

    def __read_convert_type(self, dtype, sample=None):

        # Pandas types
        if pdc.is_bool_dtype(dtype):
            return "boolean"
        elif pdc.is_datetime64_any_dtype(dtype):
            return "datetime"
        elif pdc.is_integer_dtype(dtype):
            return "integer"
        elif pdc.is_numeric_dtype(dtype):
            return "number"

        # Python types
        if sample is not None:
            if isinstance(sample, (list, tuple)):
                return "array"
            elif isinstance(sample, datetime.date):
                return "date"
            elif isinstance(sample, isodate.Duration):
                return "duration"
            elif isinstance(sample, dict):
                return "object"
            elif isinstance(sample, str):
                return "string"
            elif isinstance(sample, datetime.time):
                return "time"

        # Default
        return "string"

    def __read_pandas_dataframe(self, name):
        return self.__dataframes.get(name)

    # Write

    def write_resource(self, resource, *, force=False):
        package = Package(resources=[resource])
        return self.write_package(package, force=force)

    def write_package(self, package, *, force=False):
        existent_names = self.__read_resource_names()

        # Copy/infer package
        package = Package(package)
        package.infer()

        # Check existent
        for resource in package.resources:
            if resource.name in existent_names:
                if not force:
                    note = f'Table "{resource.name}" already exists'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                self.delete_resource(resource.name)

        # Write resources
        for resource in package.resources:
            self.__dataframes[resource.name] = self.__write_convert_resource(resource)

    # TODO: fix the logic
    def __write_convert_resource(self, resource):

        # Get data/index
        types = {}
        data_rows = []
        index_rows = []
        for row in resource.read_rows():
            data_values = []
            index_values = []
            for field, value in zip(resource.schema.fields, row):
                if isinstance(value, float) and np.isnan(value):
                    value = None
                if value and field.type == "integer":
                    value = int(value)
                # http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na
                if value is None and field.type in ("number", "integer"):
                    types[field.name] = "number"
                    value = np.NaN
                if field.name in resource.schema.primary_key:
                    index_values.append(value)
                else:
                    data_values.append(value)
            if len(resource.schema.primary_key) == 1:
                index_rows.append(index_values[0])
            elif len(resource.schema.primary_key) > 1:
                index_rows.append(tuple(index_values))
            data_rows.append(tuple(data_values))

        # Create index
        index = None
        if resource.schema.primary_key:
            if len(resource.schema.primary_key) == 1:
                index_class = pd.Index
                index_field = resource.schema.get_field(resource.schema.primary_key[0])
                index_dtype = self.write_convert_type(index_field.type)
                if field.type in ["datetime", "date"]:
                    index_class = pd.DatetimeIndex
                index = index_class(index_rows, name=index_field.name, dtype=index_dtype)
            elif len(resource.schema.primary_key) > 1:
                index = pd.MultiIndex.from_tuples(
                    index_rows, names=resource.schema.primary_key
                )

        # Create dtypes/columns
        dtypes = []
        columns = []
        for field in resource.schema.fields:
            if field.name not in resource.schema.primary_key:
                field_name = field.name
                dtype = self.__write_convert_type(types.get(field.name, field.type))
                dtypes.append((field_name, dtype))
                columns.append(field.name)

        # Create dataframe
        array = np.array(data_rows, dtype=dtypes)
        dataframe = pd.DataFrame(array, index=index, columns=columns)

        return dataframe

    def __write_convert_type(self, type):

        # Mapping
        mapping = {
            "any": np.dtype("O"),
            "array": np.dtype(list),
            "boolean": np.dtype(bool),
            "date": np.dtype("O"),
            "datetime": np.dtype("datetime64[ns]"),
            "duration": np.dtype("O"),
            "geojson": np.dtype("O"),
            "geopoint": np.dtype("O"),
            "integer": np.dtype(int),
            "number": np.dtype(float),
            "object": np.dtype(dict),
            "string": np.dtype("O"),
            "time": np.dtype("O"),
            "year": np.dtype(int),
            "yearmonth": np.dtype("O"),
        }

        # Return type
        if type in mapping:
            return mapping[type]

        # Not supported type
        note = f'Field type "{type}" is not supported'
        raise exceptions.FrictionlessException(errors.StorageError(note=note))

    # Delete

    def delete_resource(self, name, *, ignore=False):
        return self.delete_package([name], ignore=ignore)

    def delete_package(self, names, *, ignore=False):
        existent_names = self.__read_resource_names()

        # Remove dataframes
        for name in names:

            # Check existent
            if name not in existent_names:
                if not ignore:
                    note = f'Resource "{name}" does not exist'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                return

            # Remove resource
            self.__dataframes.pop(name)
