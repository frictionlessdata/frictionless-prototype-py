import isodate
import datetime
import collections
import numpy as np
import pandas as pd
import pandas.core.dtypes.api as pdc
from ..plugin import Plugin
from ..storage import Storage, StorageTable
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

    def create_storage(self, source):
        pass


# Storage


# TODO: move dependencies from the top to here
class PandasStorage(Storage):
    """Pandas storage implementation"""

    def __init__(self, dataframes=None):
        self.__dataframes = dataframes or collections.OrderedDict()
        self.__tables = {}

    def __repr__(self):
        return "Storage <pandas>"

    # Read

    def read_names(self):
        return list(sorted(self.__dataframes.keys()))

    def read_tables(self):
        tables = []
        for name in self.read_names():
            table = self.read_table(name)
            tables.append(table)
        return tables

    def read_table(self, name):
        table = self.__tables.get(name)
        if table is None:
            if name not in self.__dataframes:
                note = f'Table "{name}" does not exist'
                raise exceptions.FrictionlessException(errors.StorageError(note=note))
            dataframe = self.__dataframes[name]
            table = self.read_table_convert(name, dataframe)
        return table

    def read_table_convert_table(self, name, dataframe):
        schema = Schema()

        # Primary key
        if dataframe.index.name:
            type = self.read_table_convert_field_type(dataframe.index.dtype)
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

        # Return table
        table = StorageTable(name, schema=schema)
        return table

    def read_table_convert_field_type(self, dtype, sample=None):

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

    def read_table_to_row_stream(self, name):
        table = self.read_table(name)
        schema = table.schema

        # Yield rows
        for pk, row in self.__dataframes[name].iterrows():
            row = self.__mapper.restore_row(row, schema=schema, pk=pk)
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

    # Write

    def write_table(self, *tables, force=False):

        # Check existent
        for table in tables:
            if table.names in self.read_names():
                if not force:
                    note = f'Table "{table.name}" already exists'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                self.write_table_remove_table(table.name)

        # Define dataframes
        for table in tables:
            self.__tables[table.name] = table
            self.__dataframes[table.name] = pd.DataFrame()

    def write_table_remove_table(self, *names, ignore=False):

        # Iterate over buckets
        for name in names:

            # Check existent
            if name not in self.read_names():
                if not ignore:
                    note = f'Table "{name}" does not exist'
                    raise exceptions.FrictionlessException(errors.StorageError(note=note))
                return

            # Remove table
            self.__tables.pop(name)
            self.__dataframes.pop(name)

    def write_table_convert_table(self, table, row_stream):

        # Get data/index
        data_rows = []
        index_rows = []
        jtstypes_map = {}
        schema = table.schema
        for row in row_stream:
            data_values = []
            index_values = []
            for field, value in zip(schema.fields, row):
                if isinstance(value, float) and np.isnan(value):
                    value = None
                if value and field.type == "integer":
                    value = int(value)
                # http://pandas.pydata.org/pandas-docs/stable/gotchas.html#support-for-integer-na
                if value is None and field.type in ("number", "integer"):
                    jtstypes_map[field.name] = "number"
                    value = np.NaN
                if field.name in schema.primary_key:
                    index_values.append(value)
                else:
                    data_values.append(value)
            if len(schema.primary_key) == 1:
                index_rows.append(index_values[0])
            elif len(schema.primary_key) > 1:
                index_rows.append(tuple(index_values))
            data_rows.append(tuple(data_values))

        # Create index
        index = None
        if schema.primary_key:
            if len(schema.primary_key) == 1:
                index_class = pd.Index
                index_field = schema.get_field(schema.primary_key[0])
                index_dtype = self.convert_type(index_field.type)
                if field.type in ["datetime", "date"]:
                    index_class = pd.DatetimeIndex
                index = index_class(index_rows, name=index_field.name, dtype=index_dtype)
            elif len(schema.primary_key) > 1:
                index = pd.MultiIndex.from_tuples(index_rows, names=schema.primary_key)

        # Create dtypes/columns
        dtypes = []
        columns = []
        for field in schema.fields:
            if field.name not in schema.primary_key:
                field_name = field.name
                dtype = self.convert_type(jtstypes_map.get(field.name, field.type))
                dtypes.append((field_name, dtype))
                columns.append(field.name)

        # Create dataframe
        array = np.array(data_rows, dtype=dtypes)
        dataframe = pd.DataFrame(array, index=index, columns=columns)

        return dataframe

    def write_table_convert_field_type(self, type):

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
        note = f'Column type "{type}" is not supported'
        raise exceptions.FrictionlessException(errors.StorageError(note=note))

    def write_table_from_row_stream(self, name, row_stream):

        # Prepare
        table = self.read_table(name)
        new_data_frame = self.write_table_convert_table(table, row_stream)

        # Just set new DataFrame if current is empty
        if self.__dataframes[name].size == 0:
            self.__dataframes[name] = new_data_frame

        # Append new data frame to the old one setting new data frame
        # containing data from both old and new data frames
        else:
            self.__dataframes[name] = pd.concat([self.__dataframes[name], new_data_frame])
