from copy import copy, deepcopy
from .metadata import Metadata
from .field import Field
from . import helpers
from . import errors
from . import config


class Schema(Metadata):
    """Schema representation

    API      | Usage
    -------- | --------
    Public   | `from frictionless import Schema`

    Parameters:
        descriptor? (str|dict): schema descriptor
        fields? (dict[]): list of field descriptors
        missing_values? (str[]): missing values
        primary_key? (str[]): primary key
        foreign_keys? (dict[]): foreign keys

    Raises:
        FrictionlessException: raise any error that occurs during the process

    """

    def __init__(
        self,
        descriptor=None,
        *,
        fields=None,
        missing_values=None,
        primary_key=None,
        foreign_keys=None,
    ):
        self.setinitial("fields", fields)
        self.setinitial("missingValues", missing_values)
        self.setinitial("primaryKey", primary_key)
        self.setinitial("foreignKeys", foreign_keys)
        super().__init__(descriptor)

    @Metadata.property
    def missing_values(self):
        """
        Returns:
            str[]: missing values
        """
        missing_values = self.get("missingValues", copy(config.DEFAULT_MISSING_VALUES))
        return self.metadata_attach("missingValues", missing_values)

    @Metadata.property
    def primary_key(self):
        """
        Returns:
            str[]: primary key field names
        """
        primary_key = self.get("primaryKey", [])
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        return self.metadata_attach("primaryKey", primary_key)

    @Metadata.property
    def foreign_keys(self):
        """
        Returns:
            dict[]: foreign keys
        """
        foreign_keys = deepcopy(self.get("foreignKeys", []))
        for index, fk in enumerate(foreign_keys):
            if not isinstance(fk, dict):
                continue
            fk.setdefault("fields", [])
            fk.setdefault("reference", {})
            fk["reference"].setdefault("resource", "")
            fk["reference"].setdefault("fields", [])
            if not isinstance(fk["fields"], list):
                fk["fields"] = [fk["fields"]]
            if not isinstance(fk["reference"]["fields"], list):
                fk["reference"]["fields"] = [fk["reference"]["fields"]]
        return self.metadata_attach("foreignKeys", foreign_keys)

    # Fields

    @Metadata.property
    def fields(self):
        """
        Returns:
            Field[]: an array of field instances
        """
        fields = self.get("fields", [])
        return self.metadata_attach("fields", fields)

    @Metadata.property(write=False)
    def field_names(self):
        """
        Returns:
            str[]: an array of field names
        """
        return [field.name for field in self.fields]

    def add_field(self, descriptor):
        """Add new field to schema.

        The schema descriptor will be validated with newly added field descriptor.

        Parameters:
            descriptor (dict): field descriptor

        Returns:
            Field/None: added `Field` instance or `None` if not added
        """
        self.setdefault("fields", [])
        self["fields"].append(descriptor)
        return self.fields[-1]

    def get_field(self, name):
        """Get schema's field by name.

        Parameters:
            name (str): schema field name

        Returns:
           Field/None: `Field` instance or `None` if not found
        """
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def has_field(self, name):
        """Check if a field is present

        Parameters:
            name (str): schema field name

        Returns:
           bool: whether there is the field
        """
        for field in self.fields:
            if field.name == name:
                return True
        return False

    def remove_field(self, name):
        """Remove field by name.

        The schema descriptor will be validated after field descriptor removal.

        Parameters:
            name (str): schema field name

        Returns:
            Field/None: removed `Field` instances or `None` if not found
        """
        field = self.get_field(name)
        if field:
            predicat = lambda field: field.name != name
            self["fields"] = list(filter(predicat, self.fields))
        return field

    # Expand

    def expand(self):
        """Expand the schema"""
        self.setdefault("fields", [])
        self.setdefault("missingValues", config.DEFAULT_MISSING_VALUES)
        for field in self.fields:
            field.expand()

    # Infer

    def infer(
        self,
        sample,
        *,
        type=None,
        names=None,
        confidence=config.DEFAULT_INFER_CONFIDENCE,
        missing_values=config.DEFAULT_MISSING_VALUES,
    ):
        """Infer schema

        Parameters:
            sample (any[][]): data sample
            type? (str): enforce all the field to be the given type
            names (str[]): enforce field names
            confidence (float): infer confidence from 0 to 1
            missing_values (str[]): provide custom missing values
        """

        # Missing values
        if missing_values != config.DEFAULT_MISSING_VALUES:
            self.missing_values = missing_values

        # Prepare names
        if not names:
            if not sample:
                return
            names = [f"field{number}" for number in range(1, len(sample[0]) + 1)]

        # Handle name/empty
        names = copy(names)
        for index, name in enumerate(names):
            names[index] = name or f"field{index+1}"

        # Deduplicate names
        if len(names) != len(set(names)):
            seen_names = []
            names = names.copy()
            for index, name in enumerate(names):
                count = seen_names.count(name) + 1
                names[index] = "%s%s" % (name, count) if count > 1 else name
                seen_names.append(name)

        # Handle type/empty
        if type or not sample:
            self.fields = [{"name": name, "type": type or "any"} for name in names]
            return

        # Prepare fields
        fields = []
        candidates = []
        max_score = [len(sample)] * len(names)
        for index, name in enumerate(names):
            candidates.append([])
            for type in INFER_TYPES:
                field = Field(name=name, type=type, schema=self)
                candidates[index].append({"field": field, "score": 0})
            fields.append(Field(name=name, type="any", schema=self))

        # Infer fields
        for cells in sample:
            for index, name in enumerate(names):
                if fields[index].type != "any":
                    continue
                source = cells[index] if len(cells) > index else None
                if source in missing_values:
                    max_score[index] -= 1
                    continue
                for candidate in candidates[index]:
                    if candidate["score"] < len(sample) * (confidence - 1):
                        continue
                    target, notes = candidate["field"].read_cell(source)
                    candidate["score"] += 1 if not notes else -1
                    if candidate["score"] >= max_score[index] * confidence:
                        fields[index] = candidate["field"]
                        break

        # Apply fields
        self.fields = fields

    # Read

    def read_data(self, cells):
        """Read a list of cells (normalize/cast)

        Parameters:
            cells (any[]): list of cells

        Returns:
            any[]: list of processed cells
        """
        result_cells = []
        result_notes = []
        for index, field in enumerate(self.fields):
            cell = cells[index] if len(cells) > index else None
            cell, notes = field.read_cell(cell)
            result_cells.append(cell)
            result_notes.append(notes)
        return result_cells, result_notes

    # Write

    def write_data(self, cells, *, native_types=[]):
        """Write a list of cells (normalize/uncast)

        Parameters:
            cells (any[]): list of cells

        Returns:
            any[]: list of processed cells
        """
        result_cells = []
        result_notes = []
        for index, field in enumerate(self.fields):
            notes = None
            cell = cells[index] if len(cells) > index else None
            if field.type not in native_types:
                cell, notes = field.write_cell(cell)
            result_cells.append(cell)
            result_notes.append(notes)
        return result_cells, result_notes

    # Import/Export

    @staticmethod
    def from_sample(
        sample,
        *,
        type=None,
        names=None,
        confidence=config.DEFAULT_INFER_CONFIDENCE,
        missing_values=config.DEFAULT_MISSING_VALUES,
    ):
        """Infer schema from sample

        Parameters:
            sample (any[][]): data sample
            type? (str): enforce all the field to be the given type
            names (str[]): enforce field names
            confidence (float): infer confidence from 0 to 1
            missing_values (str[]): provide custom missing values

        Returns:
            Schema: schema
        """
        schema = Schema()
        schema.infer(
            sample,
            type=type,
            names=names,
            confidence=confidence,
            missing_values=missing_values,
        )
        return schema

    def to_dict(self, expand=False):
        """Convert resource to dict

        Parameters:
            expand (bool): whether to expand
        """
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result

    # Metadata

    metadata_duplicate = True
    metadata_Error = errors.SchemaError  # type: ignore
    metadata_profile = deepcopy(config.SCHEMA_PROFILE)
    metadata_profile["properties"]["fields"] = {
        "type": "array",
        "items": {"type": "object"},
    }

    def metadata_process(self):

        # Fields
        fields = self.get("fields")
        if isinstance(fields, list):
            for index, field in enumerate(fields):
                if not isinstance(field, Field):
                    if not isinstance(field, dict):
                        field = {"name": f"field{index+1}", "type": "any"}
                    field = Field(field, schema=self)
                    list.__setitem__(fields, index, field)
            if not isinstance(fields, helpers.ControlledList):
                fields = helpers.ControlledList(fields)
                fields.__onchange__(self.metadata_process)
                dict.__setitem__(self, "fields", fields)

    def metadata_validate(self):
        yield from super().metadata_validate()

        # Fields
        for field in self.fields:
            yield from field.metadata_errors

        # Primary Key
        for name in self.primary_key:
            if name not in self.field_names:
                note = 'primary key "%s" does not match the fields "%s"'
                note = note % (self.primary_key, self.field_names)
                yield errors.SchemaError(note=note)

        # Foreign Keys
        for fk in self.foreign_keys:
            for name in fk["fields"]:
                if name not in self.field_names:
                    note = 'foreign key "%s" does not match the fields "%s"'
                    note = note % (fk, self.field_names)
                    yield errors.SchemaError(note=note)
            if len(fk["fields"]) != len(fk["reference"]["fields"]):
                note = 'foreign key fields "%s" does not match the reference fields "%s"'
                note = note % (fk["fields"], fk["reference"]["fields"])
                yield errors.SchemaError(note=note)


# Internal

INFER_TYPES = [
    "yearmonth",
    "geopoint",
    "duration",
    "geojson",
    "object",
    "array",
    "datetime",
    "time",
    "date",
    "integer",
    "number",
    "boolean",
    "year",
    "string",
]
