from .metadata import Metadata
from . import helpers
from . import errors


class Query(Metadata):
    """Query representation

    """

    metadata_Error = errors.QueryError
    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "pickFields": {"type": "array"},
            "skipFields": {"type": "array"},
            "limitFields": {"type": "number"},
            "offsetFields": {"type": "number"},
            "pickRows": {"type": "array"},
            "skipRows": {"type": "array"},
            "limitRows": {"type": "number"},
            "offsetRows": {"type": "number"},
        },
    }

    def __init__(
        self,
        descriptor=None,
        *,
        pick_fields=None,
        skip_fields=None,
        limit_fields=None,
        offset_fields=None,
        pick_rows=None,
        skip_rows=None,
        limit_rows=None,
        offset_rows=None,
    ):
        self.setinitial("pickFields", pick_fields)
        self.setinitial("skipFields", skip_fields)
        self.setinitial("limitFields", limit_fields)
        self.setinitial("offsetFields", offset_fields)
        self.setinitial("pickRows", pick_rows)
        self.setinitial("skipRows", skip_rows)
        self.setinitial("limitRows", limit_rows)
        self.setinitial("offsetRows", offset_rows)
        super().__init__(descriptor)

    @Metadata.property
    def pick_fields(self):
        return self.get("pickFields")

    @Metadata.property
    def skip_fields(self):
        return self.get("skipFields")

    @Metadata.property
    def limit_fields(self):
        return self.get("limitFields")

    @Metadata.property
    def offset_fields(self):
        return self.get("offsetFields")

    @Metadata.property
    def pick_rows(self):
        return self.get("pickRows")

    @Metadata.property
    def skip_rows(self):
        return self.get("skipRows")

    @Metadata.property
    def limit_rows(self):
        return self.get("limitRows")

    @Metadata.property
    def offset_rows(self):
        return self.get("offsetRows")

    @Metadata.property(write=False)
    def is_field_filtering(self):
        return (
            self.pick_fields is not None
            or self.skip_fields is not None
            or self.limit_fields is not None
            or self.offset_fields is not None
        )

    @Metadata.property(write=False)
    def pick_fields_compiled(self):
        return helpers.compile_regex(self.pick_fields)

    @Metadata.property(write=False)
    def skip_fields_compiled(self):
        return helpers.compile_regex(self.skip_fields)

    @Metadata.property(write=False)
    def pick_rows_compiled(self):
        return helpers.compile_regex(self.pick_rows)

    @Metadata.property(write=False)
    def skip_rows_compiled(self):
        return helpers.compile_regex(self.skip_rows)

    # Expand

    def expand(self):
        pass

    # Import/Export

    def to_dict(self, expand=False):
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result
