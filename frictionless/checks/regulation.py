import simpleeval
from .. import errors
from ..check import Check


class BlacklistedValueCheck(Check):
    metadata_profile = {  # type: ignore
        "type": "object",
        "requred": ["fieldName", "blacklist"],
        "properties": {"fieldName": {"type": "string"}, "blacklist": {"type": "array"}},
    }
    possible_Errors = [  # type: ignore
        errors.BlacklistedValueError
    ]

    def prepare(self):
        self.__field_name = self["fieldName"]
        self.__blacklist = self["blacklist"]

    # Validate

    def validate_task(self):
        if self.__field_name not in self.table.schema.field_names:
            note = 'blacklisted value check requires field "%s"' % self.__field_name
            yield errors.TaskError(note=note)

    def validate_row(self, row):
        cell = row[self.__field_name]
        if cell in self.__blacklist:
            yield errors.BlacklistedValueError.from_row(
                row,
                note='blacklisted values are "%s"' % self.__blacklist,
                field_name=self.__field_name,
            )


class SequentialValueCheck(Check):
    metadata_profile = {  # type: ignore
        "type": "object",
        "requred": ["fieldName"],
        "properties": {"fieldName": {"type": "string"}},
    }
    possible_Errors = [  # type: ignore
        errors.SequentialValueError
    ]

    def prepare(self):
        self.__cursor = None
        self.__exited = False
        self.__field_name = self.get("fieldName")

    # Validate

    def validate_task(self):
        if self.__field_name not in self.table.schema.field_names:
            note = 'sequential value check requires field "%s"' % self.__field_name
            yield errors.TaskError(note=note)

    def validate_row(self, row):
        if not self.__exited:
            cell = row[self.__field_name]
            try:
                self.__cursor = self.__cursor or cell
                assert self.__cursor == cell
                self.__cursor += 1
            except Exception:
                self.__exited = True
                yield errors.SequentialValueError.from_row(
                    row, note="the value is not sequential", field_name=self.__field_name,
                )


class RowConstraintCheck(Check):
    metadata_profile = {  # type: ignore
        "type": "object",
        "requred": ["constraint"],
        "properties": {"constraint": {"type": "string"}},
    }
    possible_Errors = [  # type: ignore
        errors.RowConstraintError
    ]

    def prepare(self):
        self.__constraint = self["constraint"]

    # Validate

    def validate_row(self, row):
        try:
            # This call should be considered as a safe expression evaluation
            # https://github.com/danthedeckie/simpleeval
            assert simpleeval.simple_eval(self.__constraint, names=row)
        except Exception:
            yield errors.RowConstraintError.from_row(
                row, note='the row constraint to conform is "%s"' % self.__constraint,
            )
