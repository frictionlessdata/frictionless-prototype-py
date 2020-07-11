from .. import errors
from ..check import Check


class ChecksumCheck(Check):
    metadata_profile = {  # type: ignore
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "hash": {"type": "string"},
            "bytes": {"type": "number"},
            "rows": {"type": "number"},
        },
    }
    possible_Errors = [  # type: ignore
        errors.ChecksumError,
    ]

    # Validate

    def validate_table(self):

        # Hash
        if self.get("hash"):
            hashing = self.table.hashing
            if self["hash"] != self.table.stats["hash"]:
                note = 'expected hash in %s is "%s" and actual is "%s"'
                note = note % (hashing, self["hash"], self.table.stats["hash"])
                yield errors.ChecksumError(note=note)

        # Bytes
        if self.get("bytes"):
            if self["bytes"] != self.table.stats["bytes"]:
                note = 'expected bytes count is "%s" and actual is "%s"'
                note = note % (self["bytes"], self.table.stats["bytes"])
                yield errors.ChecksumError(note=note)

        # Rows
        if self.get("rows"):
            if self["rows"] != self.table.stats["rows"]:
                note = 'expected rows count is "%s" and actual is "%s"'
                note = note % (self["rows"], self.table.stats["rows"])
                yield errors.ChecksumError(note=note)
