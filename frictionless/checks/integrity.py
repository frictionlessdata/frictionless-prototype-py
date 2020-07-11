from .. import errors
from ..check import Check


# TODO: merge to baseline?
class IntegrityCheck(Check):
    metadata_profile = {  # type: ignore
        "type": "object",
        "properties": {
            "stats": {
                "type": ["object", "null"],
                "properties": {
                    "hash": {"type": ["string", "null"]},
                    "bytes": {"type": ["number", "null"]},
                },
            },
            "lookup": {"type": ["object", "null"]},
        },
    }
    possible_Errors = [  # type: ignore
        # table
        errors.ChecksumError,
        # body
        errors.UniqueError,
        errors.PrimaryKeyError,
        errors.ForeignKeyError,
    ]

    def prepare(self):
        self.stats = self.get("stats") or {}
        self.stats_hash = self.stats.get("hash")
        self.stats_bytes = self.stats.get("bytes")

    # Validate

    def validate_table(self):

        # Hash
        if self.stats_hash:
            hashing = self.table.hashing
            if self.stats_hash != self.table.stats["hash"]:
                note = 'expected hash in %s is "%s" and actual is "%s"'
                note = note % (hashing, self.stats_hash, self.table.stats["hash"])
                yield errors.ChecksumError(note=note)

        # Bytes
        if self.stats_bytes:
            if self.stats_bytes != self.table.stats["bytes"]:
                note = 'expected size in bytes is "%s" and actual is "%s"'
                note = note % (self.stats_bytes, self.table.stats["bytes"])
                yield errors.ChecksumError(note=note)
