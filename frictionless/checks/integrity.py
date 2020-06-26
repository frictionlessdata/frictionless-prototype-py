from .. import errors
from ..check import Check


class IntegrityCheck(Check):
    metadata_profile = {  # type: ignore
        'type': 'object',
        'properties': {
            'stats': {
                'type': ['object', 'null'],
                'properties': {
                    'hash': {'type': ['string', 'null']},
                    'bytes': {'type': ['number', 'null']},
                },
            },
            'lookup': {'type': ['object', 'null']},
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
        self.stats = self.get('stats') or {}
        self.stats_hash = self.stats.get('hash')
        self.stats_bytes = self.stats.get('bytes')
        self.lookup = self.get('lookup')
        self.memory_unique = {}
        for field in self.schema.fields:
            if field.constraints.get('unique'):
                self.memory_unique[field.name] = {}
        self.memory_primary = {}
        self.foreign_groups = []
        if self.lookup:
            for fk in self.schema.foreign_keys:
                group = {}
                group['sourceName'] = fk['reference']['resource']
                group['sourceKey'] = tuple(fk['reference']['fields'])
                group['targetKey'] = tuple(fk['fields'])
                self.foreign_groups.append(group)

    # Validate

    def validate_row(self, row):

        # Unique Error
        if self.memory_unique:
            for field_name in self.memory_unique.keys():
                cell = row[field_name]
                if cell is not None:
                    match = self.memory_unique[field_name].get(cell)
                    self.memory_unique[field_name][cell] = row.row_position
                    if match:
                        note = 'the same as in the row at position %s' % match
                        yield errors.UniqueError.from_row(
                            row, note=note, field_name=field_name
                        )

        # Primary Key Error
        if self.schema.primary_key:
            cells = tuple(row[field_name] for field_name in self.schema.primary_key)
            if set(cells) == {None}:
                note = 'cells composing the primary keys are all "None"'
                yield errors.PrimaryKeyError.from_row(row, note=note)
            else:
                match = self.memory_primary.get(cells)
                self.memory_primary[cells] = row.row_position
                if match:
                    if match:
                        note = 'the same as in the row at position %s' % match
                        yield errors.PrimaryKeyError.from_row(row, note=note)

        # Foreign Key Error
        if self.foreign_groups:
            for group in self.foreign_groups:
                group_lookup = self.lookup.get(group['sourceName'])
                if group_lookup:
                    cells = tuple(row[field_name] for field_name in group['targetKey'])
                    if set(cells) == {None}:
                        continue
                    match = cells in group_lookup.get(group['sourceKey'], set())
                    if not match:
                        note = 'not found in the lookup table'
                        yield errors.ForeignKeyError.from_row(row, note=note)

    def validate_table(self):

        # Hash
        if self.stats_hash:
            hashing = self.stream.hashing
            if self.stats_hash != self.stream.stats['hash']:
                note = 'expected hash in %s is "%s" and actual is "%s"'
                note = note % (hashing, self.stats_hash, self.stream.stats['hash'])
                yield errors.ChecksumError(note=note)

        # Bytes
        if self.stats_bytes:
            if self.stats_bytes != self.stream.stats['bytes']:
                note = 'expected size in bytes is "%s" and actual is "%s"'
                note = note % (self.stats_bytes, self.stream.stats['bytes'])
                yield errors.ChecksumError(note=note)
