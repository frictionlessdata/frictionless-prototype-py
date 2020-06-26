from frictionless import validate


# General


def test_validate_size():
    report = validate('data/table.csv', stats={'bytes': 30})
    assert report.table['valid']


def test_validate_size_invalid():
    report = validate('data/table.csv', stats={'bytes': 40})
    assert report.table.error.get('rowPosition') is None
    assert report.table.error.get('fieldPosition') is None
    assert report.flatten(['code', 'note']) == [
        ['checksum-error', 'expected size in bytes is "40" and actual is "30"'],
    ]


def test_validate_hash():
    hash = '6c2c61dd9b0e9c6876139a449ed87933'
    report = validate('data/table.csv', stats={'hash': hash})
    assert report.table['valid']


def test_validate_hash_invalid():
    hash = '6c2c61dd9b0e9c6876139a449ed87933'
    report = validate('data/table.csv', stats={'hash': 'bad'})
    assert report.flatten(['code', 'note']) == [
        ['checksum-error', 'expected hash in md5 is "bad" and actual is "%s"' % hash],
    ]


def test_validate_hash_md5():
    hash = '6c2c61dd9b0e9c6876139a449ed87933'
    report = validate('data/table.csv', stats={'hash': hash})
    assert report.table['valid']


def test_validate_hash_md5_invalid():
    hash = '6c2c61dd9b0e9c6876139a449ed87933'
    report = validate('data/table.csv', stats={'hash': 'bad'})
    assert report.flatten(['code', 'note']) == [
        ['checksum-error', 'expected hash in md5 is "bad" and actual is "%s"' % hash],
    ]


def test_validate_hash_sha1():
    hash = 'db6ea2f8ff72a9e13e1d70c28ed1c6b42af3bb0e'
    report = validate('data/table.csv', hashing='sha1', stats={'hash': hash})
    assert report.table['valid']


def test_validate_hash_sha1_invalid():
    hash = 'db6ea2f8ff72a9e13e1d70c28ed1c6b42af3bb0e'
    report = validate('data/table.csv', hashing='sha1', stats={'hash': 'bad'})
    assert report.flatten(['code', 'note']) == [
        ['checksum-error', 'expected hash in sha1 is "bad" and actual is "%s"' % hash],
    ]


def test_validate_hash_sha256():
    hash = 'a1fd6c5ff3494f697874deeb07f69f8667e903dd94a7bc062dd57550cea26da8'
    report = validate('data/table.csv', hashing='sha256', stats={'hash': hash})
    assert report.table['valid']


def test_validate_hash_sha256_invalid():
    hash = 'a1fd6c5ff3494f697874deeb07f69f8667e903dd94a7bc062dd57550cea26da8'
    report = validate('data/table.csv', hashing='sha256', stats={'hash': 'bad'})
    assert report.flatten(['code', 'note']) == [
        ['checksum-error', 'expected hash in sha256 is "bad" and actual is "%s"' % hash],
    ]


def test_validate_hash_sha512():
    hash = 'd52e3f5f5693894282f023b9985967007d7984292e9abd29dca64454500f27fa45b980132d7b496bc84d336af33aeba6caf7730ec1075d6418d74fb8260de4fd'
    report = validate('data/table.csv', hashing='sha512', stats={'hash': hash})
    assert report.table['valid']


def test_validate_hash_sha512_invalid():
    hash = 'd52e3f5f5693894282f023b9985967007d7984292e9abd29dca64454500f27fa45b980132d7b496bc84d336af33aeba6caf7730ec1075d6418d74fb8260de4fd'
    report = validate('data/table.csv', hashing='sha512', stats={'hash': 'bad'})
    assert report.flatten(['code', 'note']) == [
        ['checksum-error', 'expected hash in sha512 is "bad" and actual is "%s"' % hash],
    ]


def test_validate_unique_error():
    report = validate(
        'data/unique-field.csv',
        schema='data/unique-field.json',
        pick_errors=['unique-error'],
    )
    assert report.flatten(['rowPosition', 'fieldPosition', 'code']) == [
        [10, 1, 'unique-error'],
    ]


def test_validate_unique_error_and_type_error():
    source = [
        ['id', 'unique_number'],
        ['a1', 100],
        ['a2', 'bad'],
        ['a3', 100],
    ]
    schema = {
        'fields': [
            {'name': 'id'},
            {'name': 'unique_number', 'type': 'number', 'constraints': {'unique': True}},
        ]
    }
    report = validate(source, schema=schema)
    assert report.flatten(['rowPosition', 'fieldPosition', 'code']) == [
        [3, 2, 'type-error'],
        [4, 2, 'unique-error'],
    ]


def test_validate_primary_key_error():
    report = validate(
        'data/unique-field.csv',
        schema='data/unique-field.json',
        pick_errors=['primary-key-error'],
    )
    assert report.flatten(['rowPosition', 'fieldPosition', 'code']) == [
        [10, None, 'primary-key-error'],
    ]


def test_validate_primary_key_and_unique_error():
    report = validate('data/unique-field.csv', schema='data/unique-field.json',)
    assert report.flatten(['rowPosition', 'fieldPosition', 'code']) == [
        [10, 1, 'unique-error'],
        [10, None, 'primary-key-error'],
    ]


def test_validate_primary_key_error_composite():
    source = [
        ['id', 'name'],
        [1, 'Alex'],
        [1, 'John'],
        ['', 'Paul'],
        [1, 'John'],
        ['', None],
    ]
    schema = {
        'fields': [
            {'name': 'id', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
        ],
        'primaryKey': ['id', 'name'],
    }
    report = validate(source, schema=schema)
    assert report.flatten(['rowPosition', 'fieldPosition', 'code']) == [
        [5, None, 'primary-key-error'],
        [6, None, 'blank-row'],
        [6, None, 'primary-key-error'],
    ]


def test_validate_foreign_key_error():
    schema = {
        'fields': [
            {'name': 'id', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
        ],
        'foreignKeys': [
            {'fields': 'id', 'reference': {'resource': 'ids', 'fields': 'id'}}
        ],
    }
    lookup = {'ids': {('id',): set([(1,), (2,)])}}
    report = validate('data/table.csv', schema=schema, lookup=lookup)
    assert report.valid


def test_validate_foreign_key_error_invalid():
    schema = {
        'fields': [
            {'name': 'id', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
        ],
        'foreignKeys': [
            {'fields': 'id', 'reference': {'resource': 'ids', 'fields': 'id'}}
        ],
    }
    lookup = {'ids': {('id',): set([(1,)])}}
    report = validate('data/table.csv', schema=schema, lookup=lookup)
    assert report.flatten(['rowPosition', 'fieldPosition', 'code']) == [
        [3, None, 'foreign-key-error'],
    ]
