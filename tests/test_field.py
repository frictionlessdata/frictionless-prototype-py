import pytest
from functools import partial
from frictionless import Field


# General

DESCRIPTOR = {
    'name': 'id',
    'type': 'integer',
    'format': 'default',
    'missingValues': ['-'],
    'constraints': {'required': True},
}


def test_field():
    field = Field(DESCRIPTOR)
    assert field.name == 'id'
    assert field.type == 'integer'
    assert field.format == 'default'
    assert field.missing_values == ['-']
    assert field.constraints == {'required': True}
    assert field.required is True


def test_field_defaults():
    field = Field({'name': 'id'})
    assert field.name == 'id'
    assert field.type == 'any'
    assert field.format == 'default'
    assert field.missing_values == ['']
    assert field.constraints == {}
    assert field.required is False


def test_read_cell():
    field = Field(DESCRIPTOR)
    assert field.read_cell('1') == (1, None)
    assert field.read_cell('string') == (None, {'type': 'type is "integer/default"'},)
    assert field.read_cell('-') == (None, {'required': 'constraint "required" is "True"'})


def test_read_cell_string_missing_values():
    field = Field({'name': 'name', 'type': 'string', 'missingValues': ['', 'NA', 'N/A']})
    assert field.read_cell('') == (None, None)
    assert field.read_cell('NA') == (None, None)
    assert field.read_cell('N/A') == (None, None)


def test_read_cell_number_missingValues():
    field = Field({'name': 'name', 'type': 'number', 'missingValues': ['', 'NA', 'N/A']})
    assert field.read_cell('') == (None, None)
    assert field.read_cell('NA') == (None, None)
    assert field.read_cell('N/A') == (None, None)


# Constraints


def test_read_cell_required():
    field = Field(
        {
            'name': 'name',
            'type': 'string',
            'constraints': {'required': True},
            'missingValues': ['', 'NA', 'N/A'],
        }
    )
    read = field.read_cell
    assert read('test') == ('test', None)
    assert read('null') == ('null', None)
    assert read('none') == ('none', None)
    assert read('nil') == ('nil', None)
    assert read('nan') == ('nan', None)
    assert read('-') == ('-', None)
    assert read('NA') == (None, {'required': 'constraint "required" is "True"'})
    assert read('N/A') == (None, {'required': 'constraint "required" is "True"'})
    assert read('') == (None, {'required': 'constraint "required" is "True"'})
    assert read(None) == (None, {'required': 'constraint "required" is "True"'})


def test_read_cell_minLength():
    field = Field({'name': 'name', 'type': 'string', 'constraints': {'minLength': 2}})
    read = field.read_cell
    assert read('abc') == ('abc', None)
    assert read('ab') == ('ab', None)
    assert read('a') == ('a', {'minLength': 'constraint "minLength" is "2"'})
    # Null value passes
    assert read('') == (None, None)


def test_read_cell_maxLength():
    field = Field({'name': 'name', 'type': 'string', 'constraints': {'maxLength': 2}})
    read = field.read_cell
    assert read('abc') == ('abc', {'maxLength': 'constraint "maxLength" is "2"'})
    assert read('ab') == ('ab', None)
    assert read('a') == ('a', None)
    # Null value passes
    assert read('') == (None, None)


def test_read_cell_minimum():
    field = Field({'name': 'name', 'type': 'integer', 'constraints': {'minimum': 2}})
    read = field.read_cell
    assert read('3') == (3, None)
    assert read(3) == (3, None)
    assert read('2') == (2, None)
    assert read(2) == (2, None)
    assert read('1') == (1, {'minimum': 'constraint "minimum" is "2"'})
    assert read(1) == (1, {'minimum': 'constraint "minimum" is "2"'})
    # Null value passes
    assert read('') == (None, None)


def test_read_cell_maximum():
    field = Field({'name': 'name', 'type': 'integer', 'constraints': {'maximum': 2}})
    read = field.read_cell
    assert read('3') == (3, {'maximum': 'constraint "maximum" is "2"'})
    assert read(3) == (3, {'maximum': 'constraint "maximum" is "2"'})
    assert read('2') == (2, None)
    assert read(2) == (2, None)
    assert read('1') == (1, None)
    assert read(1) == (1, None)
    # Null value passes
    assert read('') == (None, None)


def test_read_cell_pattern():
    field = Field({'name': 'name', 'type': 'string', 'constraints': {'pattern': 'a|b'}})
    read = field.read_cell
    assert read('a') == ('a', None)
    assert read('b') == ('b', None)
    assert read('c') == ('c', {'pattern': 'constraint "pattern" is "a|b"'})
    # Null value passes
    assert read('') == (None, None)


def test_read_cell_enum():
    field = Field(
        {'name': 'name', 'type': 'integer', 'constraints': {'enum': ['1', '2', '3']}}
    )
    read = field.read_cell
    assert read('1') == (1, None)
    assert read(1) == (1, None)
    assert read('4') == (4, {'enum': 'constraint "enum" is "[\'1\', \'2\', \'3\']"'})
    assert read(4) == (4, {'enum': 'constraint "enum" is "[\'1\', \'2\', \'3\']"'})
    # Null value passes
    assert read('') == (None, None)


def test_read_cell_multiple_constraints():
    field = Field(
        {
            'name': 'name',
            'type': 'string',
            'constraints': {'pattern': 'a|b', 'enum': ['a', 'b']},
        }
    )
    read = field.read_cell
    assert read('a') == ('a', None)
    assert read('b') == ('b', None)
    assert read('c') == (
        'c',
        {
            'pattern': 'constraint "pattern" is "a|b"',
            'enum': 'constraint "enum" is "[\'a\', \'b\']"',
        },
    )
    # Null value passes
    assert read('') == (None, None)
