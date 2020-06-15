import pytest
from frictionless import Field


# General


@pytest.mark.parametrize(
    'format, source, target',
    [
        ('default', 2000, 2000),
        ('default', '2000', 2000),
        ('default', -2000, None),
        ('default', 20000, None),
        ('default', '3.14', None),
        ('default', '', None),
    ],
)
def test_read_cell_year(format, source, target):
    field = Field({'name': 'name', 'type': 'year', 'format': format})
    cell, notes = field.read_cell(source)
    assert cell == target
