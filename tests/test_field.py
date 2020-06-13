from frictionless import Field


# From exception


def test_field():
    field = Field({'name': 'name'})
    assert field
