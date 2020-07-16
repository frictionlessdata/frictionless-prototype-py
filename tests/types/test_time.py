import pytest
from datetime import time
from frictionless import Field


# General


@pytest.mark.parametrize(
    "format, source, target",
    [
        ("default", time(6), time(6)),
        ("default", "06:00:00", time(6)),
        ("default", "09:00", None),
        ("default", "3 am", None),
        ("default", "3.00", None),
        ("default", "invalid", None),
        ("default", True, None),
        ("default", "", None),
        ("any", time(6), time(6)),
        ("any", "06:00:00", time(6)),
        ("any", "3:00 am", time(3)),
        ("any", "some night", None),
        ("any", "invalid", None),
        ("any", True, None),
        ("any", "", None),
        ("%H:%M", time(6), time(6)),
        ("%H:%M", "06:00", time(6)),
        ("%M:%H", "06:50", None),
        ("%H:%M", "3:00 am", None),
        ("%H:%M", "some night", None),
        ("%H:%M", "invalid", None),
        ("%H:%M", True, None),
        ("%H:%M", "", None),
        ("invalid", "", None),
        # Deprecated
        ("fmt:%H:%M", time(6), time(6)),
        ("fmt:%H:%M", "06:00", time(6)),
        ("fmt:%M:%H", "06:50", None),
        ("fmt:%H:%M", "3:00 am", None),
        ("fmt:%H:%M", "some night", None),
        ("fmt:%H:%M", "invalid", None),
        ("fmt:%H:%M", True, None),
        ("fmt:%H:%M", "", None),
    ],
)
def test_time_read_cell(format, source, target):
    with pytest.warns(None) as recorded:
        field = Field({"name": "name", "type": "time", "format": format})
        cell, notes = field.read_cell(source)
        assert cell == target
    if not format.startswith("fmt:"):
        assert recorded.list == []
