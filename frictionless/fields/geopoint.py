import json
from collections import namedtuple
from decimal import Decimal
from ..field import Field


class GeopointField(Field):
    supported_constraints = [
        'required',
        'enum',
    ]

    # Read

    def read_cell_cast(self, cell):

        # Parse
        if isinstance(cell, str):
            try:
                if self.format == 'default':
                    lon, lat = cell.split(',')
                    lon = lon.strip()
                    lat = lat.strip()
                elif self.format == 'array':
                    lon, lat = json.loads(cell)
                elif self.format == 'object':
                    if isinstance(cell, str):
                        cell = json.loads(cell)
                    if len(cell) != 2:
                        return None
                    lon = cell['lon']
                    lat = cell['lat']
                cell = geopoint(Decimal(lon), Decimal(lat))
            except Exception:
                return None

        # Validate
        try:
            cell = geopoint(*cell)
            if cell.lon > 180 or cell.lon < -180:
                return None
            if cell.lat > 90 or cell.lat < -90:
                return None
        except Exception:
            return None

        return cell

    # Write

    def write_cell_cast(self, cell):
        return str(cell)


# Internal

geopoint = namedtuple('geopoint', ['lon', 'lat'])
geopoint.__repr__ = lambda self: str([float(self[0]), float(self[1])])  # type: ignore
