import json
import jsonschema
from .. import config
from ..field import Field


class GeojsonField(Field):
    def __init__(self, descriptor):
        super().__init__(descriptor)
        self.__validator = jsonschema.validators.validator_for(config.GEOJSON_PROFILE)(
            config.GEOJSON_PROFILE
        )

    # Read

    def read_cell_cast(self, cell):
        if isinstance(cell, str):
            try:
                cell = json.loads(cell)
            except Exception:
                return None
        if not isinstance(cell, dict):
            return None
        if format == 'default':
            try:
                self.__validator.validate(cell)
            except Exception:
                return None
        elif format == 'topojson':
            pass  # Accept any dict as possibly topojson for now
        return cell

    # Write

    def write_cell(self, cell):
        return str(cell)
