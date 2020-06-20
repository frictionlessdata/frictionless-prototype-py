import re
import uuid
import base64
import rfc3986.exceptions
import rfc3986.validators
import rfc3986.uri
from ..field import Field


class StringField(Field):
    supported_constraints = [
        'required',
        'minLength',
        'maxLength',
        'pattern',
        'enum',
    ]

    # Read

    def read_cell_cast(self, cell):
        if not isinstance(cell, str):
            return None
        if self.format == 'default':
            return cell
        elif self.format == 'uri':
            uri = uri_from_string(cell)
            try:
                uri_validator.validate(uri)
            except rfc3986.exceptions.ValidationError:
                return None
        elif self.format == 'email':
            if not re.match(EMAIL_PATTERN, cell):
                return None
        elif self.format == 'uuid':
            try:
                uuid.UUID(cell, version=4)
            except Exception:
                return None
        elif self.format == 'binary':
            try:
                base64.b64decode(cell)
            except Exception:
                return None
        return cell

    # Write

    def write_cell_cast(self, cell):
        return cell


# Internal

EMAIL_PATTERN = re.compile(r'[^@]+@[^@]+\.[^@]+')
uri_from_string = rfc3986.uri.URIReference.from_string
uri_validator = rfc3986.validators.Validator().require_presence_of('scheme')
