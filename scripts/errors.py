from jinja2 import Template
from frictionless import errors


with open("scripts/assets/errors.md") as file:
    template = Template(file.read())
    Errors = [item for item in vars(errors).values() if hasattr(item, "code")]
    document = template.render(Errors=Errors)
    print(document)
