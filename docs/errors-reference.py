import os
from jinja2 import Template
from frictionless import errors


# General


source = os.path.join("docs", "assets", "errors-reference.md")
target = os.path.join("docs", "build", "errors-reference")
target_md = os.path.join(target, "README.md")
with open(source) as file:
    template = Template(file.read())
    Errors = [item for item in vars(errors).values() if hasattr(item, "code")]
    document = template.render(Errors=Errors)
with open(target_md, "wt") as file:
    file.write(document)
