import os
from jinja2 import Template
from frictionless import errors


# General


source = os.path.join("docs", "source", "errors-reference.md")
target_dir = os.path.join("docs", "target", "errors-reference")
target = os.path.join(target_dir, "README.md")
with open(source) as file:
    template = Template(file.read())
    Errors = [item for item in vars(errors).values() if hasattr(item, "code")]
    document = template.render(Errors=Errors)
os.makedirs(target_dir, exist_ok=True)
with open(target, "wt") as file:
    file.write(document)
