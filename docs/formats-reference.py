import os
from scripts import docs
from jinja2 import Template


# General


source = os.path.join(docs.SOURCE_DIR, "formats-reference.md")
target_dir = os.path.join(docs.TARGET_DIR, "formats-reference")
target_md = os.path.join(target_dir, "README.md")
os.makedirs(target_dir, exist_ok=True)
with open(source) as file:
    template = Template(file.read())
    document = template.render()
with open(target_md, "wt") as file:
    file.write(document)

