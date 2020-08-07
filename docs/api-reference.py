import os
import re
from subprocess import check_output


target = os.path.join("docs", "build", "api-reference")
target_md = os.path.join(target, "README.md")
document = check_output("pydoc-markdown -p frictionless", shell=True).decode()
with open(target_md, "wt") as file:
    for line in document.splitlines(keepends=True):
        line = re.sub(r"^## ", "### ", line)
        line = re.sub(r"^# ", "## ", line)
        line = re.sub(r"^## frictionless$", "# API Reference", line)
        line = re.sub(r" Objects$", "", line)
        file.write(line)
