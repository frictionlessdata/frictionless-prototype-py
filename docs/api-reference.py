import os
import re
from subprocess import check_output


target_dir = os.path.join("docs", "target", "api-reference")
target = os.path.join(target_dir, "README.md")
document = check_output("pydoc-markdown -p frictionless", shell=True).decode()
os.makedirs(target_dir, exist_ok=True)
with open(target, "wt") as file:
    for line in document.splitlines(keepends=True):
        line = re.sub(r"^## ", "### ", line)
        line = re.sub(r"^# ", "## ", line)
        line = re.sub(r"^## frictionless$", "# API Reference", line)
        line = re.sub(r" Objects$", "", line)
        line = re.sub(r"^#### (.*)$", "#### <big>\\1</big>", line)
        file.write(line)
