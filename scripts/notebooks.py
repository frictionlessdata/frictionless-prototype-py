import os
import yaml
import gdown
import pathlib
from urllib.parse import urlparse


# Convert notebooks
with open("scripts/assets/notebooks.yaml") as file:
    config = yaml.safe_load(file)
    for name, link in config["source"].items():
        dirpath = os.path.join(config["target"], name)
        docpath_md = os.path.join(dirpath, "README.md")
        docpath_py = os.path.join(dirpath, "README.ipynb")
        pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)
        url = f"https://drive.google.com/uc?id={os.path.split(urlparse(link).path)[-1]}"
        gdown.download(url, docpath_py, quiet=True)
        os.system(f"python3 -m nbconvert --to markdown {docpath_py} --log-level 0")
        lines = []
        opening = True
        with open(docpath_md) as file:
            for index, line in enumerate(file.read().splitlines()):
                line = line.replace("[0m", "")
                line = line.replace("[1m", "")
                line = line.rstrip()
                if index == 1:
                    lines.append(
                        f"\n[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)]({link})\n\n"
                    )
                if line.startswith("```"):
                    if opening:
                        #  line = "```python"
                        opening = False
                    else:
                        opening = True
                if "README_files" in line:
                    line = line.replace("README_files", "./README_files")
                lines.append(line)
        with open(docpath_md, "w") as file:
            file.write("\n".join(lines))
        print(f"Converted: {docpath_md}")
