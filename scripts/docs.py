import os
import gdown
import shutil
import pathlib
from urllib.parse import urlparse


# Converters


def convert_markdown(name):
    source = os.path.join("docs", f"{name}.md")
    target = os.path.join("docs", "build", name)
    target_md = os.path.join("docs", "build", name, "README.md")
    os.makedirs(target, exist_ok=True)
    shutil.copy(source, target_md)


def convert_notebook(name):
    source = os.path.join("docs", f"{name}.nb")
    target = os.path.join("docs", "build", name)
    target_md = os.path.join(target, "README.md")
    target_py = os.path.join(target, "README.ipynb")
    pathlib.Path(target).mkdir(parents=True, exist_ok=True)
    link = open(source).read().strip()
    url = f"https://drive.google.com/uc?id={os.path.split(urlparse(link).path)[-1]}"
    gdown.download(url, target_py, quiet=True)
    os.system(f"python3 -m nbconvert --to markdown {target_py} --log-level 0")
    lines = []
    opening = True
    with open(target_md) as file:
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
    with open(target_md, "w") as file:
        file.write("\n".join(lines))


def convert_python(name):
    source = os.path.join("docs", f"{name}.py")
    os.system(f"python3 {source}")


# Main

if __name__ == "__main__":
    for path in sorted(os.listdir("docs")):
        fullpath = os.path.join("docs", path)
        if os.path.isfile(fullpath):
            name, format = os.path.splitext(path)
            if format == ".md":
                convert_markdown(name)
            elif format == ".nb":
                convert_notebook(name)
            elif format == ".py":
                convert_python(name)
            print(f"Converted: {fullpath}")
    shutil.copy("README.md", os.path.join("docs", "build", "README.md"))
    print(f"Converted: README.md")
