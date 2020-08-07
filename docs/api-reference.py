import os


target = os.path.join("docs", "build", "api-reference")
target_md = os.path.join(target, "README.md")
os.system(f"pydoc-markdown -p frictionless > {target_md}")
