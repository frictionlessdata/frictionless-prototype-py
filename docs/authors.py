import os
import shutil


source = "AUTHORS.md"
target_dir = os.path.join("docs", "build", "authors")
target = os.path.join(target_dir, "README.md")
os.makedirs(target_dir)
shutil.copy(source, target)
