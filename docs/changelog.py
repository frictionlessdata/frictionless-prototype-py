import os
import shutil


source = "CHANGELOG.md"
target_dir = os.path.join("docs", "build", "changelog")
target = os.path.join(target_dir, "README.md")
os.makedirs(target_dir)
shutil.copy(source, target)
