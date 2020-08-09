import os
import shutil


source = "CONTRIBUTING.md"
target_dir = os.path.join("docs", "build", "contribution-guide")
target = os.path.join(target_dir, "README.md")
os.makedirs(target_dir, exist_ok=True)
shutil.copy(source, target)
