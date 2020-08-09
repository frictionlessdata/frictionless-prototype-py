import os
import shutil


source = "CHANGELOG.md"
target = os.path.join("docs", "build", "changelog", "README.md")
shutil.copy(source, target)
