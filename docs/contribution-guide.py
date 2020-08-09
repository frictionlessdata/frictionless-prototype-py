import os
import shutil


source = "CONTRIBUTING.md"
target = os.path.join("docs", "build", "contribution-guide", "README.md")
shutil.copy(source, target)
