import os
import shutil


source = "CONTRIBUTORS.md"
target = os.path.join("docs", "build", "contributors", "README.md")
shutil.copy(source, target)
