import glob
import os
import sys
from shutil import copyfile, rmtree

from setuptools import find_packages
from setuptools import setup

VERSION = "0.1.dev0"

TEMP_PATH = "deps"
CORE_DIR = os.path.abspath("../core")

JARS_PATH = glob.glob(os.path.join(CORE_DIR, f"target/"))
JARS_TARGET = os.path.join(TEMP_PATH, "jars")

if len(JARS_PATH) == 0:
    print("Can't find core module jars, you need to build the jars with 'mvn clean package'"
          " under core directly first.", file=sys.stderr)
    sys.exit(-1)
else:
    JARS_PATH = JARS_PATH[0]

# build the temp dir
try:
    os.mkdir(TEMP_PATH)
except:
    print(f"Temp path for symlink to parent already exists {TEMP_PATH}", file=sys.stderr)
    sys.exit(-1)


def _supports_symlinks():
    """Check if the system supports symlinks (e.g. *nix) or not."""
    return getattr(os, "symlink", None) is not None

try:
    # TODO: maybe we should package the spark jars as well
    if _supports_symlinks():
        os.symlink(JARS_PATH, JARS_TARGET)
    else:
        copyfile(JARS_PATH, JARS_TARGET)

    install_requires = [
        "pandas",
        "psutil",
        "pyarrow >= 0.10",
        "pyspark == 3.0.0",
        "ray",
        "pyjnius"
    ]

    with open('../README.md') as f:
        long_description = f.read()

    setup(
        name="raydp",
        version=VERSION,
        description="RayDP: Distributed Data Processing on Ray",
        long_description=long_description,
        long_description_content_type="text/markdown",
        packages=find_packages(),
        include_package_data=True,
        package_dir={"raydp.jars": "deps/jars"},
        install_requires=install_requires
    )
finally:
    if _supports_symlinks():
        os.remove(os.path.join(TEMP_PATH, "jars"))
    else:
        rmtree(os.path.join(TEMP_PATH, "jars"))
    os.rmdir(TEMP_PATH)
