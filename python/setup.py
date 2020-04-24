from setuptools import find_packages
from setuptools import setup

install_requires = [
    "pandas",
    "psutil",
    "pyarrow >= 0.10",
    "pyspark >= 3.0.0.dev2",
    "ray",
]

setup(
    name="raydp",
    version="0.1",
    description="RayDP: Distributed Data Processing on Ray",
    packages=find_packages(where="."),
    install_requires=install_requires
)

