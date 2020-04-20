from setuptools import find_packages
from setuptools import setup

setup(
    name="spark-on-ray",
    version="0.1",
    description="Running Spark on Ray",
    packages=find_packages(where=".")
)

