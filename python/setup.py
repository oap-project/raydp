#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import glob
import io
import os
import sys
from datetime import datetime
from shutil import copy2, rmtree

from grpc_tools.command import build_package_protos
from setuptools import find_packages, setup, Command

package_name = os.getenv("RAYDP_PACKAGE_NAME", "raydp")
if package_name == 'raydp_nightly':
    VERSION = datetime.today().strftime("%Y.%m.%d.dev0")
else:
    VERSION = "0.3.0.dev0"

ROOT_DIR = os.path.dirname(__file__)

TEMP_PATH = "deps"
CORE_DIR = os.path.abspath("../core")

JARS_PATH = glob.glob(os.path.join(CORE_DIR, f"target/raydp-*.jar"))
JARS_TARGET = os.path.join(TEMP_PATH, "jars")

if len(JARS_PATH) == 0:
    print("Can't find core module jars, you need to build the jars with 'mvn clean package'"
          " under core directory first.", file=sys.stderr)
    sys.exit(-1)
else:
    JARS_PATH = JARS_PATH[0]

# build the temp dir
try:
    os.mkdir(TEMP_PATH)
    os.mkdir(JARS_TARGET)
except:
    print(f"Temp path for symlink to parent already exists {TEMP_PATH}", file=sys.stderr)
    sys.exit(-1)


class CustomBuildPackageProtos(Command):
    """Command to generate project *_pb2.py modules from proto files.

    Copied from grpc_tools.command.BuildPackageProtos and change the proto root dir.
    """

    description = 'build grpc protobuf modules'
    user_options = [('strict-mode', 's',
                     'exit with non-zero value if the proto compiling fails.')]

    def initialize_options(self):
        self.strict_mode = False

    def finalize_options(self):
        pass

    def run(self):
        # due to limitations of the proto generator, we require that only *one*
        # directory is provided as an 'include' directory. We assume it's the '' key
        # to `self.distribution.package_dir` (and get a key error if it's not
        # there).
        build_package_protos(self.distribution.package_dir["mpi_network_proto"],
                             self.strict_mode)


try:
    copy2(JARS_PATH, JARS_TARGET)

    install_requires = [
        "numpy",
        "typing",
        "pandas == 1.1.4",
        "psutil",
        "pyarrow >= 0.10",
        "ray >= 1.1.0",
        "pyspark >= 3.0.0, < 3.1.0"
    ]

    _packages = find_packages()
    _packages.append("raydp.jars")

    setup(
        name=package_name,
        version=VERSION,
        author="RayDP Developers",
        author_email="raydp-dev@googlegroups.com",
        license="Apache 2.0",
        url="https://github.com/oap-project/raydp",
        keywords="raydp spark ray distributed data-processing",
        description="RayDP: Distributed Data Processing on Ray",
        long_description=io.open(
            os.path.join(ROOT_DIR, os.path.pardir, "README.md"),
            "r",
            encoding="utf-8").read(),
        long_description_content_type="text/markdown",
        packages=_packages,
        include_package_data=True,
        package_dir={"raydp.jars": "deps/jars",
                     "mpi_network_proto": "raydp/mpi/network"},
        package_data={"raydp.jars": ["*.jar"]},
        cmdclass={
            'build_proto_modules': CustomBuildPackageProtos,
        },
        install_requires=install_requires,
        setup_requires=["grpcio-tools"],
        python_requires='>=3.6',
        classifiers=[
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8']
    )
finally:
    rmtree(os.path.join(TEMP_PATH, "jars"))
    os.rmdir(TEMP_PATH)
