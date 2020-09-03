#!/usr/bin/env bash

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

set -ex

if ! command -v mvn &> /dev/null
then
    echo "mvn could not be found, please install maven first"
    exit
else
    mvn_path=`which mvn`
    echo "Using ${mvn_path} for build ray java module"
fi

CURRENT_DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
DIST_PATH=${CURRENT_DIR}/../dist/
TMP_DIR=".tmp_dir"

if [ ! -d ${DIST_PATH} ];
then
  mkdir ${DIST_PATH}
fi

pushd ${CURRENT_DIR}

if [ -d ${TMP_DIR} ];
then
  rm -rf ${TMP_DIR}
fi

# create tmp dir
mkdir ${TMP_DIR}
# cd tmp dir
pushd ${TMP_DIR}

# download ray
git clone -b releases/0.8.7 --single-branch https://github.com/ray-project/ray.git

if ! command -v bazel &> /dev/null
then
    # install Bazel
    echo "bazel has not been installed, install it..."
    ray/ci/travis/install-bazel.sh
else
    bazel_path=`which bazel`
    echo "Using ${bazel_path} for build ray"
fi

if ! command -v npm &> /dev/null
then
    echo "npm could not be found, skipp build the ray dashboard"
else
    pushd ray/python/ray/dashboard/client
    npm ci
    npm run build
    popd
fi

pushd ray
# add patch
git apply --check ${CURRENT_DIR}/ray.patch
git am ${CURRENT_DIR}/ray.patch

# Build
export RAY_INSTALL_JAVA=1

pushd python
python setup.py -q bdist_wheel
popd # python

pushd java
mvn clean install -q -Dmaven.test.skip
popd # java

popd # ray

mv ray/python/dist/ray-0.8.7-* ${DIST_PATH}

popd # tmp dir
rm -rf ${TMP_DIR}
popd # ${HOME}

set +ex
