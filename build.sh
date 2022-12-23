#!/bin/bash

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
    echo "Using ${mvn_path} for build core module"
fi

CURRENT_DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
DIST_PATH=${CURRENT_DIR}/dist/

if [[ ! -d ${DIST_PATH} ]];
then
  mkdir ${DIST_PATH}
fi

# build core part
CORE_DIR="${CURRENT_DIR}/core"
pushd ${CORE_DIR}
if [[ -z $GITHUB_CI ]];
then
  mvn clean package -q -DskipTests
else
  mvn verify -q
fi
popd # core dir

# build python part
RAYDP_PACKAGE_NAME=${RAYDP_PACKAGE_NAME:-raydp}
PYTHON_DIR="${CURRENT_DIR}/python"

if [[ -d "${PYTHON_DIR}/build" ]];
then
  rm -rf "${PYTHON_DIR}/build"
fi

pushd ${PYTHON_DIR}
python setup.py bdist_wheel
cp ${PYTHON_DIR}/dist/${RAYDP_PACKAGE_NAME}-* ${DIST_PATH}
popd # python dir

set +ex
