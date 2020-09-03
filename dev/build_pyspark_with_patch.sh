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
    echo "Using ${mvn_path} for build Spark"
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
git clone -b branch-3.0 --single-branch https://github.com/apache/spark.git

pushd spark

git reset --hard 3fdfce3120f307147244e5eaf46d61419a723d50

# add patch
git apply --check ${CURRENT_DIR}/spark.patch
git am ${CURRENT_DIR}/spark.patch

# build spark
mvn clean package -q -DskipTests

# build pyspark
pushd python
python setup.py -q bdist_wheel
popd # python

popd # spark

# copy the build dist to the given dir
cp spark/python/dist/pyspark-* ${DIST_PATH}
# mv the build spark to the given dir
mv spark ${DIST_PATH}

popd # tmp dir
rm -rf ${TMP_DIR}
popd

set +ex
