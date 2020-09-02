#!/usr/bin/env bash

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

pushd ${TMP_DIR}

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
