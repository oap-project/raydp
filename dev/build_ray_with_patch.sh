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

# cd home dir
pushd ${HOME}

if [ ! -d "raydp_tmp_dir" ]; then
  mkdir raydp_tmp_dir
fi

# cd raydp tmp dir
pushd raydp_tmp_dir

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
### add patch
git apply --check ../ray.patch
git am ../ray.patch

### Build
export RAY_INSTALL_JAVA=1

pushd python
python setup.py bdist_wheel
popd # python

pushd java
mvn clean install -q -Dmaven.test.skip
popd # java

popd # ray

mv ray/python/dist/ray-0.8.7-* .
rm -rf ray

popd # raydp_tmp_dir
popd # ${HOME}

set +ex
