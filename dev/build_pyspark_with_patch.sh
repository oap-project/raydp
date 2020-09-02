#!/usr/bin/env bash

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
python setup.py bdist_wheel
popd # python

popd # spark

# copy the build dist to the given dir
copy spark/python/dist/pyspark-* ${DIST_PATH}
# mv the build spark to the given dir
mv spark ${DIST_PATH}

popd # tmp dir
rm -rf ${TMP_DIR}
popd

set +ex
