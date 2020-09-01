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

# cd home dir
pushd ${HOME}

if [ ! -d "raydp_tmp_dir" ]; then
  mkdir raydp_tmp_dir
fi

# cd raydp tmp dir
pushd raydp_tmp_dir

# download ray
git clone -b branch-3.0 --single-branch https://github.com/apache/spark.git

pushd spark

git reset --hard 3fdfce3120f307147244e5eaf46d61419a723d50

### add patch
git apply --check ../spark.patch
git am ../spark.patch

# build spark
mvn clean package -DskipTests

# build pyspark
pushd python
python setup.py. bdist_wheel
popd # python

popd # spark

mv spark/python/dist/pyspark-* .
rm -rf spark

popd # raydp_tmp_dir
popd # ${HOME}

set +ex
