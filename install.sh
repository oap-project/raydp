#!/bin/bash

set -ex

current_dir=`pwd $(dirname "$0")`

if ! command -v mvn &> /dev/null
then
    echo "mvn could not be found, please install maven first"
    exit
else
    mvn_path=`which mvn`
    echo "Using ${mvn_path} for build core module"
fi

core_dir="${current_dir}/core"
pushd ${core_dir}
mvn clean package -q -DskipTests
popd

python_path=`which python`
echo "Using ${python_path} to install RayDP"
python_dir="${current_dir}/python"
pushd ${python_dir}
python setup.py install
popd

set +ex
