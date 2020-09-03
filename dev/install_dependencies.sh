#!/usr/bin/env bash

set -ex

CURRENT_DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
DIST_PATH=${CURRENT_DIR}/../dist/

pip install torch==1.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install tensorflow==2.0.0

# build and install pyspark
${CURRENT_DIR}/build_pyspark_with_patch.sh
pip install ${DIST_PATH}/pyspark-*
export SPARK_HOME=${DIST_PATH}/spark

# build and install ray
${CURRENT_DIR}/build_ray_with_patch.sh
pip install ${DIST_PATH}/ray-0.8.7-*

set +ex
