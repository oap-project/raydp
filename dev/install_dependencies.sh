#!/usr/bin/env bash

set -ex

pip install torch==1.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install tensorflow==2.0.0

RAYDP_TMP_DIR=${HOME}/raydp_tmp_dir

dev/build_pyspark_with_patch.sh
pip install ${RAYDP_TMP_DIR}/pyspark-*
dev/build_ray_with_patch.sh
pip install ${RAYDP_TMP_DIR}/ray-0.8.7-*

set +ex
