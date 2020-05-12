#!/usr/bin/env bash

wget -cq https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
tar zxf spark-3.0.0-preview2-bin-hadoop2.7.tgz
pushd spark-3.0.0-preview2-bin-hadoop2.7/python/
pip install .
popd

pip install koalas numpy pandas pyarrow pytest ray typing
pip install torch==1.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
