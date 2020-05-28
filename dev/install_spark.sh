#!/usr/bin/env bash

wget -cq https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
tar zxf spark-3.0.0-preview2-bin-hadoop2.7.tgz

pushd spark-3.0.0-preview2-bin-hadoop2.7/python/
pip install .
popd
