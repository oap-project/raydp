#!/usr/bin/env bash

if ! [ -d ~/spark ]; then
  mkdir ~/spark
fi

if ${reinstall}; then
  if [ -d ~/spark/spark-3.0.0-preview2-bin-hadoop2.7 ]; then
    rm -rf ~/spark/spark-3.0.0-preview2-bin-hadoop2.7
  fi
fi

pushd ~/spark/
pwd
wget -cq https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
tar zxf spark-3.0.0-preview2-bin-hadoop2.7.tgz

pushd spark-3.0.0-preview2-bin-hadoop2.7/python/
pwd
pip install -e .
popd

popd
