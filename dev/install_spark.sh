#!/usr/bin/env bash

if ! [ -d ~/spark ]; then
  mkdir ~/spark
fi

if ${reinstall}; then
  if [ -d ~/spark/spark-3.0.0-bin-hadoop2.7 ]; then
    rm -rf ~/spark/spark-3.0.0-bin-hadoop2.7
  fi
fi

pushd ~/spark/
pwd
wget -cq https://www.apache.org/dyn/closer.lua/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
tar zxf spark-3.0.0-bin-hadoop2.7.tgz

pushd spark-3.0.0-bin-hadoop2.7.tgz/python/
pwd
pip install -e .
popd

popd
