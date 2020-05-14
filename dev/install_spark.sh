#!/usr/bin/env bash

reinstall=false

if [ $1 == "true" ]; then
  reinstall=true
fi

if ${reinstall}; then
  if [ -d ~/spark/spark-3.0.0-preview2-bin-hadoop2.7 ]; then
    rm -rf ~/spark/spark-3.0.0-preview2-bin-hadoop2.7
  fi
fi

if ! [ -d ~/spark/spark-3.0.0-preview2-bin-hadoop2.7 ]; then
  pushd ~/spark/
  wget -cq https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
  tar zxf spark-3.0.0-preview2-bin-hadoop2.7.tgz
  popd
fi

pushd ~/spark/spark-3.0.0-preview2-bin-hadoop2.7/python/
pip install .
popd
