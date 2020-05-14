#!/usr/bin/env bash

reinstall=false

if [ $1 == "true" ]; then
  reinstall=true
fi

if ${reinstall}; then
  if [ -d ~/spark-3.0.0-preview2-bin-hadoop2.7 ]; then
    rm -rf ~/spark-3.0.0-preview2-bin-hadoop2.7
  fi
fi

if ! [ -d ~/spark-3.0.0-preview2-bin-hadoop2.7 ]; then
  wget -cq https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
  tar zxf spark-3.0.0-preview2-bin-hadoop2.7.tgz
fi

pushd spark-3.0.0-preview2-bin-hadoop2.7/python/
pip install .
popd