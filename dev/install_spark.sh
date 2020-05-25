#!/usr/bin/env bash

reinstall=false

if [ $1 == "true" ]; then
  reinstall=true
fi

if ! [ -d ~/spark ]; then
  mkdir ~/spark
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

  pushd spark-3.0.0-preview2-bin-hadoop2.7
  ./build/mvn -DskipTests clean package

  pushd python
  python setup.py sdist
  popd # python

  popd # spark-3.0.0-preview2-bin-hadoop2.7
  popd # spark
fi

pushd ~/spark/spark-3.0.0-preview2-bin-hadoop2.7/python/
python -m pip install dist/pyspark-3.1.0.dev0.tar.gz
popd
