#!/bin/bash

SPARK_HOME=$(python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")
RAYDP_HOME=$(python -c "import os, raydp; print(os.path.dirname(raydp.__file__))")
bin/raydp start localhost:7077
bin/raydp submit --master localhost:7077 --conf spark.executor.cores=1 \
  --conf spark.executor.instances=1 $SPARK_HOME/examples/src/main/python/pi.py
bin/raydp stop
# for debug purpose in CI
cat $SPARK_HOME/logs/*