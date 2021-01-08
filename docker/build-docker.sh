!/bin/bash

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --pyspark-wheel)
    shift
    PYSPARK_WHEEL=$1
    ;;
    --raydp-wheel)
    shift
    RAYDP_WHEEL=$1
    ;;
    *)
    echo "Usage: build-docker.sh --pyspark-wheel path-to-pyspark-wheel --raydp-wheel path-to-raydp-wheel"
    exit 1
esac
shift
done

docker build --build-arg PYSPARK_WHEEL_PATH=${PYSPARK_WHEEL} \
             --build-arg RAYDP_WHEEL_PATH=${PYSPARK_WHEEL} \
             -t intel-bigdata/raydp:latest .
