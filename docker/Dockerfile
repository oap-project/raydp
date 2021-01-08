FROM rayproject/ray:91d54e

ARG PYSPARK_WHEEL_PATH
ARG RAYDP_WHEEL_PATH

RUN sudo apt-get update -y && sudo apt-get install -y openjdk-8-jdk \
    && sudo mkdir /raydp && sudo chown -R ray /raydp
WORKDIR /raydp

COPY $PYSPARK_WHEEL_PATH /raydp
COPY $RAYDP_WHEEL_PATH /raydp

RUN $HOME/anaconda3/bin/pip --no-cache-dir \
    install $(basename $PYSPARK_WHEEL_PATH) $(basename $RAYDP_WHEEL_PATH) \
    && sudo rm /raydp/$(basename $PYSPARK_WHEEL_PATH) \
    && sudo rm /raydp/$(basename $RAYDP_WHEEL_PATH)