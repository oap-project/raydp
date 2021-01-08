FROM rayproject/ray:91d54e

ARG PYSPARK_WHEEL_PATH
ARG RAYDP_WHEEL_PATH

RUN sudo mkdir /raydp && sudo chown -R ray /raydp
WORKDIR /raydp

COPY $PYSPARK_WHEEL_PATH .
COPY $RAYDP_WHEEL_PATH .

RUN $HOME/anaconda3/bin/pip --no-cache-dir install $(basename $PYSPARK_WHEEL_PATH) \
    $(basename $RAYDP_WHEEL_PATH) \
    && sudo rm $(basename $PYSPARK_WHEEL_PATH) && sudo rm $(basename $RAYDP_WHEEL_PATH)