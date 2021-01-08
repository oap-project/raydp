FROM rayproject/ray:91d54e

# path to pyspark wheel file
ARG PYSPARK_WHEEL_PATH
# path to raydp wheel file
ARG RAYDP_WHEEL_PATH
ARG HTTP_PROXY
ARG HTTPS_PROXY

# set http_proxy & https_proxy
ENV http_proxy=${HTTP_PROXY}
ENV https_proxy=${HTTPS_PROXY}

# install java and create workdir
RUN sudo http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} apt-get update -y \
    && sudo http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} apt-get install -y openjdk-8-jdk \
    && sudo mkdir /raydp && sudo chown -R ray /raydp
WORKDIR /raydp

COPY $PYSPARK_WHEEL_PATH /raydp
COPY $RAYDP_WHEEL_PATH /raydp

# install pyspark and raydp
RUN $HOME/anaconda3/bin/pip --no-cache-dir \
    install $(basename $PYSPARK_WHEEL_PATH) $(basename $RAYDP_WHEEL_PATH) \
    && sudo rm /raydp/$(basename $PYSPARK_WHEEL_PATH) \
    && sudo rm /raydp/$(basename $RAYDP_WHEEL_PATH)

# unset http_proxy & https_proxy
ENV http_proxy=
ENV https_proxy=
