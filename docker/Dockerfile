FROM rayproject/ray:1.2.0

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

COPY $RAYDP_WHEEL_PATH /raydp

# install raydp
RUN $HOME/anaconda3/bin/pip --no-cache-dir \
    install raydp

# unset http_proxy & https_proxy
ENV http_proxy=
ENV https_proxy=
