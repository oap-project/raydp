FROM rayproject/ray:latest@sha256:c864e37f4ce516ff49425f69cac5503a51e84c333d30928416714a2c3da55b43

ARG HTTP_PROXY
ARG HTTPS_PROXY

# set http_proxy & https_proxy
ENV http_proxy=${HTTP_PROXY}
ENV https_proxy=${HTTPS_PROXY}

# install java, create workdir and install raydp
# You could change the raydp to raydp-nightly if you want to try the master branch code
RUN sudo http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} apt-get update -y \
    && sudo http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} apt-get install -y openjdk-8-jdk \
    && sudo mkdir /raydp \
    && sudo chown -R ray /raydp \
    && $HOME/anaconda3/bin/pip --no-cache-dir install raydp

WORKDIR /raydp

# unset http_proxy & https_proxy
ENV http_proxy=
ENV https_proxy=
