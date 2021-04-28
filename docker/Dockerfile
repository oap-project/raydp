FROM rayproject/ray:1.3.0

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
