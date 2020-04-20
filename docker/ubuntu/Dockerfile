FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
ENV TERM xterm-256color

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests \
    python3.6 \
    python3-setuptools \
    python3-pip \
    git \
    pv \
    htop \
    nano && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --upgrade pip && \
    pip3 install git+https://github.com/wal-e/wal-e.git && \
    pip3 install boto

ARG use_lzo
ARG use_brotli

RUN (([ -z $use_lzo ] || (apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests \
    liblzo2-2 \
    lzop)))

RUN (([ -z $use_brotli ] || (apt-get update && apt-get install --yes --no-install-recommends --no-install-suggests \
    libbrotli1)))
