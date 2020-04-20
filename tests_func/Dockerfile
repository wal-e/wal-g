# vim:set ft=dockerfile:
FROM walg-func-test-base

ARG MONGO_PACKAGE=mongodb-org
ARG MONGO_REPO=repo.mongodb.org
ARG WALG_REPO=${GOPATH}/src/github.com/wal-g/wal-g

ENV MONGO_PACKAGE=${MONGO_PACKAGE} MONGO_REPO=${MONGO_REPO}
ARG MONGO_MAJOR=4.2
ARG MONGO_VERSION=4.2.3
ENV TMP_DIR /var/tmp/wal-g
ENV WALG_CONF_DIR /etc/wal-g/

RUN echo "deb http://$MONGO_REPO/apt/ubuntu xenial/${MONGO_PACKAGE%-unstable}/$MONGO_MAJOR multiverse" | tee "/etc/apt/sources.list.d/${MONGO_PACKAGE%-unstable}.list"
RUN apt-get update -qq && \
    apt-get install -y apt-transport-https tzdata && \
    mkdir -p /etc/apt/sources.list.d && \
    apt-get update -qq && \
    apt-get install --allow-unauthenticated -y \
        ${MONGO_PACKAGE}-server=$MONGO_VERSION \
        ${MONGO_PACKAGE}-shell=$MONGO_VERSION \
        ${MONGO_PACKAGE}-tools=$MONGO_VERSION && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/lib/mongodb/* && \
    apt-get clean

RUN mkdir -p /home/mongodb /var/log/wal-g/ ${TMP_DIR} && \
    chown mongodb:mongodb /home/mongodb /var/log/wal-g/ && \
    ln --force -s /config/supervisor/conf.d/mongodb.conf /etc/supervisor/conf.d/mongodb.conf && \
    mkdir -p ${WALG_REPO} ${WALG_CONF_DIR} && \
    mkdir -p /home/mongodb/.gnupg && touch /home/mongodb/.gnupg/gpg.conf && \
    chmod -R 700 /home/mongodb/.gnupg && chown mongodb:mongodb -R /home/mongodb/.gnupg && \
    ln --force -s /config/wal-g-${MONGO_MAJOR}.json ${WALG_CONF_DIR}/wal-g.json && \
    ln --force -s /config/gpg-key.armor ${WALG_CONF_DIR}/gpg-key.armor

COPY staging/wal-g ${WALG_REPO}/
WORKDIR ${WALG_REPO}

ENV WALG_COMPRESSION_METHOD brotli
RUN USE_BROTLI=1 make link_external_deps
RUN make BUILD_ARGS=-race mongo_build && \
    cp main/mongo/wal-g /usr/bin/wal-g
