FROM wal-g/golang:latest as build

WORKDIR /go/src/github.com/wal-g/wal-g

RUN apt-get update && \
    apt-get install --yes --no-install-recommends --no-install-suggests

COPY go.mod go.mod
COPY vendor/ vendor/
COPY internal/ internal/
COPY cmd/ cmd/
COPY main/ main/
COPY utility/ utility/

ARG use_brotli

RUN [ -z $use_brotli ] || sed -i 's|#cgo LDFLAGS: -lbrotli.*|&-static -lbrotlicommon-static -lm|' \
        vendor/github.com/google/brotli/go/cbrotli/cgo.go

RUN cd main/mysql && \
    go build -mod vendor -tags brotli -race -o wal-g -ldflags "-s -w -X main.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S`"

FROM wal-g/mysql:latest
COPY --from=build /go/src/github.com/wal-g/wal-g/main/mysql/wal-g /usr/bin

RUN mkdir /root/testtools

COPY docker/mysql_tests/scripts/ /tmp

CMD /tmp/run_integration_tests.sh
