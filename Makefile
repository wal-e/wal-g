MAIN_PG_PATH := main/pg
MAIN_MYSQL_PATH := main/mysql
MAIN_REDIS_PATH := main/redis
MAIN_MONGO_PATH := main/mongo
DOCKER_COMMON := golang ubuntu s3
CMD_FILES = $(wildcard wal-g/*.go)
PKG_FILES = $(wildcard internal/**/*.go internal/**/**/*.go internal/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g
COVERAGE_FILE := coverage.out
TEST := "pg_tests"

MONGO_MAJOR ?= "4.2"
MONGO_VERSION ?= "4.2.1"

WALG_COMPRESSION_METHOD:=lz4
BUILD_TAGS:=

ifdef USE_LZO
	BUILD_TAGS:=$(BUILD_TAGS) lzo
	WALG_COMPRESSION_METHOD:=lzo
endif

ifdef USE_BROTLI
	BUILD_TAGS:=$(BUILD_TAGS) brotli
	WALG_COMPRESSION_METHOD:=brotli
endif

ifdef USE_LIBSODIUM
	BUILD_TAGS:=$(BUILD_TAGS) libsodium
endif

.PHONY: unittest fmt lint install clean

test: install deps lint unittest pg_build mysql_build redis_build mongo_build unlink_brotli pg_integration_test mysql_integration_test redis_integration_test

pg_test: install deps pg_build lint unlink_brotli pg_integration_test

pg_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PG_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/pg.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/pg.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/pg.WalgVersion=`git tag -l --points-at HEAD`")

install_and_build_pg: install deps pg_build

pg_build_image:
	docker-compose build --build-arg build_tags=$(BUILD_TAGS) --build-arg use_brotli=$(USE_BROTLI) --build-arg use_libsodium=$(USE_LIBSODIUM) --build-arg use_lzo=$(USE_LZO) --build-arg walg_compression_method=$(WALG_COMPRESSION_METHOD) $(DOCKER_COMMON) pg pg_build_docker_prefix

pg_save_image: install_and_build_pg pg_build_image
	mkdir -p ${CACHE_FOLDER}
	sudo rm -rf ${CACHE_FOLDER}/*
	docker save ${IMAGE} | gzip -c > ${CACHE_FILE_DOCKER_PREFIX}
	docker save ${IMAGE_UBUNTU} | gzip -c > ${CACHE_FILE_UBUNTU}
	docker save ${IMAGE_GOLANG} | gzip -c > ${CACHE_FILE_GOLANG}
	ls ${CACHE_FOLDER}

pg_integration_test:
	@if [ "x" = "${CACHE_FILE_DOCKER_PREFIX}x" ]; then\
		echo "Rebuild";\
		make install_and_build_pg;\
		make pg_build_image;\
	else\
		docker load -i ${CACHE_FILE_DOCKER_PREFIX};\
	fi
	docker-compose build --build-arg build_tags=$(BUILD_TAGS) --build-arg use_brotli=$(USE_BROTLI) --build-arg use_libsodium=$(USE_LIBSODIUM) --build-arg use_lzo=$(USE_LZO) --build-arg walg_compression_method=$(WALG_COMPRESSION_METHOD) $(TEST)
	docker-compose up --exit-code-from $(TEST) $(TEST)

all_unittests: install deps lint unittest

pg_int_tests_only:
	docker-compose build pg_tests
	docker-compose up --exit-code-from pg_tests pg_tests
	
pg_clean:
	(cd $(MAIN_PG_PATH) && go clean)
	./cleanup.sh

pg_install: pg_build
	mv $(MAIN_PG_PATH)/wal-g $(GOBIN)/wal-g

mysql_test: install deps mysql_build lint unlink_brotli mysql_integration_test

mysql_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MYSQL_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/mysql.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/mysql.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/mysql.WalgVersion=`git tag -l --points-at HEAD`")

load_docker_common:
	@if [ "x" = "${CACHE_FILE_UBUNTU}x" ]; then\
		echo "Rebuild";\
		docker-compose build $(DOCKER_COMMON);\
	else\
		docker load -i ${CACHE_FILE_UBUNTU};\
		docker load -i ${CACHE_FILE_GOLANG};\
	fi

mysql_integration_test: load_docker_common
	docker-compose build mysql mysql_tests
	docker-compose up --exit-code-from mysql_tests mysql_tests

mysql_clean:
	(cd $(MAIN_MYSQL_PATH) && go clean)
	./cleanup.sh

mysql_install: mysql_build
	mv $(MAIN_MYSQL_PATH)/wal-g $(GOBIN)/wal-g

mongo_test: install deps mongo_build lint unlink_brotli

mongo_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MONGO_PATH) && go build $(BUILD_ARGS) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/mongo.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/mongo.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/mongo.WalgVersion=`git tag -l --points-at HEAD`")

mongo_install: mongo_build
	mv $(MAIN_MONGO_PATH)/wal-g $(GOBIN)/wal-g

mongo_features:
	set -e
	make go_deps
	cd tests_func/ && MONGO_MAJOR=$(MONGO_MAJOR) MONGO_VERSION=$(MONGO_VERSION) go test -v -count=1 -timeout 20m  -tf.test=true -tf.debug=false -tf.clean=true -tf.stop=true

clean_mongo_features:
	set -e
	cd tests_func/ && MONGO_MAJOR=$(MONGO_MAJOR) MONGO_VERSION=$(MONGO_VERSION) go test -v -count=1  -timeout 5m -tf.test=false -tf.debug=false -tf.clean=true -tf.stop=true

redis_test: install deps redis_build lint unlink_brotli redis_integration_test

redis_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_REDIS_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/redis.BuildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/redis.GitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/redis.WalgVersion=`git tag -l --points-at HEAD`")

redis_integration_test: load_docker_common
	docker-compose build redis redis_tests
	docker-compose up --exit-code-from redis_tests redis_tests

redis_clean:
	(cd $(MAIN_REDIS_PATH) && go clean)
	./cleanup.sh

redis_install: redis_build
	mv $(MAIN_REDIS_PATH)/wal-g $(GOBIN)/wal-g

unittest:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -v $(TEST_MODIFIER) ./internal/
	go test -v $(TEST_MODIFIER) ./internal/compression/
	go test -v $(TEST_MODIFIER) ./internal/crypto/openpgp/
	go test -v $(TEST_MODIFIER) ./internal/crypto/awskms/
	@if [ ! -z "${USE_LIBSODIUM}" ]; then\
		go test -v $(TEST_MODIFIER) -tags libsodium ./internal/crypto/libsodium/;\
	fi
	go test -v $(TEST_MODIFIER) ./internal/databases/mysql
	go test -v $(TEST_MODIFIER) ./internal/databases/mongo/...
	go test -v $(TEST_MODIFIER) ./internal/walparser/
	go test -v $(TEST_MODIFIER) ./utility

coverage:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go test -v $(TEST_MODIFIER) -coverprofile=$(COVERAGE_FILE) | grep -v 'no test files'
	go tool cover -html=$(COVERAGE_FILE)

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

lint: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs golint

deps: go_deps link_external_deps

go_deps:
	git submodule update --init
	go mod vendor
ifdef USE_LZO
	sed -i 's|\(#cgo LDFLAGS:\) .*|\1 -Wl,-Bstatic -llzo2 -Wl,-Bdynamic|' vendor/github.com/cyberdelia/lzo/lzo.go
endif

link_external_deps: link_brotli link_libsodium

unlink_external_deps: unlink_brotli unlink_libsodium

install:
	go get -u golang.org/x/lint/golint

link_brotli:
	@if [ ! -z "${USE_BROTLI}" ]; then\
		./link_brotli.sh;\
	fi

link_libsodium:
	@if [ ! -z "${USE_LIBSODIUM}" ]; then\
		./link_libsodium.sh;\
	fi

unlink_brotli:
	@if [ ! -z "${USE_BROTLI}" ]; then\
		rm -rf vendor/github.com/google/brotli/*;\
		mv tmp/brotli/* vendor/github.com/google/brotli/;\
		rm -rf tmp/brotli;\
	fi

unlink_libsodium:
	rm -rf tmp/libsodium
