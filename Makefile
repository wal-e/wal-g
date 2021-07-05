MAIN_PG_PATH := main/pg
MAIN_MYSQL_PATH := main/mysql
MAIN_SQLSERVER_PATH := main/sqlserver
MAIN_REDIS_PATH := main/redis
MAIN_MONGO_PATH := main/mongo
MAIN_FDB_PATH := main/fdb
MAIN_ROCKSDB_PATH := main/rocksdb
DOCKER_COMMON := golang ubuntu s3
CMD_FILES = $(wildcard cmd/**/*.go)
PKG_FILES = $(wildcard internal/**/*.go internal/**/**/*.go internal/*.go)
TEST_FILES = $(wildcard test/*.go testtools/*.go)
PKG := github.com/wal-g/wal-g
COVERAGE_FILE := coverage.out
TEST := "pg_tests"
MYSQL_TEST := "mysql_base_tests"
MONGO_MAJOR ?= "4.2"
MONGO_VERSION ?= "4.2.8"
GOLANGCI_LINT_VERSION ?= "v1.37.0"
REDIS_VERSION ?= "5.0.8"

BUILD_TAGS:=brotli

ifdef USE_LIBSODIUM
	BUILD_TAGS:=$(BUILD_TAGS) libsodium
endif

ifdef USE_LZO
	BUILD_TAGS:=$(BUILD_TAGS) lzo
endif

.PHONY: unittest fmt lint clean

test: deps unittest pg_build mysql_build redis_build mongo_build unlink_brotli pg_integration_test mysql_integration_test redis_integration_test fdb_integration_test

pg_test: deps pg_build unlink_brotli pg_integration_test

pg_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_PG_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/pg.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/pg.gitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/pg.walgVersion=`git tag -l --points-at HEAD`")

install_and_build_pg: deps pg_build

pg_build_image:
	docker-compose build $(DOCKER_COMMON) pg pg_build_docker_prefix

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
	docker-compose build $(TEST)
	docker-compose up --exit-code-from $(TEST) $(TEST)

all_unittests: deps unittest

pg_int_tests_only:
	docker-compose build pg_tests
	docker-compose up --exit-code-from pg_tests pg_tests

pg_clean:
	(cd $(MAIN_PG_PATH) && go clean)
	./cleanup.sh

pg_install: pg_build
	mv $(MAIN_PG_PATH)/wal-g $(GOBIN)/wal-g

mysql_base: deps mysql_build unlink_brotli
mysql_test: deps mysql_build unlink_brotli mysql_integration_test

mysql_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MYSQL_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/mysql.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/mysql.gitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/mysql.walgVersion=`git tag -l --points-at HEAD`")

sqlserver_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_SQLSERVER_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/sqlserver.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/sqlserver.gitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/sqlserver.walgVersion=`git tag -l --points-at HEAD`")

load_docker_common:
	@if [ "x" = "${CACHE_FILE_UBUNTU}x" ]; then\
		echo "Rebuild";\
		docker-compose build $(DOCKER_COMMON);\
	else\
		docker load -i ${CACHE_FILE_UBUNTU};\
		docker load -i ${CACHE_FILE_GOLANG};\
	fi

mysql_integration_test: deps mysql_build unlink_brotli load_docker_common
	./link_brotli.sh
	docker-compose build mysql $(MYSQL_TEST)
	docker-compose up --force-recreate --exit-code-from $(MYSQL_TEST) $(MYSQL_TEST)

mysql_clean:
	(cd $(MAIN_MYSQL_PATH) && go clean)
	./cleanup.sh

mysql_install: mysql_build
	mv $(MAIN_MYSQL_PATH)/wal-g $(GOBIN)/wal-g

mariadb_test: deps mysql_build unlink_brotli mariadb_integration_test

mariadb_integration_test: load_docker_common
	docker-compose build mariadb mariadb_tests
	docker-compose up --force-recreate --exit-code-from mariadb_tests mariadb_tests

mongo_test: deps mongo_build unlink_brotli

mongo_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_MONGO_PATH) && go build $(BUILD_ARGS) -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/mongo.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/mongo.gitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/mongo.walgVersion=`git tag -l --points-at HEAD`")

mongo_install: mongo_build
	mv $(MAIN_MONGO_PATH)/wal-g $(GOBIN)/wal-g

mongo_features:
	set -e
	make go_deps
	cd tests_func/ && MONGO_MAJOR=$(MONGO_MAJOR) MONGO_VERSION=$(MONGO_VERSION) go test -v -count=1 -timeout 20m  -tf.test=true -tf.debug=false -tf.clean=true -tf.stop=true -tf.database=mongodb

clean_mongo_features:
	set -e
	cd tests_func/ && MONGO_MAJOR=$(MONGO_MAJOR) MONGO_VERSION=$(MONGO_VERSION) go test -v -count=1  -timeout 5m -tf.test=false -tf.debug=false -tf.clean=true -tf.stop=true -tf.database=mongodb

fdb_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_FDB_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w")

fdb_install: fdb_build
	mv $(MAIN_FDB_PATH)/wal-g $(GOBIN)/wal-g

fdb_integration_test: load_docker_common
	docker-compose down -v
	docker-compose build fdb_tests
	docker-compose up --force-recreate --renew-anon-volumes --exit-code-from fdb_tests fdb_tests

redis_test: deps redis_build unlink_brotli redis_integration_test

redis_build: $(CMD_FILES) $(PKG_FILES)
	(cd $(MAIN_REDIS_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w -X github.com/wal-g/wal-g/cmd/redis.buildDate=`date -u +%Y.%m.%d_%H:%M:%S` -X github.com/wal-g/wal-g/cmd/redis.gitRevision=`git rev-parse --short HEAD` -X github.com/wal-g/wal-g/cmd/redis.walgVersion=`git tag -l --points-at HEAD`")

redis_integration_test: load_docker_common
	docker-compose build redis redis_tests
	docker-compose up --exit-code-from redis_tests redis_tests

redis_clean:
	(cd $(MAIN_REDIS_PATH) && go clean)
	./cleanup.sh

redis_install: redis_build
	mv $(MAIN_REDIS_PATH)/wal-g $(GOBIN)/wal-g

redis_features:
	set -e
	make go_deps
	cd tests_func/ && REDIS_VERSION=$(REDIS_VERSION) go test -v -count=1 -timeout 20m  -tf.test=true -tf.debug=false -tf.clean=true -tf.stop=true -tf.database=redis

rocksdb_build:
	./link_rocksdb.sh
	(cd $(MAIN_ROCKSDB_PATH) && go build -mod vendor -tags "$(BUILD_TAGS)" -o wal-g -ldflags "-s -w")

clean_redis_features:
	set -e
	cd tests_func/ && REDIS_VERSION=$(REDIS_VERSION) go test -v -count=1  -timeout 5m -tf.test=false -tf.debug=false -tf.clean=true -tf.stop=true -tf.database=redis


unittest:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/compression/
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/crypto/openpgp/
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/crypto/awskms/
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/abool
	@if [ ! -z "${USE_LIBSODIUM}" ]; then\
		go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/crypto/libsodium/;\
	fi
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/databases/mysql
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/databases/mongo/...
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/databases/postgres
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./internal/walparser/
	go test -mod vendor -v $(TEST_MODIFIER) -tags "$(BUILD_TAGS)" ./utility

coverage:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go test -v $(TEST_MODIFIER) -coverprofile=$(COVERAGE_FILE) | grep -v 'no test files'
	go tool cover -html=$(COVERAGE_FILE)

fmt: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	gofmt -s -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

goimports: $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)
	goimports -w $(CMD_FILES) $(PKG_FILES) $(TEST_FILES)

lint:
	@#Github Actions
	@if [ "$(shell command -v golangci-lint)" = "" ] && [ "$(GITHUB_WORKFLOW)" != "" ]; then curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sudo sh -s -- -b $(go env GOPATH)/bin $(GOLANGCI_LINT_VERSION); fi;
	@#MacOS (brew)
	@if [ "$(shell command -v golangci-lint)" = "" ] && [ "$(shell command -v brew)" != "" ]; then brew install golangci-lint; fi;
	@#Linux (has sudo)
	@if [ "$(shell command -v golangci-lint)" = "" ]; then curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s $(GOLANGCI_LINT_VERSION) && sudo cp ./bin/golangci-lint $(go env GOPATH)/bin/; fi;
	@echo "running golangci-lint..."
	@golangci-lint run

docker_lint:
	docker build -t wal-g/lint - < docker/lint/Dockerfile
	docker run --rm -v `pwd`:/app wal-g/lint golangci-lint run -v

deps: go_deps link_external_deps

go_deps:
	git submodule update --init

	mkdir -p tmp/rocksdb
	cp -rf internal/databases/rocksdb/* tmp/rocksdb/

	go mod vendor
ifdef USE_LZO
	sed -i 's|\(#cgo LDFLAGS:\) .*|\1 -Wl,-Bstatic -llzo2 -Wl,-Bdynamic|' vendor/github.com/cyberdelia/lzo/lzo.go
endif

link_external_deps: link_brotli link_libsodium

unlink_external_deps: unlink_brotli unlink_libsodium

install:
	@echo "Nothing to be done. Use pg_install/mysql_install/mongo_install/fdb_install/... instead."

link_brotli:
	./link_brotli.sh

link_libsodium:
	@if [ ! -z "${USE_LIBSODIUM}" ]; then\
		./link_libsodium.sh;\
	fi

unlink_brotli:
	rm -rf vendor/github.com/google/brotli/*
	mv tmp/brotli/* vendor/github.com/google/brotli/
	rm -rf tmp/brotli

unlink_libsodium:
	rm -rf tmp/libsodium
