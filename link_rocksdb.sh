#!/bin/sh


readonly CWD=$PWD

cd submodules/rocksdb
echo "Building rocksdb. It take a few time"
make static_lib

echo "Rocksdb library has been built"
LDFLAGS=$(grep -oP 'PLATFORM_LDFLAGS=(.+)' make_config.mk | sed 's/^PLATFORM_LDFLAGS=\s\?//')" -lm -lstdc++"
echo "LDFLAGS is $LDFLAGS"

cd ${CWD}
cd tmp/rocksdb

# patch cgo.go to force usage of static libraries for linking
sed -i -e 's/\#cgo CFLAGS:.*/\#cgo CFLAGS: -I..\/..\/submodules\/rocksdb\/include/' rocksdb.go
sed -i -e "s/\#cgo LDFLAGS:.*/\#cgo LDFLAGS: -L..\/..\/submodules\/rocksdb -lrocksdb $LDFLAGS/" rocksdb.go

cd ${CWD}
