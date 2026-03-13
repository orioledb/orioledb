#!/bin/bash

set -eux

# Local build cache for self-hosted runners.
# Avoids rebuilding PG+OrioleDB in each matrix job.
ORIOLEDB_SHA=$(cd orioledb && git rev-parse HEAD)
CACHE_KEY="${COMPILER}-${PGTAG}-${ORIOLEDB_SHA}"
CACHE_DIR="/tmp/perf-build-cache/${CACHE_KEY}"

if [ -d "$CACHE_DIR/pgsql" ]; then
	echo "=== Restoring build from local cache ==="
	cp -a "$CACHE_DIR/pgsql" "$GITHUB_WORKSPACE/pgsql"
	exit 0
fi

echo "=== Building from scratch ==="

if [ $COMPILER = "clang" ]; then
	export CC=clang-$LLVM_VER
else
	export CC=gcc
fi

# configure & build PostgreSQL (debug symbols, no asserts)
CONFIG_ARGS="--enable-debug --disable-cassert --with-icu --prefix=$GITHUB_WORKSPACE/pgsql"

cd postgresql
./configure $CONFIG_ARGS
if printf "%s\n" "$PGTAG" | grep -v -Fqe "patches$(sed -n "/PACKAGE_VERSION='\(.*\)'/ s//\1/ p" configure | cut -d'.' -f1 )_"; then \
	echo "ORIOLEDB_PATCHSET_VERSION = $PGTAG" >> src/Makefile.global; \
fi ;
make -sj `nproc`
make -sj `nproc` install
make -C contrib -sj `nproc`
make -C contrib -sj `nproc` install
cd ..

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

# build OrioleDB (no coverage, no sanitizer, no -Werror)
cd orioledb
make -j `nproc` USE_PGXS=1
make -j `nproc` USE_PGXS=1 install
cd ..

# Save to local cache, clean up old entries (keep last 4)
mkdir -p "$CACHE_DIR"
cp -a "$GITHUB_WORKSPACE/pgsql" "$CACHE_DIR/pgsql"
find /tmp/perf-build-cache/ -maxdepth 1 -mindepth 1 -type d \
	| sort | head -n -4 | xargs -r rm -rf
