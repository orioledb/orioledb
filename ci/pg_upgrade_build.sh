#!/bin/bash
#
# Build a single PostgreSQL major version with orioledb against it.
# Usage: pg_upgrade_build.sh <pg_major> <postgres_src_dir> <install_prefix>
#
# Expects CC selection driven by $COMPILER / $LLVM_VER from the workflow.

set -eux

PG_MAJOR=$1
SRC_DIR=$2
PREFIX=$3

if [ "$COMPILER" = "clang" ]; then
	export CC=clang-$LLVM_VER
else
	export CC=gcc
fi

# Look up the orioledb-patches tag for this PG major so the
# ORIOLEDB_PATCHSET_VERSION imprint matches what ci/build.sh produces.
PGTAG=$(grep "^${PG_MAJOR}: " "$GITHUB_WORKSPACE/orioledb/.pgtags" | cut -d' ' -f2-)
export PGTAG

# Configure and build PostgreSQL.  Mirrors ci/build.sh for a debug build
# (asserts on), which is what we want for a pg_upgrade regression run.
cd "$SRC_DIR"
./configure --enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix="$PREFIX"
if printf "%s\n" "$PGTAG" | grep -v -Fqe "patches$(sed -n "/PACKAGE_VERSION='\(.*\)'/ s//\1/ p" configure | cut -d'.' -f1 )_"; then
	echo "ORIOLEDB_PATCHSET_VERSION = $PGTAG" >> src/Makefile.global
fi
make -sj "$(nproc)"
make -sj "$(nproc)" install
make -C contrib -sj "$(nproc)"
make -C contrib -sj "$(nproc)" install

if [ "$PG_MAJOR" = "17" ]; then
	make -C src/test/modules/injection_points -sj "$(nproc)" install
fi

# Build and install orioledb against this PG.  pg_config under $PREFIX
# selects the right server headers / pkglibdir.  A clean is mandatory
# between the two PG versions because the build artefacts are not
# binary-compatible across PG major versions.
cd "$GITHUB_WORKSPACE/orioledb"
PATH="$PREFIX/bin:$PATH"
export PATH

make USE_PGXS=1 IS_DEV=1 clean
make -j "$(nproc)" USE_PGXS=1 IS_DEV=1 \
	CFLAGS_SL="$(pg_config --cflags_sl) -Werror"
make -j "$(nproc)" USE_PGXS=1 IS_DEV=1 install
