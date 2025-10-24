#!/bin/bash

set -eux

if [ $COMPILER = "clang" ]; then
	export CC=clang-$LLVM_VER
else
	export CC=gcc
fi

if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	sed -i.bak "s/\/\* #define USE_VALGRIND \*\//#define USE_VALGRIND/g" postgresql/src/include/pg_config_manual.h
fi

# configure & build
if [ $GITHUB_JOB = "run-benchmark" ]; then
	# Asserts slow down the benchmarking, but we still need debug symbols for
	# profiling.
	CONFIG_ARGS="--enable-debug --disable-cassert --enable-tap-tests --with-icu --prefix=$GITHUB_WORKSPACE/pgsql"
elif [ $CHECK_TYPE = "normal" ]; then
	CONFIG_ARGS="--disable-debug --disable-cassert --enable-tap-tests --with-icu --prefix=$GITHUB_WORKSPACE/pgsql"
else
	CONFIG_ARGS="--enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix=$GITHUB_WORKSPACE/pgsql"
fi

cd postgresql
./configure $CONFIG_ARGS

# Check if PGTAG looks like a full commit hash (40 hex chars)
if [[ "$PGTAG" =~ ^[0-9a-f]{40}$ ]]; then
	echo "ORIOLEDB_PATCHSET_VERSION = $PGTAG" >> src/Makefile.global
fi

make -sj `nproc`
make -sj `nproc` install
make -C contrib -sj `nproc`
make -C contrib -sj `nproc` install
cd ..

if [ $CHECK_TYPE = "static" ] && [ $COMPILER = "clang" ]; then
	sed -i.bak "s/ -Werror=unguarded-availability-new//g" pgsql/lib/pgxs/src/Makefile.global
fi

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

cd orioledb
if [ $CHECK_TYPE = "sanitize" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -fno-omit-frame-pointer -fsanitize=alignment -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=nonnull-attribute -fstack-protector" LDFLAGS_SL="-lubsan -fsanitize=address -fsanitize=undefined -lasan"
elif [ $CHECK_TYPE = "check_page" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -DCHECK_PAGE_STRUCT -DCHECK_PAGE_STATS"
elif [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage -fprofile-update=atomic -flto"
elif [ $CHECK_TYPE != "static" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage -fprofile-update=atomic"
fi
if [ $CHECK_TYPE != "static" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 install
fi
cd ..
