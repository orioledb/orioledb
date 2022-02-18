#!/bin/bash

set -eu

if [ $COMPILER = "clang" ]; then
	export CC=clang-$LLVM_VER
else
	export CC=gcc
fi

if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	sed -i.bak "s/\/\* #define USE_VALGRIND \*\//#define USE_VALGRIND/g" postgresql/src/include/pg_config_manual.h
fi

# configure & build
if [ $CHECK_TYPE = "normal" ]; then
	CONFIG_ARGS="--disable-debug --disable-cassert --enable-tap-tests --with-icu --prefix=$GITHUB_WORKSPACE/pgsql"
else
	CONFIG_ARGS="--enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix=$GITHUB_WORKSPACE/pgsql"
fi

cd postgresql
./configure $CONFIG_ARGS
make -sj4
make -sj4 install
cd ..

if [ $CHECK_TYPE = "static" ] && [ $COMPILER = "clang" ]; then
	sed -i.bak "s/ -Werror=unguarded-availability-new//g" pgsql/lib/pgxs/src/Makefile.global
fi

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

if [ $CHECK_TYPE = "alignment" ]; then
	cd orioledb
	make USE_PGXS=1 CFLAGS_SL="$(pg_config --cflags_sl) -fsanitize=alignment -fno-sanitize-recover=alignment" LDFLAGS_SL="-lubsan"
	cd ..
elif [ $CHECK_TYPE != "static" ]; then
	cd orioledb
	make USE_PGXS=1 CFLAGS_SL="$(pg_config --cflags_sl) -coverage"
	cd ..
fi
