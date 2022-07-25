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

cd orioledb
if [ $CHECK_TYPE = "alignment" ]; then
	make USE_PGXS=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -fsanitize=alignment -fno-sanitize-recover=alignment" LDFLAGS_SL="-lubsan"
elif [ $CHECK_TYPE = "check_page" ]; then
	make USE_PGXS=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -DCHECK_PAGE_STRUCT"
elif [ $CHECK_TYPE != "static" ]; then
	make USE_PGXS=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage"
fi
cd ..
