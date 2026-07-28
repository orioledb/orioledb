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

# pg_tests run TAP suites (recovery/041, /046, /047, …) that skip without
# injection-point support, so opt in for that check type.
if [ $CHECK_TYPE = "pg_tests" ] || [ $CHECK_TYPE = "pg_tests_asan" ]; then
	CONFIG_ARGS="$CONFIG_ARGS --enable-injection-points"
fi

cd postgresql
./configure $CONFIG_ARGS
if printf "%s\n" "$PGTAG" | grep -v -Fqe "patches$(sed -n "/PACKAGE_VERSION='\(.*\)'/ s//\1/ p" configure | cut -d'.' -f1 )_"; then \
	echo "ORIOLEDB_PATCHSET_VERSION = $PGTAG" >> src/Makefile.global; \
fi ;
make -sj `nproc`
make -sj `nproc` install
make -C contrib -sj `nproc`
make -C contrib -sj `nproc` install

if [ $PG_VERSION = "17" ]; then
	make -C src/test/modules/injection_points -sj `nproc` install
fi
cd ..

if [ $CHECK_TYPE = "static" ] && [ $COMPILER = "clang" ]; then
	sed -i.bak "s/ -Werror=unguarded-availability-new//g" pgsql/lib/pgxs/src/Makefile.global
fi

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

cd orioledb
if [ $CHECK_TYPE = "sanitize" ] || [ $CHECK_TYPE = "pg_tests_asan" ]; then
	# pg_tests_asan builds orioledb with ASAN/UBSAN (like sanitize) but runs the
	# pg_tests churn workload, so the rare memory-corruption bugs the stress hunt
	# surfaces (descr-invalidation MAXALIGN, iterator ordering) are caught at the
	# first bad access with a precise alloc/free/access trace.  For pg_tests_asan
	# also enable -DCHECK_PAGE_STRUCT (o_check_page_struct(NULL, p), no descriptor,
	# no allocation) so page corruption is caught at the modification that
	# introduces it, not later at page_split_chunk.  NOT -DCHECK_PAGE_STATS: its
	# unlock_check_page path fetches the index descriptor, which allocates / grows
	# a hash table under the page's critical section -- illegal, and frequent
	# under the churn's DDL-driven descriptor cache misses.
	EXTRA_CFLAGS=""
	if [ $CHECK_TYPE = "pg_tests_asan" ]; then
		EXTRA_CFLAGS="-DCHECK_PAGE_STRUCT"
	fi
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -fno-omit-frame-pointer -fsanitize=alignment -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fno-sanitize=nonnull-attribute -fstack-protector $EXTRA_CFLAGS" LDFLAGS_SL="-lubsan -fsanitize=address -fsanitize=undefined -lasan"
elif [ $CHECK_TYPE = "check_page" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -DCHECK_PAGE_STRUCT -DCHECK_PAGE_STATS"
elif [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage -fprofile-update=atomic -flto"
elif [ $CHECK_TYPE = "pg_tests" ]; then
	# The stress-hunt churn: enable -DCHECK_PAGE_STRUCT (o_check_page_struct(NULL,
	# p) -- no descriptor, no allocation) so page corruption (e.g. the recovery
	# page_split_chunk assert) is caught early, at the modification that corrupts
	# the page rather than a later split/read.  NOT -DCHECK_PAGE_STATS: it fetches
	# the index descriptor in unlock_check_page and allocates under the page's
	# critical section, which crashes constantly under the churn's DDL cache misses.
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage -fprofile-update=atomic -DCHECK_PAGE_STRUCT"
elif [ $CHECK_TYPE != "static" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 CFLAGS_SL="$(pg_config --cflags_sl) -Werror -coverage -fprofile-update=atomic"
fi
if [ $CHECK_TYPE != "static" ]; then
	make -j `nproc` USE_PGXS=1 IS_DEV=1 install
fi
cd ..
