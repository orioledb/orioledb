#!/bin/bash

set -eu
export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

status=0

cd orioledb

if [ $COMPILER = "clang" ]; then
	export CC=clang-$LLVM_VER
else
	export CC=gcc
fi

if [ "$COMPILER" = "clang" ]; then
	scan-build-$LLVM_VER --status-bugs \
		-disable-checker deadcode.DeadStores \
		make USE_PGXS=1 IS_DEV=1 USE_ASSERT_CHECKING=1 || status=$?

elif [ "$COMPILER" = "gcc" ]; then
	ARCH="$(uname -m)"
	CPPCHK_DEFS=()

	case "$ARCH" in
		x86_64|amd64)
			CPPCHK_DEFS+=("-D__x86_64__=1")
			;;
		aarch64|arm64)
			CPPCHK_DEFS+=("-D__aarch64__=1")
			;;
		armv7l|armv6l)
			CPPCHK_DEFS+=("-D__arm__=1" "-D__arm=1")
			;;
		*)
			echo "Unknown arch: $ARCH"
			;;
	esac

	CPPCHK_DEFS+=("-DPG_MAJORVERSION=${PG_VERSION}")

	# Collect all orioledb's include paths recursively
	ORIOLEDB_INCLUDE_DIRS=$(find include -type d)

	# Collect all PostgreSQL include paths recursively
	PG_INCLUDE_DIRS=$(find "$(pg_config --includedir-server)" -type d)

	# Combine and convert to -I flags
	INCLUDE_FLAGS=$(for dir in $ORIOLEDB_INCLUDE_DIRS $PG_INCLUDE_DIRS; do echo -n "-I$dir "; done)

	cppcheck \
		--enable=warning,portability,performance \
		--suppressions-list=ci/cppcheck-suppress \
		--std=c99 --inline-suppr --verbose \
		-D__GNUC__ \
		${CPPCHK_DEFS[@]} \
		-DUSE_ASSERT_CHECKING \
		$INCLUDE_FLAGS \
		--include=$(pg_config --includedir-server)/pg_config_manual.h \
		src/*.c src/*/*.c include/*.h include/*/*.h 2> cppcheck.log

	if [ -s cppcheck.log ]; then
		echo "cppcheck report:"
		cat cppcheck.log
		status=1 # error
	fi
fi
cd ..

exit $status
