#!/bin/bash

set -eu
export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

status=0

cd orioledb
if [ "$COMPILER" = "clang" ]; then
	scan-build-$LLVM_VER --status-bugs \
		-disable-checker deadcode.DeadStores \
		make USE_PGXS=1 IS_DEV=1 USE_ASSERT_CHECKING=1 || status=$?

elif [ "$COMPILER" = "gcc" ]; then
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
		-D__x86_64__ \
		-D__aarch64__ \
		-D__arm \
		-D__arm__ \
		-DUSE_ASSERT_CHECKING \
		$INCLUDE_FLAGS \
		src/*.c src/*/*.c include/*.h include/*/*.h 2> cppcheck.log

	if [ -s cppcheck.log ]; then
		echo "cppcheck report:"
		cat cppcheck.log
		status=1 # error
	fi
fi
cd ..

exit $status
