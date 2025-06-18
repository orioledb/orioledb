#!/bin/bash

set -eu

cd orioledb
if [ $COMPILER = "clang" ]; then
    # trick with gcov-tool needed to not have gcov version mismatch: file.gcno:version '...*', prefer '...*'
    lcov --gcov-tool "$PWD/ci/llvm-gcov.sh" --capture --directory . --no-external --output-file coverage.info
else
	lcov --capture --directory . --no-external --output-file coverage.info
fi
cd ..