#!/bin/bash

set -eu

cd orioledb
if [ $COMPILER = "clang" ]; then
	llvm-cov-$LLVM_VER gcov src/*.c src/*/*.c include/*.h include/*/*.h -r
else
	gcov src/*.c src/*/*.c -r
fi
cd ..