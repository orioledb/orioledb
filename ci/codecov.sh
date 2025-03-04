#!/bin/bash

set -eu

cd orioledb
if [ $COMPILER = "clang" ]; then
	bash <(curl -s https://codecov.io/bash) -x "llvm-cov-$LLVM_VER gcov"
else
	bash <(curl -s https://codecov.io/bash)
fi
cd ..
