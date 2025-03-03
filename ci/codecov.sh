#!/bin/bash

set -eu

cd orioledb
bash <(curl -s https://codecov.io/bash) -X gcov
cd ..
