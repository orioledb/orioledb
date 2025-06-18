#!/bin/bash

set -eu

sudo apt-get update -qq
sudo apt-get install lcov
lcov $(ls -d1 *coverage.info/coverage.info | xargs -I{} echo "-a {}") -o ./orioledb/coverage.info