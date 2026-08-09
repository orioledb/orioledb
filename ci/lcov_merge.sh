#!/bin/bash

set -eu

sudo apt-get update -qq
sudo apt-get install lcov
# Matrix entries that carry a "replica" dimension append an _r<N> suffix to the
# artifact name, so the downloaded directories are named ..._coverage.info_r3
# rather than ending in coverage.info.  Match both, or the merge runs with no
# inputs and lcov fails the job.
lcov $(ls -d1 *coverage.info*/coverage.info | xargs -I{} echo "-a {}") -o ./orioledb/coverage.info