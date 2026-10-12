#!/usr/bin/env bash
# shellcheck disable=SC2034,SC2154
#
# OrioleDB + Docker Official test suite
# See usage and examples: docker/README.md
# Test suite reference: https://github.com/docker-library/official-images/tree/master/test

# Reuse upstream `postgres` tests for repository `orioletest`.
testAlias[orioletest]=postgres

# Image tests for `orioletest`:
# - <project-root>/docker/test/orioledb-basics : minimal OrioleDB smoke test
# - <project-root>/docker/test/orioledb-check  : extended regression checks
#
# Disable by removing the line below:
imageTests[orioletest]='
	orioledb-basics
	orioledb-check
'
