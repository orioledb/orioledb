#!/usr/bin/env bash
# shellcheck disable=SC2034,SC2154

testAlias[orioletest]=postgres

imageTests[orioletest]='
	orioledb-basics
'
