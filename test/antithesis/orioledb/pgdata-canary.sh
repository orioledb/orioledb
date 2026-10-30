#!/bin/sh
set -eu

state_dir=${RUNTIME_STATE_DIR:-/runtime-state}
test -s "$state_dir/volume-canary"
cp "$state_dir/volume-canary" "$PGDATA/.antithesis-volume-canary"
sync "$state_dir/volume-canary" "$PGDATA/.antithesis-volume-canary"
