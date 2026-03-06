#!/bin/bash

set -eux

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

PGDATA="$GITHUB_WORKSPACE/pgdata"

pg_ctl -D "$PGDATA" stop || true
rm -rf "$PGDATA"
