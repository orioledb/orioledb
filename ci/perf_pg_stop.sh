#!/bin/bash

set -eux

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

PGDATA="$GITHUB_WORKSPACE/pgdata"

pg_ctl -D "$PGDATA" stop || true

# Save PostgreSQL log before cleanup
LOG_DIR="$HOME/logs"
mkdir -p "$LOG_DIR"
if [ -n "${GITHUB_RUN_ID:-}" ]; then
  LOG_NAME="${GITHUB_RUN_ID}-postgres.log"
else
  # Incrementing counter fallback
  COUNTER=1
  while [ -f "$LOG_DIR/${COUNTER}-postgres.log" ]; do
    COUNTER=$((COUNTER + 1))
  done
  LOG_NAME="${COUNTER}-postgres.log"
fi

echo "Saving PostgreSQL log to $LOG_DIR/$LOG_NAME"
cp "$PGDATA/postgresql.log" "$LOG_DIR/$LOG_NAME" 2>/dev/null || echo "Warning: no postgresql.log found"

rm -rf "$PGDATA"
