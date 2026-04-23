#!/bin/bash
set -eux

# Cache/restore pgdata to skip TPC-C data loading on repeated runs.
# Usage:
#   perf_db_cache.sh restore   — restore pgdata from cache if available
#   perf_db_cache.sh save      — save current pgdata to cache
#
# Sets DB_CACHED=true/false in $GITHUB_ENV for downstream steps.
# Cache key: ${PG_VERSION}-${WAREHOUSES} (data is code-independent).

export PATH="$GITHUB_WORKSPACE/pgsql/bin:$PATH"

PGDATA="$GITHUB_WORKSPACE/pgdata"
CACHE_BASE="/tmp/perf-db-cache"
CACHE_KEY="${PG_VERSION:-17}-${WAREHOUSES:-1}W"
CACHE_DIR="${CACHE_BASE}/${CACHE_KEY}"
MAX_CACHES=4

ACTION="${1:-}"

case "$ACTION" in
  restore)
    if [ -d "$CACHE_DIR/pgdata" ]; then
      echo "=== Restoring DB from cache: ${CACHE_KEY} ==="

      # Stop PG, replace pgdata with cached copy, restart
      pg_ctl -D "$PGDATA" stop -m fast 2>/dev/null || true
      rm -rf "$PGDATA"
      cp -a "$CACHE_DIR/pgdata" "$PGDATA"
      pg_ctl -D "$PGDATA" -l "$PGDATA/postgresql.log" start
      pg_isready -t 30

      echo "DB_CACHED=true" >> "$GITHUB_ENV"
      echo "=== DB restored from cache ==="
    else
      echo "=== No DB cache found for: ${CACHE_KEY} ==="
      echo "DB_CACHED=false" >> "$GITHUB_ENV"
    fi
    ;;

  save)
    echo "=== Saving DB to cache: ${CACHE_KEY} ==="

    # Stop PG cleanly before snapshot
    pg_ctl -D "$PGDATA" stop -m fast
    sleep 2

    # Save pgdata to cache (overwrite if exists)
    rm -rf "$CACHE_DIR"
    mkdir -p "$CACHE_DIR"
    cp -a "$PGDATA" "$CACHE_DIR/pgdata"

    # Restart PG (workflow continues after save)
    pg_ctl -D "$PGDATA" -l "$PGDATA/postgresql.log" start
    pg_isready -t 30

    # Retention: keep only last MAX_CACHES entries
    ENTRIES=$(find "$CACHE_BASE" -maxdepth 1 -mindepth 1 -type d | wc -l)
    if [ "$ENTRIES" -gt "$MAX_CACHES" ]; then
      find "$CACHE_BASE" -maxdepth 1 -mindepth 1 -type d \
        -printf '%T@ %p\n' | sort -n | head -n -"$MAX_CACHES" | cut -d' ' -f2 | xargs -r rm -rf
    fi

    echo "=== DB cached (${CACHE_KEY}), total entries: $(find "$CACHE_BASE" -maxdepth 1 -mindepth 1 -type d | wc -l) ==="
    ;;

  *)
    echo "Usage: $0 {restore|save}"
    exit 1
    ;;
esac
