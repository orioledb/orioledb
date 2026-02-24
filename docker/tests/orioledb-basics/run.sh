#!/bin/bash
# shellcheck disable=SC2119,SC2120
set -eo pipefail

image="$1"

export POSTGRES_USER='my cool orioledb user'
export POSTGRES_PASSWORD='my cool orioledb password'
export POSTGRES_DB='my cool orioledb database'

cname="orioletest-container-$RANDOM-$RANDOM"
cid="$(docker run -d -e POSTGRES_USER -e POSTGRES_PASSWORD -e POSTGRES_DB --name "$cname" "$image")"
trap 'docker rm -vf "$cid" > /dev/null' EXIT

psql() {
    docker run --rm -i \
        --link "$cname":orioletest \
        --entrypoint psql \
        -e PGPASSWORD="$POSTGRES_PASSWORD" \
        "$image" \
        --host orioletest \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        --quiet --no-align --tuples-only \
        "$@"
}

# Set default values for POSTGRES_TEST_TRIES and POSTGRES_TEST_SLEEP if they are not set.
# You can change the default value of POSTGRES_TEST_TRIES and the POSTGRES_TEST_SLEEP in the CI build settings.
# For special cases like Buildx/qemu tests, you may need to set POSTGRES_TEST_TRIES to 42.
: "${POSTGRES_TEST_TRIES:=15}"
: "${POSTGRES_TEST_SLEEP:=2}"
tries="$POSTGRES_TEST_TRIES"
while ! echo 'SELECT 1' | psql &>/dev/null; do
    ((tries--))
    if [ $tries -le 0 ]; then
        echo >&2 'postgres failed to accept connections in a reasonable amount of time!'
        echo 'SELECT 1' | psql # to hopefully get a useful error message
        false
    fi
    sleep "$POSTGRES_TEST_SLEEP"
done


# minimal OrioleDB test
psql <<'EOSQL'

    CREATE EXTENSION IF NOT EXISTS orioledb;
    SELECT orioledb_commit_hash();
    CREATE TABLE o_test_generated (
        a int,
        b int GENERATED ALWAYS AS (a * 2) STORED
    ) USING orioledb;
    INSERT INTO o_test_generated VALUES (1), (2);
    SELECT * FROM o_test_generated;

EOSQL

echo "SELECT version();" | psql
echo "\dx" | psql

# Helper: assert two values are equal; print PASS/FAIL and exit on failure
assert_eq() {
    local description="$1"
    local expected="$2"
    local actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "PASS: $description"
    else
        echo "FAIL: $description"
        echo "  expected: $(printf '%q' "$expected")"
        echo "  actual:   $(printf '%q' "$actual")"
        exit 1
    fi
}

echo ""
echo "=== Test: Dev functions available (required for regression tests) ==="
# Verify that IS_DEV=1 build produced all dev-only functions.
# If any are missing, .dockerignore likely leaks a stale generated SQL file.
result=$(echo "SELECT count(*) FROM pg_proc WHERE proname = 'orioledb_parallel_debug_start';" | psql)
assert_eq "dev functions: orioledb_parallel_debug_start (orioledb--1.0_dev.sql)" "1" "$result"

result=$(echo "SELECT count(*) FROM pg_proc WHERE proname = 'orioledb_rewind_set_complete';" | psql)
assert_eq "dev functions: orioledb_rewind_set_complete (orioledb--1.4--1.5_dev.sql)" "1" "$result"

result=$(echo "SELECT count(*) FROM pg_proc WHERE proname = 'orioledb_insert_sys_xid_undo_location';" | psql)
assert_eq "dev functions: orioledb_insert_sys_xid_undo_location (orioledb--1.5--1.6_dev.sql)" "1" "$result"

echo ""
echo "All smoke tests passed."
