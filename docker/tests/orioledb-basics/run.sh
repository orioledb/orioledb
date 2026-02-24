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
        --set=ON_ERROR_STOP=1 \
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
echo "=== Test: Primary key CRUD ==="
psql <<'EOSQL'
    CREATE TABLE o_test_pk (
        id   integer NOT NULL PRIMARY KEY,
        val  text
    ) USING orioledb;
    INSERT INTO o_test_pk VALUES (1, 'one'), (2, 'two'), (3, 'three');
    UPDATE o_test_pk SET val = 'ONE' WHERE id = 1;
    DELETE FROM o_test_pk WHERE id = 3;
    SELECT id, val FROM o_test_pk ORDER BY id;
EOSQL

result=$(echo "SELECT val FROM o_test_pk WHERE id = 1;" | psql)
assert_eq "primary key: UPDATE changed value" "ONE" "$result"

result=$(echo "SELECT count(*) FROM o_test_pk;" | psql)
assert_eq "primary key: 2 rows after insert/update/delete" "2" "$result"

echo ""
echo "=== Test: Secondary index scan ==="
psql <<'EOSQL'
    CREATE TABLE o_test_idx (
        id    integer NOT NULL PRIMARY KEY,
        score integer
    ) USING orioledb;
    CREATE INDEX o_test_idx_score ON o_test_idx (score);
    INSERT INTO o_test_idx SELECT i, i * 10 FROM generate_series(1, 20) AS i;
EOSQL

result=$(echo "SELECT count(*) FROM o_test_idx WHERE score BETWEEN 50 AND 100;" | psql)
assert_eq "secondary index: rows with score 50-100" "6" "$result"

# Force index scan and verify the planner actually uses our secondary index.
explain_plan=$(echo "SET enable_seqscan = off; EXPLAIN SELECT count(*) FROM o_test_idx WHERE score BETWEEN 50 AND 100;" | psql)
if [[ "$explain_plan" != *"o_test_idx_score"* ]]; then
    echo "FAIL: secondary index: expected EXPLAIN to use o_test_idx_score" >&2
    echo "$explain_plan" >&2
    exit 1
fi
echo "PASS: secondary index: EXPLAIN confirms index scan on o_test_idx_score"

echo ""
echo "=== Test: NULL handling ==="
psql <<'EOSQL'
    CREATE TABLE o_test_nulls (
        id  integer NOT NULL PRIMARY KEY,
        val integer
    ) USING orioledb;
    INSERT INTO o_test_nulls VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
EOSQL

result=$(echo "SELECT count(*) FROM o_test_nulls WHERE val IS NULL;" | psql)
assert_eq "nulls: IS NULL count" "2" "$result"

result=$(echo "SELECT count(*) FROM o_test_nulls WHERE val IS NOT NULL;" | psql)
assert_eq "nulls: IS NOT NULL count" "2" "$result"

echo ""
echo "=== Test: INSERT ON CONFLICT DO NOTHING ==="
psql <<'EOSQL'
    CREATE TABLE o_test_ioc (
        id  integer NOT NULL PRIMARY KEY,
        val text
    ) USING orioledb;
    INSERT INTO o_test_ioc VALUES (1, 'original');
    INSERT INTO o_test_ioc VALUES (1, 'conflict') ON CONFLICT (id) DO NOTHING;
    INSERT INTO o_test_ioc VALUES (2, 'new')      ON CONFLICT (id) DO NOTHING;
EOSQL

result=$(echo "SELECT val FROM o_test_ioc WHERE id = 1;" | psql)
assert_eq "ioc: DO NOTHING keeps original value" "original" "$result"

result=$(echo "SELECT count(*) FROM o_test_ioc;" | psql)
assert_eq "ioc: 2 rows total" "2" "$result"

echo ""
echo "=== Test: INSERT ON CONFLICT DO UPDATE (upsert) ==="
psql <<'EOSQL'
    CREATE TABLE o_test_upsert (
        id    integer NOT NULL PRIMARY KEY,
        hits  integer NOT NULL DEFAULT 0
    ) USING orioledb;
    INSERT INTO o_test_upsert VALUES (1, 1);
    INSERT INTO o_test_upsert AS t VALUES (1, 1)
        ON CONFLICT (id) DO UPDATE SET hits = t.hits + EXCLUDED.hits;
    INSERT INTO o_test_upsert AS t VALUES (1, 1)
        ON CONFLICT (id) DO UPDATE SET hits = t.hits + EXCLUDED.hits;
EOSQL

result=$(echo "SELECT hits FROM o_test_upsert WHERE id = 1;" | psql)
assert_eq "upsert: hits accumulated to 3" "3" "$result"

echo ""
echo "=== Test: DDL ALTER TABLE ADD COLUMN ==="
psql <<'EOSQL'
    CREATE TABLE o_test_ddl (
        id  integer NOT NULL PRIMARY KEY,
        val text
    ) USING orioledb;
    INSERT INTO o_test_ddl VALUES (1, 'hello');
    ALTER TABLE o_test_ddl ADD COLUMN extra integer DEFAULT 42;
EOSQL

result=$(echo "SELECT extra FROM o_test_ddl WHERE id = 1;" | psql)
assert_eq "ddl: added column has default value" "42" "$result"

echo ""
echo "=== Test: Foreign key constraint ==="
psql <<'EOSQL'
    CREATE TABLE o_test_fk_parent (
        id  integer NOT NULL PRIMARY KEY,
        val text
    ) USING orioledb;
    CREATE TABLE o_test_fk_child (
        id        integer NOT NULL PRIMARY KEY,
        parent_id integer NOT NULL REFERENCES o_test_fk_parent (id)
    ) USING orioledb;
    INSERT INTO o_test_fk_parent VALUES (1, 'parent');
    INSERT INTO o_test_fk_child  VALUES (10, 1);
EOSQL

result=$(echo "SELECT count(*) FROM o_test_fk_child;" | psql)
assert_eq "fk: child row inserted" "1" "$result"

# FK violation must raise an error
fk_err=$(psql <<'EOSQL' 2>&1 || true
    INSERT INTO o_test_fk_child VALUES (99, 999);
EOSQL
)
if echo "$fk_err" | grep -q 'violates foreign key constraint'; then
    echo "PASS: fk: violation correctly rejected"
else
    echo "FAIL: fk: expected FK violation error, got: $fk_err"
    exit 1
fi

echo ""
echo "=== Test: TRUNCATE ==="
psql <<'EOSQL'
    CREATE TABLE o_test_truncate (
        id integer NOT NULL PRIMARY KEY
    ) USING orioledb;
    INSERT INTO o_test_truncate SELECT i FROM generate_series(1, 100) AS i;
    TRUNCATE o_test_truncate;
EOSQL

result=$(echo "SELECT count(*) FROM o_test_truncate;" | psql)
assert_eq "truncate: table is empty after TRUNCATE" "0" "$result"

echo ""
echo "=== Test: Generated columns ==="
psql <<'EOSQL'
    CREATE TABLE o_test_generated_multi (
        a integer NOT NULL PRIMARY KEY,
        b integer GENERATED ALWAYS AS (a * a) STORED,
        c text  GENERATED ALWAYS AS ('item_' || a::text) STORED
    ) USING orioledb;
    INSERT INTO o_test_generated_multi (a) VALUES (3), (5);
EOSQL

result=$(echo "SELECT b FROM o_test_generated_multi WHERE a = 5;" | psql)
assert_eq "generated: square of 5 is 25" "25" "$result"

result=$(echo "SELECT c FROM o_test_generated_multi WHERE a = 3;" | psql)
assert_eq "generated: text column for a=3" "item_3" "$result"

echo ""
echo "All smoke tests passed."
