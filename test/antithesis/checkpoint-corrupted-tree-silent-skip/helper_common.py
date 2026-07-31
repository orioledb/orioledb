import json
import os
import re

import psycopg2
from antithesis.assertions import always

PGHOST = os.environ.get("PGHOST", "orioledb")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]

# Path at which the orioledb container's PGDATA is also visible to this
# client container.
PGDATA_DIR = os.environ.get("PGDATA_DIR", "/var/lib/postgresql/data")

CORRUPT_TABLE = "o_ckpt_corrupt"
HEALTHY_TABLE = "o_ckpt_healthy"
HEALTHY_SEQUENCE = "o_ckpt_healthy_seq"
CONFIG_TABLE = "o_ckpt_corrupt_config"

CONNECTION_LOST_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

CORRUPTION_STYLES = ["truncate_zero", "truncate_partial", "bitflip"]

BATCH_TYPICAL_MENU = [2, 8, 32]

CHECKPOINT_CONFIG_PRESETS = [
    {"checkpoint_timeout": "5min", "max_wal_size": "1GB",
     "checkpoint_completion_target": "0.9"},  # ~Postgres default ("relaxed")
    {"checkpoint_timeout": "2min", "max_wal_size": "256MB",
     "checkpoint_completion_target": "0.5"},  # moderate
    {"checkpoint_timeout": "30s", "max_wal_size": "64MB",
     "checkpoint_completion_target": "0.1"},  # aggressive
]

MAX_TOLERATED_FATAL_PROBES = 2


def connect(application_name):
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        application_name=application_name,
    )
    conn.autocommit = True
    return conn


def execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchall()
    return None


def ensure_schema(conn):
    execute(conn, "CREATE EXTENSION IF NOT EXISTS orioledb;")
    execute(
        conn, f"""
        CREATE TABLE IF NOT EXISTS {CORRUPT_TABLE} (
            k int PRIMARY KEY,
            v text NOT NULL
        ) USING orioledb;
    """)
    (count,) = execute(conn, f"SELECT count(*) FROM {CORRUPT_TABLE}")[0]
    if count == 0:
        execute(
            conn, f"""
            INSERT INTO {CORRUPT_TABLE}
                SELECT i, repeat('x', 200) FROM generate_series(1, 5000) i;
        """)
    execute(
        conn, f"""
        CREATE TABLE IF NOT EXISTS {HEALTHY_TABLE} (
            id bigint NOT NULL,
            val bigint NOT NULL,
            PRIMARY KEY (id)
        ) USING orioledb;
    """)
    execute(conn, f"CREATE SEQUENCE IF NOT EXISTS {HEALTHY_SEQUENCE};")
    (count,) = execute(conn, f"SELECT count(*) FROM {HEALTHY_TABLE}")[0]
    if count == 0:
        execute(
            conn, f"""
            INSERT INTO {HEALTHY_TABLE}
                SELECT nextval('{HEALTHY_SEQUENCE}'), 0
                FROM generate_series(1, 8);
        """)
    execute(
        conn, f"""
        CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (
            key text PRIMARY KEY,
            value text NOT NULL
        );
    """)
    # Deterministic, required for reliable reproduction (mirrors the fix's
    # own regression test, test_checkpoint_fatal_on_corrupted_tree): without
    # disabling the background writer, it could race to rewrite the target
    # tree's pages between our forced eviction and our corruption step.
    execute(conn, "ALTER SYSTEM SET orioledb.debug_disable_bgwriter = true;")
    execute(conn, "SELECT pg_reload_conf();")


def save_config(conn, key, value):
    execute(
        conn, f"""
        INSERT INTO {CONFIG_TABLE} (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value;
    """, (key, json.dumps(value)))


def load_config(conn, key, default):
    rows = execute(
        conn, f"SELECT value FROM {CONFIG_TABLE} WHERE key = %s", (key,))
    if not rows:
        return default
    return json.loads(rows[0][0])


def bump_counter(conn, key, delta=1):
    rows = execute(
        conn, f"""
        INSERT INTO {CONFIG_TABLE} (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE
            SET value = (({CONFIG_TABLE}.value)::bigint + %s)::text
        RETURNING value;
    """, (key, str(delta), delta))
    return int(rows[0][0])


def load_counter(conn, key, default=0):
    rows = execute(
        conn, f"SELECT value FROM {CONFIG_TABLE} WHERE key = %s", (key,))
    if not rows:
        return default
    return int(rows[0][0])


def apply_checkpoint_config(conn, preset):
    # Preset keys/values are always drawn from CHECKPOINT_CONFIG_PRESETS
    # above (fixed, hardcoded constants, never external input), so building
    # the SQL directly is safe -- ALTER SYSTEM SET doesn't support
    # parameterizing the GUC name via a placeholder.
    for key, value in preset.items():
        execute(conn, f"ALTER SYSTEM SET {key} = '{value}';")
    execute(conn, "SELECT pg_reload_conf();")


def postmaster_start_time(conn):
    (start_time,) = execute(conn, "SELECT pg_postmaster_start_time()")[0]
    return start_time.isoformat()


def maybe_record_restart(conn, current_start_time):
    """
    Atomically detects whether the server has restarted since the last time
    any command recorded its postmaster start time, and if so bumps
    restart_count. The WHERE clause on the upsert makes the compare-and-set
    atomic within one statement, so concurrent invocations from
    serial_driver_/parallel_driver_/anytime_ can't double-count the same
    transition.
    """
    rows = execute(
        conn, f"""
        WITH prev AS (
            SELECT value FROM {CONFIG_TABLE} WHERE key = 'postmaster_start'
        ), upsert AS (
            INSERT INTO {CONFIG_TABLE} (key, value)
                VALUES ('postmaster_start', %s)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
                WHERE {CONFIG_TABLE}.value IS DISTINCT FROM excluded.value
            RETURNING 1
        )
        SELECT (SELECT value FROM prev), EXISTS(SELECT 1 FROM upsert);
    """, (current_start_time,))
    prev_value, changed = rows[0]
    if changed and prev_value is not None:
        bump_counter(conn, "restart_count", 1)
        return True
    return False


def get_datoid(conn):
    (datoid,) = execute(
        conn, "SELECT oid FROM pg_database WHERE datname = current_database()"
    )[0]
    return datoid


def get_relfilenode(conn, table):
    (relfilenode,) = execute(
        conn, f"SELECT relfilenode FROM pg_class WHERE oid = '{table}'::regclass"
    )[0]
    return relfilenode


_FILE_PREFIX_RE = re.compile(r"^(\d+)([.\-]|$)")


def find_target_files(datoid, relfilenode):
    """
    Locate exactly this table's on-disk tree file(s) (not its .map
    checkpoint-bookkeeping file, and not any other table's files that
    happen to share a numeric prefix) under orioledb_data/<datoid>/.
    Filenames follow btree_smgr_filename()'s convention: "<relnode>",
    "<relnode>.<segno>", "<relnode>-<chkpnum>", or
    "<relnode>.<segno>-<chkpnum>".
    """
    dir_path = os.path.join(PGDATA_DIR, "orioledb_data", str(datoid))
    if not os.path.isdir(dir_path):
        return []
    matches = []
    for name in os.listdir(dir_path):
        if name.endswith("map"):
            continue
        m = _FILE_PREFIX_RE.match(name)
        if m and int(m.group(1)) == relfilenode:
            matches.append(os.path.join(dir_path, name))
    return matches


def corrupt_file(path, style, rng_value):
    size = os.path.getsize(path)
    if size == 0:
        return
    with open(path, "r+b") as fh:
        if style == "truncate_zero":
            fh.truncate(0)
        elif style == "truncate_partial":
            fh.truncate(min(size, 512))
        elif style == "bitflip":
            offset = rng_value % size
            fh.seek(offset)
            byte = fh.read(1)
            fh.seek(offset)
            fh.write(bytes([byte[0] ^ 0xFF]))
        else:
            raise ValueError(f"unknown corruption style: {style}")


def load_batch_menu(conn):
    typical = load_config(conn, "batch_typical", BATCH_TYPICAL_MENU[1])
    family = {1, max(1, typical - 1), typical, typical + 1, 2 * typical, 64}
    return sorted(family)


def assert_bounded_crash_loop(conn, label):
    attempts = load_counter(conn, "probe_attempts")
    fatals = load_counter(conn, "probe_fatals")
    restarts = load_counter(conn, "restart_count")
    corrupted = load_config(conn, "corrupted", False)
    print(f"[{label}] corrupted={corrupted} probe_attempts={attempts} "
          f"probe_fatals={fatals} restart_count={restarts}", flush=True)
    always(
        fatals <= MAX_TOLERATED_FATAL_PROBES,
        "checkpoint-corrupted-tree-silent-skip: a single corrupted tree "
        "does not cause an unbounded FATAL/restart crash loop across "
        "repeated deliberate CHECKPOINT probes",
        {"label": label, "probe_attempts": attempts, "probe_fatals": fatals,
         "restart_count": restarts, "corrupted": corrupted},
    )
