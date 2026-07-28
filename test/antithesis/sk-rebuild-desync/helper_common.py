"""
Shared helpers for the recovery-sk-rebuild-desync workload
(antithesis/scratchbook/properties/recovery-sk-rebuild-desync.md).

Not a test command itself (the `helper_` filename prefix tells Antithesis to
ignore it), just a module imported by the actual test commands
(first_/parallel_driver_/anytime_/finally_) living alongside it.
"""
import json
import os

import psycopg2
from antithesis.assertions import always

PGHOST = os.environ.get("PGHOST", "orioledb")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]

TABLE = "o_sk_desync"
SEQUENCE = "o_sk_desync_seq"
CONFIG_TABLE = "o_sk_desync_config"

CONNECTION_LOST_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

# Menu axis (see antithesis-workload references/interesting-values.md):
# swarmed once per timeline in first_, then read back by every later
# invocation so the whole timeline shares one bias instead of each process
# re-rolling independently.
SEED_ROWS_MENU = [2, 8, 64]
BATCH_TYPICAL_MENU = [2, 8, 32]

# Weighted action presets: (insert, update_token, delete) relative counts.
# Deliberately includes omission cases (an action entirely absent for the
# timeline) per test-commands.md's "action omission" swarming guidance.
# update_token is weighted heaviest across most presets because it's the
# action that changes the unique-indexed column -- the specific path the
# property's root-cause docs hypothesize as the broken one (a WAL_REC_UPDATE
# that changes an indexed column being replayed as insert-only).
ACTION_WEIGHT_PRESETS = [
    {"insert": 1, "update_token": 1, "delete": 1},  # uniform
    {"insert": 1, "update_token": 5, "delete": 1},  # update-heavy (default bias)
    {"insert": 1, "update_token": 5, "delete": 0},  # no deletes
    {"insert": 0, "update_token": 5, "delete": 1},  # no inserts
    {"insert": 1, "update_token": 0, "delete": 1},  # no updates (control)
    {"insert": 3, "update_token": 3, "delete": 0},  # grow-only
]


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


def ensure_schema(conn, seed_rows):
    execute(conn, "CREATE EXTENSION IF NOT EXISTS orioledb;")
    execute(
        conn, f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id bigint NOT NULL,
            token bigint NOT NULL,
            PRIMARY KEY (id)
        ) USING orioledb;
    """)
    execute(
        conn,
        f"CREATE UNIQUE INDEX IF NOT EXISTS {TABLE}_token_idx "
        f"ON {TABLE} (token);")
    execute(conn, f"CREATE SEQUENCE IF NOT EXISTS {SEQUENCE};")
    execute(
        conn, f"""
        CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (
            key text PRIMARY KEY,
            value text NOT NULL
        );
    """)
    (count,) = execute(conn, f"SELECT count(*) FROM {TABLE}")[0]
    if count == 0:
        execute(
            conn, f"""
            INSERT INTO {TABLE}
                SELECT nextval('{SEQUENCE}'), nextval('{SEQUENCE}')
                FROM generate_series(1, {seed_rows});
        """)


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


def weighted_action_menu(weights):
    menu = []
    for action, count in weights.items():
        menu.extend([action] * count)
    return menu or ["update_token"]  # never return an empty menu


def load_action_menu(conn):
    weights = load_config(conn, "action_weights", ACTION_WEIGHT_PRESETS[1])
    return weighted_action_menu(weights)


def load_batch_menu(conn):
    typical = load_config(conn, "batch_typical", BATCH_TYPICAL_MENU[1])
    family = {1, max(1, typical - 1), typical, typical + 1, 2 * typical, 64}
    return sorted(family)


def assert_consistent(conn, label):
    (n_pk,) = execute(conn, f"SELECT count(*) FROM {TABLE}")[0]
    (n_sk,) = execute(conn, f"SELECT count(DISTINCT token) FROM {TABLE}")[0]
    (tbl_ok,) = execute(
        conn, f"SELECT orioledb_tbl_check('{TABLE}'::regclass)")[0]
    consistent = (n_pk == n_sk) and bool(tbl_ok)
    print(f"[{label}] pk_rows={n_pk} sk_distinct={n_sk} tbl_check={tbl_ok} "
          f"consistent={consistent}", flush=True)
    always(
        consistent,
        "o_sk_desync PK rows match distinct SK tokens after ordinary "
        "commits and crash recovery of unrelated transactions "
        "(recovery-sk-rebuild-desync)",
        {"label": label, "pk_rows": n_pk, "sk_distinct": n_sk,
         "tbl_check": bool(tbl_ok)},
    )
