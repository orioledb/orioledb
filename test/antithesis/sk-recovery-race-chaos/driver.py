#!/usr/bin/env python3
"""
Antithesis singleton-driver workload that stresses the PK/SK checkpoint
race behind https://github.com/orioledb/orioledb/issues/855 WITHOUT
deterministic construction: no pg_stopevent_set, just concurrent DML
against a table with a unique secondary index, under a very short
checkpoint_timeout, reported via Antithesis SDK assertions. Antithesis's
own fault injection has to both land near an automatic checkpoint AND
overlap the DML burst for this variant to ever hit the race - see
workload/sk-recovery-race for the deterministic alternative.
"""
import os
import threading
import time

import psycopg2
from antithesis.assertions import always, sometimes

PGHOST = os.environ.get("PGHOST", "orioledb")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]
BURST_SECONDS = float(os.environ.get("BURST_SECONDS", "5"))
BURST_WORKERS = int(os.environ.get("BURST_WORKERS", "8"))

TABLE = "o_sk_pending"
SEED_ROWS = 5


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
    execute(conn, f"CREATE SEQUENCE IF NOT EXISTS {TABLE}_race_seq;")
    (count,) = execute(conn, f"SELECT count(*) FROM {TABLE}")[0]
    if count == 0:
        execute(
            conn, f"""
            INSERT INTO {TABLE}
                SELECT nextval('{TABLE}_race_seq'),
                       nextval('{TABLE}_race_seq')
                FROM generate_series(1, {SEED_ROWS});
        """)
        execute(conn, "CHECKPOINT;")


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
        "o_sk_pending PK rows match distinct SK tokens after concurrent "
        "DML + automatic checkpoints (orioledb#855)",
        {"label": label, "pk_rows": n_pk, "sk_distinct": n_sk,
         "tbl_check": bool(tbl_ok)},
    )


def checkpoint_count(conn):
    # PostgreSQL 17 split checkpoint stats out of pg_stat_bgwriter into a
    # dedicated pg_stat_checkpointer view (checkpoints_timed/checkpoints_req
    # became num_timed/num_requested); this repo supports PG 16-18, so
    # detect which one exists at runtime instead of hardcoding a version.
    (has_checkpointer_view,) = execute(
        conn, "SELECT to_regclass('pg_stat_checkpointer') IS NOT NULL")[0]
    if has_checkpointer_view:
        (timed, req) = execute(
            conn,
            "SELECT num_timed, num_requested FROM pg_stat_checkpointer"
        )[0]
    else:
        (timed, req) = execute(
            conn,
            "SELECT checkpoints_timed, checkpoints_req "
            "FROM pg_stat_bgwriter"
        )[0]
    return timed + req


def dml_worker(stop_at, errors, index):
    conn = connect(f"chaos_{index}")
    conn.autocommit = False
    try:
        while time.monotonic() < stop_at:
            try:
                (ids,) = execute(
                    conn,
                    f"SELECT array_agg(id) FROM (SELECT id FROM {TABLE} "
                    f"ORDER BY random() LIMIT 2) s")[0]
                upd_id, del_id = ids[0], ids[1]
                execute(
                    conn,
                    f"INSERT INTO {TABLE} VALUES "
                    f"(nextval('{TABLE}_race_seq'), "
                    f"nextval('{TABLE}_race_seq'));")
                execute(
                    conn,
                    f"UPDATE {TABLE} SET token = "
                    f"nextval('{TABLE}_race_seq') WHERE id = {upd_id};")
                execute(conn, f"DELETE FROM {TABLE} WHERE id = {del_id};")
                conn.commit()
            except psycopg2.Error:
                conn.rollback()
    except Exception as exc:  # noqa: BLE001 - surfaced via errors list
        errors[index] = exc
    finally:
        conn.close()


def dml_burst(ctl_conn):
    checkpoints_before = checkpoint_count(ctl_conn)
    stop_at = time.monotonic() + BURST_SECONDS

    errors = [None] * BURST_WORKERS
    threads = [
        threading.Thread(target=dml_worker, args=(stop_at, errors, i))
        for i in range(BURST_WORKERS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=BURST_SECONDS + 30)

    for err in errors:
        if err is not None:
            raise err

    checkpoints_after = checkpoint_count(ctl_conn)
    overlapped = checkpoints_after > checkpoints_before
    print(f"checkpoints during burst: "
          f"{checkpoints_after - checkpoints_before}", flush=True)
    sometimes(
        overlapped,
        "at least one automatic checkpoint fired while concurrent "
        "INSERT/UPDATE/DELETE were in flight against o_sk_pending",
        {"checkpoints_before": checkpoints_before,
         "checkpoints_after": checkpoints_after},
    )


def main():
    ctl_conn = None
    try:
        ctl_conn = connect("s_ctl")
        ensure_schema(ctl_conn)
        assert_consistent(ctl_conn, "startup")
        dml_burst(ctl_conn)
        assert_consistent(ctl_conn, "post-burst")
    except psycopg2.OperationalError as exc:
        print(
            f"lost connection to target (likely fault injection landed "
            f"mid-burst), will re-check on next run: {exc}",
            flush=True)
        return
    finally:
        if ctl_conn is not None:
            try:
                ctl_conn.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass


if __name__ == "__main__":
    main()
