#!/usr/bin/env python3
"""
Antithesis singleton-driver workload that constructs the PK/SK checkpoint
race behind https://github.com/orioledb/orioledb/issues/855 and reports
consistency via Antithesis SDK assertions, so Antithesis's fault injection
has a real chance of landing inside the race window instead of a random
point in an unrelated workload.

Mirrors test/t/recovery_test.py::test_recovery_sk_modify_pending_concurrent,
minus the self-triggered crash: Antithesis's hypervisor decides if/when
test.orioledb dies, this driver only has to hold the window open.
"""
import os
import threading
import time

import psycopg2
from antithesis.assertions import always, reachable

PGHOST = os.environ.get("PGHOST", "orioledb")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ["PGDATABASE"]
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]
RACE_WINDOW_SECONDS = float(os.environ.get("RACE_WINDOW_SECONDS", "2"))
STOPEVENT_TIMEOUT_SECONDS = float(
    os.environ.get("STOPEVENT_TIMEOUT_SECONDS", "30"))

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
        "o_sk_pending PK rows match distinct SK tokens after the "
        "sk_modify_pending race (orioledb#855)",
        {"label": label, "pk_rows": n_pk, "sk_distinct": n_sk,
         "tbl_check": bool(tbl_ok)},
    )


def wait_stopevent(ctl_conn, pid, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        (parked,) = execute(
            ctl_conn, """
            SELECT EXISTS(
                SELECT se.* FROM pg_stopevents() se
                WHERE se.waiter_pids @> ARRAY[%s]
            );
        """, (pid,))[0]
        if parked:
            return
        time.sleep(0.1)
    raise TimeoutError(f"pid {pid} never parked at sk_modify_pending")


def run_dml(conn, sql, errors, index):
    try:
        execute(conn, sql)
        conn.commit()
    except Exception as exc:  # noqa: BLE001 - surfaced via errors list
        errors[index] = exc


def race_iteration(ctl_conn):
    (ids,) = execute(
        ctl_conn,
        f"SELECT array_agg(id) FROM (SELECT id FROM {TABLE} "
        f"ORDER BY random() LIMIT 2) s")[0]
    upd_id, del_id = ids[0], ids[1]

    dmls = [
        (f"INSERT INTO {TABLE} VALUES "
         f"(nextval('{TABLE}_race_seq'), nextval('{TABLE}_race_seq'));",
         "s_ins"),
        (f"UPDATE {TABLE} SET token = nextval('{TABLE}_race_seq') "
         f"WHERE id = {upd_id};", "s_upd"),
        (f"DELETE FROM {TABLE} WHERE id = {del_id};", "s_del"),
    ]

    conns = []
    pids = []
    for _, app in dmls:
        conn = connect(app)
        conn.autocommit = False
        (pid,) = execute(conn, "SELECT pg_backend_pid();")[0]
        conn.commit()
        conns.append(conn)
        pids.append(pid)

    execute(
        ctl_conn, "SELECT pg_stopevent_set('sk_modify_pending', "
        "'$applicationName starts with \"s_\" "
        "&& $applicationName != \"s_ctl\"');")

    errors = [None] * len(dmls)
    threads = []
    for index, (conn, (sql, _)) in enumerate(zip(conns, dmls)):
        t = threading.Thread(target=run_dml, args=(conn, sql, errors, index))
        t.start()
        threads.append(t)

    try:
        for pid in pids:
            wait_stopevent(ctl_conn, pid, STOPEVENT_TIMEOUT_SECONDS)

        reachable(
            "all three DML backends parked at sk_modify_pending "
            "(PK applied, SK pending)",
            {"pids": pids},
        )

        execute(ctl_conn, "CHECKPOINT;")
        # Hold the parked-backends / just-checkpointed state open so
        # Antithesis's fault injection has a real window to land in.
        time.sleep(RACE_WINDOW_SECONDS)
    finally:
        execute(ctl_conn, "SELECT pg_stopevent_reset('sk_modify_pending');")
        for t in threads:
            t.join(timeout=STOPEVENT_TIMEOUT_SECONDS)
        for conn in conns:
            conn.close()

    for err in errors:
        if err is not None:
            raise err


def main():
    ctl_conn = None
    try:
        ctl_conn = connect("s_ctl")
        ensure_schema(ctl_conn)
        # Catches a divergence caused by a fault that landed between the
        # previous iteration and this one.
        assert_consistent(ctl_conn, "startup")
        race_iteration(ctl_conn)
        assert_consistent(ctl_conn, "post-race")
    except psycopg2.OperationalError as exc:
        print(
            f"lost connection to target (likely fault injection landed "
            f"mid-iteration), will re-check on next run: {exc}",
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
