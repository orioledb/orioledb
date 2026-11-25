#!/usr/bin/env python3
"""Deterministic (seeded) interleaving of a cursor scan in a transaction with
own undo (session A) and committed concurrent changes (session B) in the
same key range.  Checks the cursor output is strictly ascending.
"""
import os
import random
import sys
import psycopg2

port = int(sys.argv[1])
seed0 = int(sys.argv[2])
trials = int(sys.argv[3])
pad = 'k' * 60
VERBOSE = len(sys.argv) > 4


def conn():
    c = psycopg2.connect(host=os.environ.get('PGHOST', '/tmp'), port=port, dbname='postgres')
    c.autocommit = True
    c.cursor().execute("SET lock_timeout='100ms'; SET statement_timeout='20s'")
    return c


def key(n):
    return f"{pad}{n:07d}"


def run_trial(seed, A, B, tbl):
    rnd = random.Random(seed)
    a = A.cursor()
    b = B.cursor()
    b.execute(f"TRUNCATE {tbl}")
    n0 = rnd.randint(20, 600)
    keys = rnd.sample(range(1, 4000), n0)
    b.execute(
        f"INSERT INTO {tbl} SELECT 4, %s || lpad(k::text, 7, '0'), k, '{{}}' "
        f"FROM unnest(%s::int[]) k", (pad, keys))
    nextc = 10000
    log = []
    a.execute("BEGIN" + rnd.choice(["", " ISOLATION LEVEL REPEATABLE READ"]))

    def own_ops(cur, nops):
        nonlocal nextc
        for _ in range(nops):
            op = rnd.choice(['ins', 'upd', 'del', 'sp'])
            k = rnd.randint(1, 4000)
            nextc += 1
            if op == 'ins':
                stmt = (f"INSERT INTO {tbl} VALUES (4, '{key(k)}', {nextc}, '{{}}') "
                        f"ON CONFLICT (a,b) DO UPDATE SET c = EXCLUDED.c")
            elif op == 'upd':
                lo = rnd.randint(1, 4000)
                stmt = (f"UPDATE {tbl} SET payload = payload || '{{\"u\":1}}' "
                        f"WHERE a=4 AND b BETWEEN '{key(lo)}' AND '{key(lo + rnd.randint(1, 300))}'")
            elif op == 'del':
                stmt = f"DELETE FROM {tbl} WHERE a=4 AND b = '{key(k)}'"
            else:
                stmt = None
            log.append((cur is a and 'A' or 'B', stmt or 'savepoint-rollback'))
            try:
                if stmt:
                    cur.execute(stmt)
                else:
                    cur.execute("SAVEPOINT s")
                    lo = rnd.randint(1, 4000)
                    cur.execute(
                        f"UPDATE {tbl} SET payload = payload || '{{\"s\":1}}' "
                        f"WHERE a=4 AND b >= '{key(lo)}'")
                    cur.execute("ROLLBACK TO SAVEPOINT s")
            except psycopg2.errors.SerializationFailure:
                raise
            except (psycopg2.errors.UniqueViolation, psycopg2.errors.Error) as e:
                if 'server closed' in str(e):
                    raise
                if cur is a:
                    # aborted txn; restart it
                    a.execute("ROLLBACK")
                    a.execute("BEGIN")

    # A builds own undo first
    own_ops(a, rnd.randint(1, 8))
    a.execute("SET enable_seqscan = off; SET enable_bitmapscan = off")
    if VERBOSE:
        a.execute(f"EXPLAIN SELECT b FROM {tbl} WHERE a = 4 ORDER BY b")
        log.append(('A', 'PLAN ' + ' | '.join(r[0] for r in a.fetchall())))
    a.execute(f"DECLARE cur NO SCROLL CURSOR FOR SELECT b FROM {tbl} "
              f"WHERE a = 4 ORDER BY b")
    prev = None
    done = False
    steps = rnd.randint(2, 40)
    for _ in range(steps):
        a.execute(f"FETCH {rnd.randint(1, 60)} FROM cur")
        rows = [r[0] for r in a.fetchall()]
        log.append(('A', f'fetch -> {len(rows)}'))
        for r in rows:
            if prev is not None and not r > prev:
                if VERBOSE:
                    i = rows.index(r)
                    log.append(('A', 'ROWS around violation: ' + ' '.join(x[-4:] for x in rows[max(0, i-6):i+4])))
                    a.execute(f"SELECT count(*) FROM {tbl} WHERE a=4 AND b = %s", (r,))
                    log.append(('A', f'rows with violating key by index lookup: {a.fetchone()[0]}'))
                    a.execute(f"SELECT b, xmin_dummy FROM (SELECT b, 1 AS xmin_dummy FROM {tbl} WHERE a=4 AND b >= %s AND b <= %s) q ORDER BY b", (r, prev))
                    log.append(('A', 'visible keys between: ' + ' '.join(x[0][-4:] for x in a.fetchall())))
                return f"ORDER/DUP violation: {prev!r} then {r!r}", log
            prev = r
        if not rows:
            done = True
            break
        # B changes the range under the open scan, in its own txns
        for _ in range(0 if os.environ.get('NOB') else rnd.randint(1, 3)):
            b.execute("BEGIN")
            try:
                own_ops(b, rnd.randint(1, 6))
                b.execute("COMMIT")
            except psycopg2.errors.Error as e:
                if 'server closed' in str(e):
                    raise
                b.execute("ROLLBACK")
        if rnd.random() < 0.3 and not os.environ.get('NOAMID'):
            own_ops(a, rnd.randint(1, 3))
    try:
        a.execute("ROLLBACK")
    except Exception:
        pass
    return None, log


def main():
    A, B = conn(), conn()
    TB = f"cf{seed0}"
    A.cursor().execute(f"DROP TABLE IF EXISTS {TB}")
    A.cursor().execute(
        f"CREATE TABLE {TB} (a int NOT NULL, b text NOT NULL, c bigint, payload jsonb, "
        "PRIMARY KEY (a,b)) USING orioledb")
    bad = 0
    errs = 0
    for i in range(trials):
        seed = seed0 + i
        try:
            err, log = run_trial(seed, A, B, TB)
        except psycopg2.Error as e:
            if A.closed or B.closed or 'server closed' in str(e) or 'terminated' in str(e):
                print(f"CRASH seed={seed}: {str(e).splitlines()[0]}")
                sys.exit(2)
            # benign statement errors (serialization etc.): reset sessions
            try:
                A.cursor().execute("ROLLBACK")
            except Exception:
                pass
            try:
                B.cursor().execute("ROLLBACK")
            except Exception:
                pass
            errs += 1
            continue
        if err:
            bad += 1
            try:
                B.cursor().execute("ROLLBACK")
            except Exception:
                pass
            try:
                c = B.cursor()
                c.execute(f"SELECT orioledb_tbl_structure('{TB}'::regclass, 'nue')")
                with open(f"curfuzz_struct_{seed}.txt", "w") as f:
                    f.write(c.fetchone()[0])
            except Exception as e:
                print("structure dump failed:", e)
            print(f"seed={seed}: {err}")
            if VERBOSE:
                for who, st in log:
                    print("   ", who, st[:150])
            if bad >= 3:
                break
    print(f"done, trials={trials}, aborted_by_error={errs}, violations={bad}")


main()
