# Bank-account stress testing — onboarding

The `rr_stress_test.py` test pounds an OrioleDB table with a bank-account
invariant (sum of balances + unique tokens) under REPEATABLE READ and
randomly arms / disarms named injection points to expose data corruption
or cluster crashes. This document explains how to drive it, what to
look at when something goes wrong, and the methodology that has worked
to localise bugs to a minimal reproducer.

The original variant of this test is `repeatable_read_stress_test.py`.
The "rr_stress" copy adds env-var configuration, a log-grep crash
watchdog, a dangling-backend reaper, automatic PANIC-log archiving, and
an `RR_PANIC_FATAL` flag — leave the original alone, iterate on the
copy.

---

## Prerequisites

- Python venv at `../venv` with `testgres` installed.
- Postgres built with `--enable-injection-points` (e.g. installed at
  `~/pg17`) and on `PATH`. `pg_config` resolved from `PATH` is what
  testgres picks up.
- The matching OrioleDB extension (`orioledb.dylib` on macOS) installed
  into `$(pg_config --pkglibdir)`. After editing C sources, rebuild:
  ```
  make && make install
  ```
  Verify the install picked up your new injection points:
  ```
  strings $(pg_config --pkglibdir)/orioledb.dylib | grep '^orioledb-'
  ```

---

## Running the test

Activate the venv first:
```
source ../venv/bin/activate
```

Default invocation (3-minute run, full default config):
```
python3 -m unittest test.t.crash.rr_stress_test.RrStressTest
```

Single-knob example: trigger the known PANIC fast.
```
RR_INJECTION_POINTS='orioledb-csn-incremented' \
RR_DURATION=60 \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
python3 -m unittest test.t.crash.rr_stress_test.RrStressTest
```

### Env vars

| Variable | Default | Meaning |
|----------|---------|---------|
| `RR_DURATION` | 180 | seconds the writers/readers keep going |
| `RR_WRITERS` | 20 | concurrent writers |
| `RR_READERS_PK` / `RR_READERS_SK` / `RR_READERS_MIXED` | 6 each | readers per scan type |
| `RR_ACCOUNTS` | 100 | rows in the bank table |
| `RR_INJECTION_POINTS` | `ALL` | comma-separated subset filter (only the listed points are armed by `wal_chaos`); use `ALL` or unset to keep the file's full list |
| `RR_WAL_CHAOS_IDLE` | 0.1 | seconds between attach/detach cycles |
| `RR_PANIC_FATAL` | 1 | 1 → PG PANIC fails the test; 0 → tolerate PANIC, validate data invariants after recovery |
| `RR_INSTANCE` | unset | suffix for `tmp_check_t/<name>_<inst>_tgsn` so parallel runs don't collide |
| `TESTGRES_BASE_PORT` | 20000 | base port. Combine with `RR_INSTANCE` for parallel runs |

---

## Where logs and artefacts go

- Live PG log: `test/t/tmp_check_t/<myName>_tgsn/logs/postgresql.log`
- Saved PANIC logs: `test/t/crash/results/<UTC>_<inst>_<points>.log` —
  the test copies the PG log here whenever it sees a PANIC, so you can
  preserve evidence even though the next run wipes `tmp_check_t/`.
- Test totals are printed at the end of the run, e.g.
  ```
  [config] duration=60 writers=20 readers_pk=0 …
  [saved-log] /…/test/t/crash/results/…_orioledb-csn-incremented.log
  [totals] writes=9953 reads=0 conflicts=4826 first_crash_at=146.66s panic_lines=1
  [PANIC] 2026-…  PANIC: stuck spinlock detected at oxid_get_csn, src/transam/oxid.c:1545
  ```
- The test also reports `surviving backends: N` after `node.stop()`. A
  non-zero count means PG didn't shut down cleanly — usually a bug
  itself.

---

## Crash detection — what the test treats as a failure

| Signal | Source | When fires |
|--------|--------|------------|
| `panic_lines > 0` | tail of `postgresql.log` | The PG postmaster wrote a `PANIC:` line. Authoritative. |
| `node.stop()` failed | tearDown | Postmaster wouldn't shut down — usually means it already crashed. |
| Surviving backends > 0 | `ps` for our data dir | Real orphan; even tearDown's reaper saw it before killing. |
| Bank-account invariant violation | `final_total != expected_total`, etc. | Data corruption. |
| Snapshot inconsistency (`seen` list) | reader threads compared totals across two scans inside one RR snapshot | Torn snapshot. |

**Don't** trust connection-probe watchdogs to detect PANIC. Under heavy
load, fresh connections can time out or refuse for innocent reasons.
The log-tail watchdog in this test is the reliable signal; we got
burned trusting connection probes earlier.

The watchdog matches both `PANIC:` lines (emitted by `ereport(PANIC)`)
and `TRAP:` lines (emitted by `ExceptionalCondition` when an `Assert`
fires inside a critical section). The `orioledb-commit-assert`
injection produces `TRAP:` lines via the mcxt:1185 "no palloc in crit
section" assertion -- expected behaviour, treat the same as PANIC.

---

## Assert-chaos worker and identifying crashed PIDs

`assert_chaos_loop` is a separate worker (enabled via `RR_ASSERT_FIRINGS`)
that periodically arms `orioledb-commit-assert`, an injection point
placed inside a `START_CRIT_SECTION` at
[src/transam/undo.c#L2378](../../src/transam/undo.c#L2378). Attaching
the `error` action there is functionally equivalent to an `Assert(0)`
at that line -- raised inside the crit section, the `ereport(ERROR)`
escalates to PANIC and the cluster goes down via `abort()`. The worker
uses the wal-chaos pattern `attach -> time.sleep(0) -> detach` so the
injection lives for ~1 ms per arm cycle; the goal is to let only one
writer hit the fault before postmaster's `quickdie` fan-out kills the
rest.

The arm cadence is `RR_DURATION / (RR_ASSERT_FIRINGS + 1)` seconds.
Suggested config for crash-recovery stress: `RR_PANIC_FATAL=0
RR_ASSERT_FIRINGS=3 RR_DURATION=120` -- 3 crash+recovery cycles in 2
minutes.

### How to find which backends hit the assertion

Two complementary signals are written to `postgresql.log` (preserved
in `test/t/crash/results/` whenever a crash is detected):

1. **`TRAP: ...PID: NNNN` lines** -- each backend that hits the
   mcxt:1185 assertion writes one of these via `ExceptionalCondition`
   before `abort()`. Direct list of crashed PIDs.

2. **Pre/post traces around the injection** -- the orioledb code
   brackets the crit section with `elog(LOG, ...)`:
   ```
   commit-assert-trace pre  pid=NNNN oxid=...
   START_CRIT_SECTION();
     INJECTION_POINT("orioledb-commit-assert");
   END_CRIT_SECTION();
   commit-assert-trace post pid=NNNN oxid=...
   ```
   Every successful commit through this site produces both lines.
   A backend that hits the injection produces a `pre` line but its
   `post` never runs (the `abort()` between them kills the process),
   so unmatched `pre` lines correspond exactly to crashed PIDs.

To count crashed backends from a saved log:

```bash
LOG=test/t/crash/results/<your-saved-log>.log

# (1) Direct PID list from TRAP lines.
grep -oE "TRAP.*PID: ([0-9]+)" "$LOG" | sed -E 's/.*PID: //' | sort -u

# (2) Cross-check via pre/post trace counts -- a backend whose `pre`
#     count exceeds its `post` count hit the assert at some point.
python3 - "$LOG" <<'PY'
import sys, re, collections
pre = collections.Counter(); post = collections.Counter()
pid_re = re.compile(r'pid=(\d+)')
with open(sys.argv[1], errors='replace') as f:
    for line in f:
        if 'commit-assert-trace pre' in line:
            m = pid_re.search(line)
            if m: pre[int(m.group(1))] += 1
        elif 'commit-assert-trace post' in line:
            m = pid_re.search(line)
            if m: post[int(m.group(1))] += 1
crashed = [p for p in pre if pre[p] > post[p]]
print('crashed PIDs:', sorted(crashed))
print('total pre - post:', sum(pre.values()) - sum(post.values()))
PY
```

The two signals should agree exactly. If `TRAP:` lists N PIDs, the
pre/post-count diff totals to N and the set of unmatched-pre PIDs
matches the TRAP PID set.

### How many writers hit the assertion per arm cycle

Empirically (Python pg8000 over loopback, 20 writers, `sleep(0)`
between attach and detach): 0 - 3 writers per arm cycle, with **mean
~1.2 when the cycle catches anyone**. The distribution is:

- **0** -- the brief attach window happens to fall between any writer's
  commits. Not unusual at short durations / low writer counts.
- **1** -- the canonical case; only one writer slipped into the
  `START_CRIT_SECTION` before the worker's detach landed. Postmaster
  SIGQUITs the rest before they reach it.
- **2-3** -- the worker's `attach + commit + sleep(0) + detach + commit`
  round-trips on a `pg8000` connection take long enough (~1 ms) for
  multiple writers to enter the crit section. Each independently
  hits the assertion before postmaster's quickdie fan-out arrives.

If you need strictly one writer per cycle, tighten the window
further (e.g. submit attach + detach in a single transaction, or
inline both into a `node.safe_psql` round-trip). For
crash-recovery stress the >1 case is still useful -- it exercises
the multi-backend abort race that real signal-driven incidents
can also produce.

---

## Investigation methodology — bisecting bugs to a minimal repro

The test has many knobs, so when something fails, work top-down:

### Phase 1 — minimum duration

1. Confirm the bug at the default duration. With `RR_PANIC_FATAL=1` the
   test fails fast — don't bother with long runs you'd just discard.
2. Halve the duration repeatedly; keep all other knobs at default.
3. Stop when you find a duration that still triggers the failure in
   ≥50% of attempts (3–5 trials per duration is usually enough).

Failures here are probabilistic. Don't trust a single trial — repeat.

### Phase 2 — minimum injection-point set

Once a short, frequently-failing duration is established:

1. Run each candidate point **alone** via
   `RR_INJECTION_POINTS=<one-name>`, 3 trials each at the chosen
   duration.
2. If a single point reproduces the failure, you've found the culprit.
3. If no single point does, try pairs / halves of the list and bisect.

This step is the highest-value: it pins the failure to a specific
code-site, often pointing straight at the bug.

### Phase 3 — minimum writer / reader / cycle config

With one (or a few) culprit points:

1. Drop readers to 0 (`RR_READERS_*=0`). Many crashes don't need
   readers at all — readers just steal CPU from the writers.
2. Halve `RR_WRITERS` until the crash stops.
3. Try `RR_WAL_CHAOS_IDLE` longer — sometimes the bug needs frequent
   cycling, sometimes not.

The smallest config that still triggers the failure is your minimal
reproducer. Save the config in the bug report.

### Anti-patterns

- **Running >2 tests in parallel.** Each test spins a real PG and ~38
  threads. CPU contention starves writers, lowers commit rate, and
  changes failure timing. Use parallel only when the trials are
  obviously independent, and never more than 2.
- **Trusting "FAILED (errors=1)" without reading the trace.** It can
  mean a real bug or just `initdb` resource exhaustion. Always look at
  the actual exception.
- **Editing the test mid-run.** unittest reimports per process, so
  later trials in a `for`-loop pick up edits — use copies / new
  instances if you want consistent code across a batch.
- **Forgetting to rebuild after C changes.** Compare
  `orioledb.dylib`'s mtime with your edited C sources; rebuild and
  reinstall before relaunching.

---

## Worked example — `orioledb-csn-incremented` PANIC

This is the bug currently surfaced by the test:

- Trigger: `INJECTION_POINT("orioledb-csn-incremented")` in
  `src/transam/undo.c:2304`, fired between
  `pg_atomic_fetch_add_u64(&nextCommitSeqNo, 1)` and
  `current_oxid_commit(csn)`.
- ERROR there enters `AbortTransaction`, which runs
  `wal_rollback → apply_undo_stack → … → current_oxid_abort()`. Until
  that completes, the per-oxid xidBuffer entry stays in the
  `COMMITTING` state.
- Concurrent backends doing UPDATEs hit the visibility check in
  `oxid_get_csn` / `oxid_match_snapshot` — both of which busy-wait on
  the `COMMITTING` bit using `perform_spin_delay`. Under heavy abort
  pile-up the spin exceeds `NUM_DELAYS` → `s_lock_stuck` → PANIC.
- Minimal reproducer:
  - `RR_INJECTION_POINTS='orioledb-csn-incremented'`
  - `RR_DURATION=60`
  - `RR_WRITERS=2`, all readers 0  (still occasional; `RR_WRITERS=20`
    is reliable)
- Crash signature in saved log:
  ```
  PANIC: stuck spinlock detected at oxid_get_csn, src/transam/oxid.c:1545
  ```
  or `oxid_match_snapshot` at `oxid.c:1623` — same bit, two callers.
- Fix direction: replace the busy-wait on `COMMITTING` with a
  condvar/proc-array wait signalled by `current_oxid_commit` /
  `current_oxid_abort`. Heap doesn't have this issue because it uses
  LWLocks (queue + sleep) for visibility, not spinlocks.

This whole chain was reconstructed by following Phase 1 → 2 → 3 above.
That's the workflow to repeat for the next bug.

---

## Cleaning up

- Test's tearDown reaps any backend whose command line contains its own
  `data/` path. Other tests' clusters are left alone.
- If you need to nuke everything from interrupted runs:
  ```
  ps -Af | grep "tmp_check_t" | grep postgres | grep -v grep | \
      awk '{print $2}' | xargs kill -9
  rm -rf test/t/tmp_check_t/
  ```

---

## Files of interest

- `rr_stress_test.py` — the test itself. Iterate here.
- `repeatable_read_stress_test.py` — original / reference variant. Do
  not modify in routine experiments.
- `tx_flow.md` / `tx_flow_formatted.md` — narrative description of the
  OrioleDB transaction flow and where each injection point fires.
- `results/` — archived PG logs from runs that hit PANIC.
