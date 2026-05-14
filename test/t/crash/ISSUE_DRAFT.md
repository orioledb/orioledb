# Deadlock between in-progress UPDATE and concurrent abort cleanup, escalates to PANIC via spinlock budget

## Summary

When an OrioleDB transaction errors out **after `current_oxid_precommit()`
has set the `COMMITSEQNO_STATUS_CSN_COMMITTING` bit on its xidBuffer
slot but before `current_oxid_commit()` (or `current_oxid_abort()`)
clears it**, its in-progress abort path can deadlock against another,
unrelated UPDATE that is already holding a B-tree page-content lock and
waiting for the aborter's oxid to leave the `COMMITTING` state. The
wait is a busy-wait (`perform_spin_delay`) with `NUM_DELAYS` budget —
when the budget is exhausted the cluster PANICs with
`stuck spinlock detected at oxid_get_csn` or `at oxid_match_snapshot`.

The bug is reachable by any in-process `ereport(ERROR)` (or `Assert`,
`palloc` OOM) that fires inside `undo_xact_callback`'s
`XACT_EVENT_COMMIT` branch anywhere between the call to
`current_oxid_precommit()` and the call to `current_oxid_commit(csn)`
(`pg_atomic_fetch_add_u64(&nextCommitSeqNo, 1)` is just one point
inside that window). It is **not** triggered by signal-driven cancels
either: PG core wraps `CallXactCallbacks(XACT_EVENT_COMMIT)` in
`HOLD_INTERRUPTS()`/`RESUME_INTERRUPTS()`
([xact.c:2317-2482](https://github.com/postgres/postgres/blob/master/src/backend/access/transam/xact.c#L2317)),
so SIGTERM/SIGINT/SIGALRM flags raised here are deferred until after
`current_oxid_commit()` has already run. The trigger is therefore a
*code-level* fault — an `ereport`/`Assert`/`palloc` added inside
this window — not an asynchronous signal. SIGKILL/SIGQUIT also do not
trigger it: postmaster's `quickdie()` calls `_exit(2)` without running
XactCallbacks at all.

## Reproducer

A stress test that arms an injection point exactly at this window
reproduces the deadlock reliably:

- `INJECTION_POINT("orioledb-csn-incremented")` placed at
  [src/transam/undo.c, between the `pg_atomic_fetch_add_u64` for
  `nextCommitSeqNo` and `current_oxid_commit(csn)` in
  `undo_xact_callback`'s `XACT_EVENT_COMMIT` branch].
- Bank-account stress test: 20 writers transferring money + tokens
  under REPEATABLE READ, an attach/detach loop arming the point with
  the `error` callback every 0.1 s, default 60 s test duration.

Minimum reproducing command:

```bash
source ../venv/bin/activate

# Minimal repro (≥2 writers required, readers not needed):
RR_PANIC_FATAL=1 \
RR_INJECTION_POINTS='orioledb-csn-incremented' \
RR_DURATION=60 \
RR_WRITERS=2 \
RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
python3 -m unittest test.t.crash.rr_stress_test.RrStressTest

# Reliable repro (deterministic within ~3 minutes):
RR_PANIC_FATAL=1 \
RR_INJECTION_POINTS='orioledb-csn-incremented' \
RR_DURATION=60 \
RR_WRITERS=20 \
RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
python3 -m unittest -v test.t.crash.rr_stress_test.RrStressTest
```

`RR_PANIC_FATAL=1` makes the test add `restart_after_crash = off` to
`postgresql.conf`, so a PANIC takes the postmaster down and is
detected unambiguously via connection-refused.

PANIC reproduces 3/3 trials at W=20 R=0, ~67 % at W=2 R=0. The PANIC
appears 70–90 s after the *last* abort begins (`NUM_DELAYS` budget),
and is logged as one of:

```
PANIC: stuck spinlock detected at oxid_get_csn,    src/transam/oxid.c:1545
PANIC: stuck spinlock detected at oxid_match_snapshot, src/transam/oxid.c:1623
```

The same `COMMITTING`-bit cause; two different callers of the busy-wait.

## Cause

The deadlock is a hard A↔B cycle on two resources, exposed by an ERROR
in the CSN-increment-without-commit window:

```
PID A  (in-progress UPDATE)               PID B  (aborter, oxid=O)
─────────────────────────                 ────────────────────────
holds page-content lock blkno=P           commit hits ERROR at the
                                          INJECTION_POINT, oxid O still
                                          marked COMMITTING in xidBuffer
                                          AbortTransaction
                                          → undo_xact_callback(ABORT)
                                          → apply_undo_stack
                                          → modify_undo_callback
                                          → refind_page → lock_page(P)
                                                              ◀── queued, PGSemaphoreLock

modify path needs to read a
tuple whose xactInfo points
at oxid O:
  oxid_get_csn(O)        ◀── spin (perform_spin_delay)
  (or oxid_match_snapshot(O) on the read path)

A waits for B's oxid O to clear COMMITTING.
B waits for A to release page lock P.

A's spin exhausts NUM_DELAYS first → s_lock_stuck → PANIC.
```

Concretely, the busy-wait happens here (caller side):

- [src/btree/modify.c:487](src/btree/modify.c#L487) — `oxid_get_csn` is
  called inside the b-tree modify conflict-resolution path **while a
  page-content lock is held**.
- [src/btree/iterator.c:332](src/btree/iterator.c#L332) — same pattern
  for readers via `oxid_match_snapshot`.

And the spin loop itself:

- `oxid_get_csn` ([src/transam/oxid.c](src/transam/oxid.c)) — `while
  (true) { … perform_spin_delay(&status); }`.
- `oxid_match_snapshot` ([src/transam/oxid.c](src/transam/oxid.c)) —
  same shape.

`perform_spin_delay` is intended for *truly short* sections
(microseconds). The `COMMITTING` window in OrioleDB is bounded only by
`current_oxid_commit` / `current_oxid_abort` actually running, which
under abort-pile-up is no longer microsecond-scale.

## Trace evidence (one captured run, end-to-end-verified call chain)

Saved log: `test/t/crash/results/20260507_175410_solo_orioledb-csn-incremented.log`

This log was produced with explicit enter/exit traces in every function
named in the cycle above (`o_btree_modify`, `o_btree_normal_modify`,
`o_btree_modify_internal`, `o_btree_modify_handle_conflicts`,
`refind_page`, `lock_page`, `unlock_page`, `oxid_get_csn`,
`oxid_match_snapshot`, `current_oxid_abort`, `undo_xact_callback`,
`walk_undo_stack`, `walk_undo_range_with_buf`, `walk_undo_range`,
`modify_undo_callback`). The full call chain on both sides is observed
in the log; the panicker stops mid-`oxid_get_csn` and the aborter
stops mid-`lock_page wait` — exactly as the cycle predicts.

Holder side (PID 2819 — UPDATE `id=87`, then PANICs):

```
14:51:56.194  [2819] csn-trace o_btree_modify_internal enter pid=2819 action=2 oxid=4944
14:51:56.194  [2819] csn-trace refind_page enter           pid=2819 blkno=2050 level=0
14:51:56.194  [2819] csn-trace lock_page request           pid=2819 blkno=2050
14:51:56.194  [2819] csn-trace lock_page wait              pid=2819 blkno=2050 tail_procno=18
14:51:56.194  [2819] csn-trace lock_page got               pid=2819 blkno=2050 waited=1
14:51:56.194  [2819] csn-trace refind_page exit            pid=2819 blkno=2050 result=Success
14:51:56.194  [2819] csn-trace handle_conflicts enter      pid=2819 blkno=2050 opOxid=4944
14:51:56.194  [2819] csn-trace oxid_get_csn enter          pid=2819 oxid=4946 getRawCsn=0
                                                            ─── no exit, holds lock_page(2050)

14:54:09.983  [2819] PANIC:  stuck spinlock detected at oxid_get_csn, oxid.c:1579
              [2819] STATEMENT: UPDATE o_bank_account SET balance=…, token=… WHERE id=87
```

Aborter side (PID 2824 — owner of oxid=4946, COMMITTING):

```
14:51:56.196  [2824] csn-trace modify_undo_callback enter pid=2824 oxid=4946 action=2 blkno=2050 (iter=5)
14:51:56.196  [2824] csn-trace refind_page enter          pid=2824 blkno=2050 level=0
14:51:56.196  [2824] csn-trace lock_page request          pid=2824 blkno=2050
14:51:56.196  [2824] csn-trace lock_page wait             pid=2824 blkno=2050 tail_procno=13
                                                           ─── PGSemaphoreLock, never wakes
```

The same physical page (`blkno=2050`) and the same oxid (`4946`)
appear on both sides; the cycle is closed by direct identity, not by
inference.

(An earlier capture in
`test/t/crash/results/20260507_171931_solo_orioledb-csn-incremented.log`
shows the same pattern with PIDs 81343/81352 and oxid=2987.)

## Why the bug class is real in production builds

In production builds the `INJECTION_POINT` macro is a no-op — but the
window itself remains. Realistic in-process triggers:

- `pg_terminate_backend(pid)` / SIGTERM landing on a victim inside the
  window. SIGTERM sets `ProcDiePending`; the next
  `CHECK_FOR_INTERRUPTS` calls `ProcessInterrupts` →
  `ereport(FATAL, …)` which goes through the same abort path.
- Any future `ereport(ERROR)`/`Assert`/`palloc` OOM/`CHECK_FOR_INTERRUPTS`
  inserted into `current_oxid_commit` or its callees. The function is
  short today but its hot-path is on the critical commit edge.
- DEBUG-build assertion failures.

Signal-kill-class events (OOM kill, kill -9, hardware fault delivered
as SIGBUS/SIGSEGV) do *not* trigger this bug because the postmaster
recycles the cluster: `quickdie()` calls `_exit(2)` without running
`AbortTransaction`/XactCallbacks ([postgres.c quickdie comment](https://github.com/postgres/postgres/blob/master/src/backend/tcop/postgres.c#L2977)).

## Severity

- **Liveness:** any backend that hits the window can take the cluster
  down via PANIC ~70-90 s later. The path is reachable from a single
  `pg_terminate_backend` while UPDATE traffic is hot.
- **Recovery integrity (separate but adjacent issue):** in
  panic-tolerant runs (postmaster recycles, recovery from WAL),
  invariants are violated in 2/3 trials of the same stress test
  (`final total ≠ expected total`). That suggests a recovery-side bug
  in handling the same CSN-without-commit state. We're tracking that
  separately; opening this issue first because the deadlock is the
  bigger surface.

## Suggested fix direction

The cleanest narrow fix: **never busy-wait on `COMMITTING` while
holding a page-content lock.**

1. At each call site that today does
   `oxid_get_csn(oxid, …)` / `oxid_match_snapshot(...)` while a content
   lock is held — currently
   [src/btree/modify.c:487](src/btree/modify.c#L487) — check the CSN
   non-blockingly. If it comes back `COMMITTING`:
   1. release the content lock,
   2. wait on a condvar / semaphore signalled by both
      `current_oxid_commit` and `current_oxid_abort`,
   3. refind+relock the page and retry the modify step.

This pattern is the one PG heap uses (`XactLockTableWait` after the
buffer is released).

Alternative / complementary:

2. Replace the spin loop in `oxid_get_csn` and `oxid_match_snapshot`
   with a wakeup-on-flip primitive. This eliminates `s_lock_stuck`
   entirely as a possible outcome of any `COMMITTING`-bit observation.
3. Tighten the CSN-increment-without-commit window: defer
   `pg_atomic_fetch_add_u64(&nextCommitSeqNo, 1)` until *after* the
   per-oxid xidBuffer slot is filled, or write both atomically so
   waiters never observe the intermediate state.

Either (1) or (2) closes the bug class for the page-lock variant. (3)
makes the window structurally vanish.

## How to reproduce

Build orioledb against a Postgres configured with
`--enable-injection-points` and install (`make && make install`). The
test source is `test/t/crash/rr_stress_test.py`; PANIC traces are
archived to `test/t/crash/results/<timestamp>_solo_orioledb-csn-incremented.log`.

The minimum and reliable command lines are listed at the top of the
"Reproducer" section above.

## Files of interest

- `src/transam/undo.c` — injection-point site,
  `undo_xact_callback`, `XACT_EVENT_COMMIT` branch.
- `src/transam/oxid.c` — `oxid_get_csn`, `oxid_match_snapshot`,
  `current_oxid_abort`.
- `src/btree/modify.c` — line 487, the in-conflict-resolution
  `oxid_get_csn` call under page lock.
- `src/btree/iterator.c` — line 332, the read-path
  `oxid_match_snapshot` call.
- `src/btree/find.c` — `refind_page` MODIFY branch invoking
  `lock_page`.
- `src/btree/page_state.c` — `lock_page` /
  `unlock_page` semaphore queue.
- `test/t/crash/rr_stress_test.py` — stress reproducer.
- `test/t/crash/RR_STRESS_TESTING.md` — methodology notes.
