# Issue — Malformed WAL container with two finish records crashes streaming replica

**Summary**

There is a scenario of a tx abort after WAL flushing. That [error-injection](https://github.com/orioledb/orioledb/blob/c02a17a6c1e6d055e7e1e8c70e4ce307cdb5cb5b/src/recovery/wal.c#L782-L783) stands specifically for forcing such a scenario. Primary-side rollback works well and preserves all DB invariants, but a side-effect of such rollback is a malformed WAL sequence with both COMMIT and ROLLBACK records within a single WAL container. The replica fails immediately after encountering such a record in the stream. Furthermore, it seems that future primary recovery must also fail if such a WAL has been produced.

```
TRAP: failed Assert("rec->oxid != InvalidOXid"),
      File: "src/recovery/recovery.c", Line: 3712
```

→ replica `startup process` killed by signal 6 → postmaster `terminating any other active server processes` → replica cluster gone for the rest of the trial. Primary keeps running unaffected.


## How to reproduce

```
git checkout add_stress_bank_account_test
```

build the oriole and pg

```bash
RR_STORAGE_ENGINE=orioledb RR_REPLICA_MODE=streaming \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
RR_DURATION=15 RR_INJECTION_POINTS=orioledb-wal-flush-guarded \
RR_ASSERT_FIRINGS=0 RR_KILL_POSTMASTER=0 RR_PANIC_FATAL=0 \
TIMEOUT=180 ./test/t/crash/run_hunt.sh 10 only_wal_flush
```

Historical rate ~8/10 trials at this duration.

## The malformed WAL container

Real example from the replica log (trial 07:13:28):

```
WAL redo at 0/3018210 for OrioleDB resource manager/OrioleDB WAL container:
  XID      (562 192 0);
  COMMIT   (562 192 0 - xmin 552 csn 420);
  ROLLBACK (562 192 0 - xmin 552 csn 420);
```

A single OrioleDB WAL container holds:

1. one `WAL_REC_XID` that sets the current oxid to `(epoch=562, xid=192, locXid=0)`;
2. one `WAL_REC_COMMIT` finalising that oxid at CSN 420;
3. one `WAL_REC_ROLLBACK` finalising the *same* oxid (same xmin, same csn).

The two finish records share one `WAL_REC_XID`. The dispatcher's canonical-order invariant — "every finish record is preceded by its own `WAL_REC_XID` that sets `rec->oxid`" — is violated.


## Replica log sequence right before the TRAP

The replica's logs leave a fully attributable chain: every LOG/ereport line emitted *during* redo of a record carries a `CONTEXT:` line naming the record. The lines immediately before the TRAP (PID 2709537 is the dying startup process; other PIDs are parallel recovery workers running unrelated records):

```
CONTEXT: WAL redo at 0/3018210 ... COMMIT (562 192 0 - xmin 552 csn 420); ROLLBACK (562 192 0 - xmin 552 csn 420);
[PID 2709537] csn-trace walk_undo_stack exit pid=2709537 oxid=562   ← COMMIT sub-record fully processed
CONTEXT: WAL redo at 0/3018210 ... (same malformed record)
CONTEXT: WAL redo at 0/3018210 ... (same malformed record — ROLLBACK sub-record about to dispatch)
TRAP: failed Assert("rec->oxid != InvalidOXid"), File: "src/recovery/recovery.c", Line: 3712, PID: 2709537
```

Two things this tells us:

1. The COMMIT half of the malformed container is **fully processed** by the replica (`walk_undo_stack exit oxid=562`). The per-oxid bookkeeping for 562 is closed out exactly as if a normal COMMIT had arrived alone.
2. The TRAP fires when the dispatcher reaches the ROLLBACK half. The `CONTEXT:` line printed by the immediately-preceding ereport is the same malformed-record line — there is no ambiguity about which record the assertion is on.


## The exact code site that produces the malformed container

The narrowest reproducer is an injection placed **inside `wal_commit`**, between `add_finish_wal_record` and `flush_local_wal` (`src/recovery/wal.c`):

```c
XLogRecPtr
wal_commit(OXid oxid, TransactionId logicalXid, bool isAutonomous)
{
    ...
    add_finish_wal_record(WAL_REC_COMMIT, ...);    // ← appends COMMIT bytes
                                                   //   into the LOCAL in-process buffer
    [INJECTION FIRES HERE — ereport(ERROR)]        // ← bug reproducer site
    walPos = flush_local_wal(true, !isAutonomous); // ← NEVER REACHED for this tx
    ...
}
```

A hunt with this injection point (`orioledb-after-finish-wal-rec`) attached caught **2/2 BUGs (100% rate)** with the exact malformed-container signature:

| Trial | Malformed WAL container observed by replica |
|---|---|
| 20260526_170336 | `XID (139 352 0); COMMIT (139 352 0 - xmin 87 csn 118); ROLLBACK (139 352 0 - xmin 87 csn 118);` |
| 20260526_170426 | `XID (1261 416 0); COMMIT (1261 416 0 - xmin 1180 csn 902); ROLLBACK (1261 416 0 - xmin 1180 csn 902);` |

Both ended in `TRAP: failed Assert("rec->oxid != InvalidOXid") ... recovery.c:3712` → replica startup process killed by signal 6 → cluster gone.

### Why this site is the precise reproducer

When `ereport(ERROR)` fires *between* `add_finish_wal_record` and `flush_local_wal`:

1. `WAL_REC_COMMIT` has already been appended into the process-local WAL buffer.
2. `flush_local_wal` has NOT yet framed and submitted the buffer's contents to PG's shared XLog buffer. The buffer still holds the WAL_REC_COMMIT bytes, and `local_wal_has_material_changes` is still `true`.
3. The abort path invoked by PG's xact machinery calls `wal_rollback`, which itself calls `add_finish_wal_record(WAL_REC_ROLLBACK, ...)` — appending the ROLLBACK bytes into the **same local buffer that still holds WAL_REC_COMMIT**.
4. `wal_rollback`'s `flush_local_wal` then frames the entire buffer as one container — producing a single WAL container that holds both `WAL_REC_COMMIT` and `WAL_REC_ROLLBACK` for the same `WAL_REC_XID`. This is the malformed shape described above.

### Bug #1 vs Bug #2 — one root cause, separated by `flush_local_wal`

The same `ereport(ERROR)` raised at a different point along the same pipeline produces a different on-WAL fingerprint:

```
add_finish_wal_record(COMMIT) ─┐
                               │  ← ERROR here → Bug #1 (this issue):
                               │      malformed same-container COMMIT+ROLLBACK
                               │      → replica TRAP at recovery.c:3712
flush_local_wal             ───┤
                               │  ← ERROR here → Bug #2 (silent divergence):
                               │      COMMIT and ROLLBACK in SEPARATE containers
                               │      → replica replays both → primary/replica
                               │        state diverges → bank-invariant violated
[wal_commit returns]        ───┘
```

`flush_local_wal` is the watershed:

- **Before `flush_local_wal`** (this bug): the COMMIT bytes are still in the primary's process-local buffer. The abort's ROLLBACK append fuses into the same buffer. Result: same-container malformed record. Documented here.
- **After `flush_local_wal`** (Bug #2): the COMMIT container is already framed and submitted to PG's shared XLog buffer; walsender may have shipped it. The abort's `wal_rollback` starts a *new* container with its own `WAL_REC_XID` for the ROLLBACK. The two records reach the replica in separate, individually-valid containers. The replica's dispatcher applies the COMMIT, then the ROLLBACK — and the replica's state ends up different from the primary's (e.g. `replica sum_balance ≠ primary sum_balance`, `distinct_tokens 99 ≠ 100`, individual row swaps).

The two bugs are the same underlying defect — `ereport(ERROR)` is allowed to propagate after `WAL_REC_COMMIT` has been written into the process buffer — manifesting differently depending on whether the buffer had been flushed when the ERROR fired. Documented separately for the streaming-replica-divergence case in [`streaming_replica_issue.md`](./streaming_replica_issue.md) Bug #2.

### Updated reproducer using the finer injection point

```bash
RR_STORAGE_ENGINE=orioledb RR_REPLICA_MODE=streaming \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
RR_DURATION=30 RR_INJECTION_POINTS=orioledb-after-finish-wal-rec \
RR_ASSERT_FIRINGS=0 RR_KILL_POSTMASTER=0 RR_PANIC_FATAL=0 \
TIMEOUT=240 ./test/t/crash/run_hunt.sh 2 only_finish_wal_rec
```

Expected: **2/2 BUGs** with `CatchUpException` + `connection refused` on replica side (replica startup process dies during recovery). The corresponding `*_invariant_replica.log` will be small (~1–30 MB instead of the typical ~50–60 MB) because the replica dies mid-stream.

### Implications for the primary's crash recovery

The hypothesis that future primary recovery may also fail (see Summary) is now sharper. The malformed container, once flushed to disk on the primary, sits in pg_wal/ permanently. If the primary subsequently crashes (postmaster SIGKILL, machine reboot, etc.) before the next orioledb checkpoint advances `controlReplayStartPtr` past the malformed record's LSN, the primary's startup process will face the same container at the same `recovery.c:3712` assert and fail to recover for the same reason.

The narrower-trigger reproducer here doesn't yet exhibit the "primary cannot start" outcome under our test harness, because each trial reaches a clean shutdown (which writes an orioledb checkpoint advancing `controlReplayStartPtr` past every record on disk before the next startup). To observe primary-recovery failure, one would have to SIGKILL the postmaster mid-trial — after the malformed record has been flushed but before the next checkpoint completes. The harness `RR_KILL_POSTMASTER` machinery already supports this scenario; pairing it with this injection point would empirically prove the latent primary-recovery exposure.
