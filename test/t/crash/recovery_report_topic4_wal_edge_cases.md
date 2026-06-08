# Topic 4: Edge Cases in the WAL Stream That Recovery Must Handle Correctly

This document enumerates ten WAL-stream edge cases that OrioleDB recovery must
process correctly. For each case we describe the conceptual scenario, sketch
the WAL byte sequence, trace the relevant code path with `file:line`
citations, and call out the invariant that recovery preserves.

The discussion assumes the reader has already absorbed Topic 1 (baseline
PostgreSQL recovery), Topic 2 (PG <-> OrioleDB recovery interface), and
Topic 3 (the OrioleDB recovery internals). Topic 4 is exclusively about
what the WAL stream itself can throw at the redo path.

> Notation in the diagrams below:
>
> * `[XID 17]` — a `WAL_REC_XID` envelope record establishing the current oxid
> * `[INS]`, `[UPD]`, `[DEL]` — a `WAL_REC_INSERT`, `WAL_REC_UPDATE` or
>   `WAL_REC_DELETE` payload record (per-row, not per-page)
> * `[COMMIT]` / `[ROLLBACK]` — a `WAL_REC_COMMIT` / `WAL_REC_ROLLBACK`
>   terminator
> * `||` — checkpoint boundary in the WAL stream
> * `<<TAIL>>` — end of valid WAL discovered by `XLogReader`
>
> Records belonging to the same oxid are not necessarily contiguous in the
> stream — PG WAL is totally ordered by LSN, but multiple backends interleave
> arbitrarily.

## 1. Unfinished-Transaction WAL on Crash

### Scenario

A backend executes one or more `INSERT`/`UPDATE`/`DELETE` statements, fills
its 8 KiB local WAL buffer (`LOCAL_WAL_BUFFER_SIZE`,
`include/recovery/wal.h:193`), and either flushes it as part of the normal
buffer-rotation logic or simply leaves it there until COMMIT. Between
"records flushed" and "WAL_REC_COMMIT appended", the backend either crashes
(`SIGQUIT` from the postmaster cascade, segfault, kernel OOM kill) or the
whole cluster PANICs.

When the cluster restarts and replays WAL, the redo stream contains the
backend's modify records but no terminator. Recovery must (a) apply the
modify records as INPROGRESS rows whose visibility-state stays open, and
(b) eventually wipe them out so that no client ever sees an unfinished
transaction's effects.

### WAL byte sequence

```
... [XID 17] [INS k1] [INS k2] [UPD k3]  <<TAIL>>
                                          ^
                                          xlogreader stops here: no COMMIT
                                          or ROLLBACK record for oxid 17
```

### Code path

On the runtime side, `wal_commit()` at
`src/recovery/wal.c:342` is the only call site that emits
`WAL_REC_COMMIT`; if the backend dies before reaching that line the
record is never produced. Conversely, `wal_rollback()` at
`src/recovery/wal.c:416` is the *only* producer of `WAL_REC_ROLLBACK`,
and it is invoked from `undo_xact_callback`'s `XACT_EVENT_ABORT` case —
which itself does not run when the backend dies on `SIGQUIT` (`quickdie`
does not invoke `RegisterXactCallback` hooks, see `CLAUDE.md` "SIGQUIT
mid-undo-replay"). The redo stream therefore has neither sentinel for
this oxid.

In replay, `replay_on_record()` is the dispatcher
(`src/recovery/recovery.c:3678`). For each modify record it calls
`recovery_switch_to_oxid()` followed by `apply_modify_record()`
(`src/recovery/worker.c:671`), and the latter applies the change with a
hard-coded `COMMITSEQNO_INPROGRESS` (`src/recovery/worker.c:686, 690`):

```c
apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
...
apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
```

Because no `WAL_REC_COMMIT` / `WAL_REC_ROLLBACK` is ever read, the
`recovery_finish_current_oxid()` call at `src/recovery/recovery.c:3739`
never runs for oxid 17. At end-of-WAL,
`o_recovery_finish_hook()`/`recovery_finish()`
(`src/recovery/recovery.c:1620`) walks every still-`INPROGRESS`
`RecoveryXidState` and calls `walk_checkpoint_stacks(cur_state,
COMMITSEQNO_ABORTED, ...)` at `src/recovery/recovery.c:1660`. That call
flips the in-memory CSN to `COMMITSEQNO_ABORTED` and runs the per-undo
stack roll-back, undoing every modify record that has been applied for
oxid 17.

### CSN state machine and persistence

The three states `INPROGRESS -> CSN_COMMITTING -> final-CSN` live in the
shared `xidBuffer` circular slot for the oxid
(`src/transam/oxid.c:594`-`627`). On the *primary* path,
`current_oxid_precommit()` (`src/transam/oxid.c:1407`) writes the
`CSN_COMMITTING` placeholder, `current_oxid_commit()`
(`src/transam/oxid.c:1492`) overwrites it with the final CSN, and
`current_oxid_abort()` (`src/transam/oxid.c:1535`) overwrites it with
`COMMITSEQNO_ABORTED`. None of these is persisted on its own — the only
durable record is the COMMIT/ROLLBACK marker in the WAL stream. The
shared-memory state is reconstructed from scratch on each restart.

For oxid 17 the recovery process therefore:

1. Sees the modify records and inserts the tuples into the in-memory PK
   B-tree with a per-tuple undo record carrying `oxid=17,
   csn=COMMITSEQNO_INPROGRESS`.
2. Hits end of WAL.
3. Inside `recovery_finish()` the dangling oxid is forced to
   `COMMITSEQNO_ABORTED`, and undo replay deletes the tuples
   (`src/recovery/recovery.c:1985`,
   `walk_checkpoint_stacks(cur_recovery_xid_state, COMMITSEQNO_ABORTED,
   ...)`).
4. Any client connecting after recovery completes sees the table as
   though oxid 17 had never run — because (a) the tuples are gone, and
   (b) even if a visibility check raced ahead it would call
   `oxid_get_csn()` (`src/transam/oxid.c:1611`), find
   `COMMITSEQNO_INPROGRESS` or `COMMITSEQNO_ABORTED`, and treat the row
   as invisible.

### Invariant preserved

> *No partial transaction's effects are ever visible after crash recovery
> completes.*

This holds because the visibility check is CSN-based, the CSN is never
`COMMITSEQNO_NORMAL` for an oxid lacking a `WAL_REC_COMMIT` in the WAL
stream, and the final cleanup pass converts dangling INPROGRESS to
ABORTED before the cluster opens for connections.

## 2. Mid-COMMIT Crash (WAL_REC_COMMIT Flushed, Bookkeeping Lost)

### Scenario

A backend's `wal_commit()` at `src/recovery/wal.c:342` appends
`WAL_REC_COMMIT` to its local buffer, `flush_local_wal(true, ...)` at
`src/recovery/wal.c:343` pushes the buffer through `XLogInsert` and then
`XLogFlush` (because the `synchronous_commit` path waits for durability).
The record is on disk. The backend now returns into
`undo_xact_callback`'s `XACT_EVENT_COMMIT` branch
(`src/transam/undo.c:2250`) and proceeds with the *in-memory* commit
bookkeeping: precommit-to-CSN_COMMITTING placeholder
(`current_oxid_precommit()`, `src/transam/undo.c:2321`), grab the global
CSN, flip the per-oxid slot to that final CSN
(`current_oxid_commit(csn)`, `src/transam/undo.c:2368`), run
`on_commit_undo_stack` for each undo type
(`src/transam/undo.c:2380-2383`).

If the backend is killed (SIGQUIT cascade from a peer PANIC, segfault,
power loss) between `XLogFlush` and `current_oxid_commit()`, the WAL on
disk says "committed", but the only thing in memory was a per-oxid
`CSN_COMMITTING` placeholder or even just `INPROGRESS`. Both are
ephemeral — the shared `xidBuffer` does not survive crash.

### WAL byte sequence

```
... [XID 17] [INS k1] [UPD k2] [COMMIT 17]   ## flushed to disk
                                          ^
                                          backend died here, before
                                          current_oxid_commit() ran
```

### Code path

The "WAL says committed" promise is what recovery must honour. In replay,
once `xlogreader` consumes the `WAL_REC_COMMIT` record, control reaches
`replay_on_record()`'s `WAL_REC_COMMIT` branch at
`src/recovery/recovery.c:3697-3751`. The branch:

1. Reads the durable `xmin` from the record
   (`src/recovery/recovery.c:3706`).
2. Calls `recovery_finish_current_oxid(commit ? COMMITSEQNO_MAX_NORMAL -
   1 : COMMITSEQNO_ABORTED, xlogPtr, -1, sync)` at
   `src/recovery/recovery.c:3739`.

The first argument is a CSN sentinel: `COMMITSEQNO_MAX_NORMAL - 1`
stands in for "this oxid committed; a real CSN will be re-assigned from
the global counter as we replay". `recovery_finish_current_oxid()` at
`src/recovery/recovery.c:1872` invokes `set_oxid_csn(oxid, finalCsn)`
(see `src/transam/oxid.c:574`) to install that CSN into the in-memory
`xidBuffer` slot for the oxid — the exact thing the dying backend never
got to do.

Because the `WAL_REC_COMMIT` record carries enough information (the
`xmin` and CSN at record-write time), recovery reconstructs the state
that was lost.

### The `XLogFlush`-vs-bookkeeping window

Conceptually the timeline is:

```
   primary timeline
   ---------------
   wal_commit()        <-- WAL_REC_COMMIT appended to local buf
     flush_local_wal() <-- XLogInsert + XLogFlush
                          ===>>> COMMIT durable in WAL
     <return>
   XACT_EVENT_COMMIT
     <flushPos = XactLastCommitEnd, XLogFlush(flushPos)>
                          ===>>> redundant flush, already durable
     current_oxid_precommit()                         <-- in-memory only
     csn = pg_atomic_fetch_add(nextCommitSeqNo)       <-- in-memory only
     INJECTION_POINT("orioledb-csn-incremented")      <-- crash window
     current_oxid_commit(csn)                         <-- in-memory only
     on_commit_undo_stack()                           <-- in-memory only
```

The crash-test harness deliberately injects `error` at
`orioledb-csn-incremented` (`src/transam/undo.c:2360`) to exercise *this
exact window*: WAL durable, CSN never installed. Recovery's job is to
replay the durable record and re-derive the missing state.

The `orioledb-commit-assert` injection a few lines earlier
(`src/transam/undo.c:2313`) sits in a `START_CRIT_SECTION` bracket and
fires *after* the WAL has flushed but *before* `current_oxid_precommit`;
that's the same in-memory window viewed from a slightly earlier point.
Both injection points map to the same recovery contract: WAL_REC_COMMIT
durable => recovery must treat the transaction as committed.

Note also `src/recovery/wal.c:319-340`, the
`orioledb-before-pre-commit-wal-finish` injection — that one PANICs
*before* the COMMIT record is appended, so the recovery behaviour is the
Case 1 path, not Case 2. The two injection points together cover both
sides of the WAL durability boundary.

### Invariant preserved

> *If `WAL_REC_COMMIT` for oxid X is durable, all of oxid X's effects
> become visible after recovery; if it is not, none of them do.*

The boundary is the WAL flush. There is no "half-committed" outcome
because the in-memory CSN/undo bookkeeping is reconstructed during
replay from the durable record, exactly as it would have been on the
primary path — only that the original backend is no longer alive to do
it.

## 3. Torn / Partial WAL Records at the Tail

### Scenario

A crash interrupts the kernel's writeback of a WAL page. The on-disk
state holds the first few sectors of a logically larger record but not
the rest, or holds the previous segment's recycled bytes spuriously
matching an old prev-link. Recovery must detect that the tail of the
stream is corrupt-by-truncation and stop replaying *before* the
incomplete record — without ever passing partial bytes to OrioleDB's
`replay_on_record()`.

This is entirely a PostgreSQL-layer concern. OrioleDB does not see the
record at all until `xlogreader` has validated it.

### WAL byte sequence

```
LSN 0/45000000  [page hdr] [XID 17] [INS k1]
LSN 0/45000200  [page hdr] [UPD k2] [DEL ...
LSN 0/45000400  <garbage from a previous WAL cycle, written by another
                 process that never reached XLogFlush>
                                                <<TAIL>>
```

The `xl_prev` field of the would-be next record points somewhere stale,
the CRC over the would-be record's bytes does not match, or the page
header's `xlp_magic` is wrong because the page was never reformatted.
Any of those conditions is sufficient to stop the parse.

### Code path

The validation funnel inside `xlogreader.c`:

* `XLogReaderValidatePageHeader()`
  (`src/backend/access/transam/xlogreader.c:1234`) checks
  `xlp_magic == XLOG_PAGE_MAGIC`, `xlp_pageaddr == recptr`, and the
  short/long page header invariants. A garbage page produces an
  "invalid magic number" diagnostic and returns false.
* `ValidXLogRecordHeader()`
  (`src/backend/access/transam/xlogreader.c:1136`) checks
  `xl_tot_len >= SizeOfXLogRecord`, `RmgrIdIsValid(xl_rmid)`, and
  crucially the *prev-link* (`xl_prev != PrevRecPtr` => bail). The
  comment at `src/backend/access/transam/xlogreader.c:1175-1185`
  explicitly calls out "torn WAL pages where a stale but valid-looking
  WAL record starts on a sector boundary".
* `ValidXLogRecord()` (`src/backend/access/transam/xlogreader.c:1202`)
  CRC-checks the *whole* record body — both the header and the data —
  to catch any partial overwrite that nonetheless yielded a plausible
  header. The CRC is initialised with `INIT_CRC32C`,
  data-then-header-up-to-`xl_crc` (`xlogreader.c:1210-1213`), and
  compared to the record's stored `xl_crc`.

Above those, `XLogDecodeNextRecord()`
(`src/backend/access/transam/xlogreader.c:528`) handles the
multi-page-spanning case via the `XLP_FIRST_IS_CONTRECORD` flag
(`xlogreader.c:625`, `756`), the `XLP_FIRST_IS_OVERWRITE_CONTRECORD`
escape hatch (`xlogreader.c:748`), and the "no contrecord flag" bailout
at `xlogreader.c:756`.

`ReadRecord()` in `xlogrecovery.c:3179` wraps the reader and, on the
recovery side, treats a NULL return as "tail reached". It records
`abortedRecPtr` / `missingContrecPtr` from the reader at
`src/backend/access/transam/xlogrecovery.c:3215-3220`. The recovery main
loop at `src/backend/access/transam/xlogrecovery.c:1842` is the consumer
of `ReadRecord` — when the tail is hit it falls out of the loop and
proceeds to `EndOfLog` finalisation.

The `emode_for_corrupt_record()` helper
(`src/backend/access/transam/xlogrecovery.c:428`) downgrades PANIC to
LOG for "looks-like-truncated-tail" cases: a CRC mismatch right at the
end of WAL is treated as "we just hit the end of valid WAL", not as a
corruption that should crash the cluster.

### Implications for OrioleDB

`replay_on_record()` (`src/recovery/recovery.c:3678`) is never reached
for a torn record. The OrioleDB redo dispatcher only ever sees records
that already passed `ValidXLogRecord`'s CRC check.

The interesting interaction is the **per-record offset bookkeeping**:
OrioleDB's wal reader (`src/recovery/wal_reader.c`) parses a *single*
PG WAL record as a *container of* several OrioleDB WAL records (XID,
modify, finish, etc.). If the PG record passed CRC, the container is
self-consistent — but the container itself has a version byte and
per-record `recType` byte that the OrioleDB reader still has to
validate. See `wal_parse_container()` invocation in `replay_container`
at `src/recovery/recovery.c:4102-4133`; an OrioleDB-side malformed
container would return `WALPARSE_*` other than `WALPARSE_OK` and stop
replay of *that container*. Recovery as a whole continues with the next
PG record.

### Invariant preserved

> *No bytes from a torn or truncated WAL record reach OrioleDB's redo
> dispatcher; the recovery cursor stops cleanly at the last fully
> validated record.*

The boundary is the CRC32C check inside `ValidXLogRecord`. The recovery
process resumes new-WAL-generation at exactly that boundary, and an
`OVERWRITE_CONTRECORD` marker is written so future readers know to
ignore the abandoned partial.

## 4. WAL Records Past Last Checkpoint That Touch Already-Flushed Pages

### Scenario

A checkpoint started at LSN `L_chkp` flushed a B-tree page P at its
current state (call it version V). Backends continued running, emitted
modify records `R_i` (at LSN > `L_chkp`) that further mutate the same
key range, and some of those modifications may have been applied to P's
in-memory image and re-flushed before the cluster crashed. After
restart, recovery begins replay at the last completed checkpoint's redo
pointer and arrives at `R_i`. Recovery must avoid double-applying the
modification to a page whose on-disk version already reflects it.

In stock PostgreSQL the answer is "compare `record->lsn` against
`PageGetLSN(page)`; skip if `page LSN >= record LSN`". OrioleDB does
not store an LSN in `BTreePageHeader` (see `OrioleDBPageHeader` at
`include/orioledb.h:356-361` and `BTreePageHeader` at
`include/btree/page_contents.h:112-142`). Instead each page carries a
`checkpointNum`, an undo-stack `undoLocation`, and the CSN of its most
recent modification (`page_contents.h:117-118`). The skip rule is
expressed differently.

### WAL byte sequence

```
... [CHECKPOINT redo=L_chkp] [..] [INS p k1] [..] [UPD p k2] [..]   <<TAIL>>
                                  ^^^^^^^^^^         ^^^^^^^^^^
                                  R_1                R_i
                                  may or may not be reflected on disk
                                  depending on whether the writer
                                  flushed P after applying these
```

### Code path

The redo path's *gate* is at `src/recovery/recovery.c:1191`:

```c
if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
{
    if (!replay_container(msg_start, msg_start + msg_len, recovery_single,
                          record->ReadRecPtr, record->EndRecPtr))
        ...
}
```

`controlReplayStartPtr` is the per-checkpoint "begin replay here" marker
written into the control file by `src/checkpoint/checkpoint.c:1392`
(`checkpoint_state->replayStartPtr = get_checkpoint_xlog_ptr()`). Any
WAL record older than this LSN is silently skipped. That's the *coarse*
skip — it covers the case where the checkpoint already captured the
effect of those records in the on-disk image.

Within the surviving range (`ReadRecPtr >= controlReplayStartPtr`), the
fine-grained "this page already has the modification" decision is *not*
made up front by comparing an LSN-on-page. Instead it falls out of the
B-tree replay path:

* A WAL_REC_INSERT/UPDATE/DELETE record carries the *full key/tuple*.
* `apply_modify_record()` (`src/recovery/worker.c:671`) walks the tree
  to locate the key and either inserts/updates/deletes it. If the
  modification has already been applied (the on-disk page already
  contains the post-state), the tree-modification machinery either
  performs a no-op (`apply_btree_modify_record` handles the
  "key already present at the post-state" case as idempotent) or
  rewrites the same bytes — net effect is zero.
* For toast/SK consistency, the `toast_consistent` boundary at
  `src/recovery/recovery.c:1171-1188` further gates whether the
  modification reaches the secondary-index tree at all; secondary
  indices are not touched until the WAL stream has crossed the toast
  consistency point (Case 5).

The `checkpointNum` field on the page is the *cross-checkpoint*
ordering datum. When a page is read from disk during recovery, the page
header's `checkpointNum` is compared against the current
`startup_chkp_num = checkpoint_state->lastCheckpointNumber`
(`src/recovery/recovery.c:1042`). A page whose `checkpointNum` is less
than the current value is loaded fresh from the previous checkpoint's
on-disk extent; a page whose number matches is taken at face value.
This is the OrioleDB equivalent of "page LSN >= WAL LSN ⇒ trust the
page".

### What if the on-disk page is newer than the WAL record

The OrioleDB invariant: a page is only written to disk by `walk_page`
inside the checkpointer or the bgwriter, and those code paths cannot
emit a page whose `csn`/`undoLocation` references a transaction that is
not yet durable in the WAL. Concretely:

* The undo stack for an in-progress oxid is held in memory; flushing a
  page that references that undo stack would corrupt visibility after
  crash. The page-state machine in `src/btree/page_state.c` prevents
  this via `lock_page_with_tuple` + the `O_BTREE_FLAG_PRE_CLEANUP`
  protocol: a page is only marked clean (and thus eligible to be
  written) after all of its undo references point at durable
  (committed-with-WAL-flushed or aborted-with-WAL-flushed) oxids.
* The checkpoint sets `replayStartPtr` *before* it picks up the
  oTables/oSysTrees locks (`checkpoint.c:1389-1392`); any modification
  whose WAL record lands at an LSN < `replayStartPtr` has already been
  serialised into the checkpoint snapshot. The page-on-disk being
  "newer than" the WAL record therefore implies the WAL record is on
  the wrong side of the `replayStartPtr` gate and is skipped by the
  comparison at `recovery.c:1191`.

### Invariant preserved

> *No modification is applied twice. Recovery replays exactly the set
> of WAL records strictly after `controlReplayStartPtr`, and the
> on-disk page state at that LSN reflects everything strictly before
> it.*

The `controlReplayStartPtr` comparison at `src/recovery/recovery.c:1191`
is the load-bearing site.

## 5. WAL Touching a Tree Whose Checkpoint Metadata Wasn't Fully Written

### Scenario

OrioleDB's checkpoint walks B-trees in a fixed order — primary keys
first, then secondaries, then TOAST. The checkpoint's *redo pointer* is
not a single LSN; it is a triple:

* `replayStartPtr` — applies to user-data PK/SK records
  (`src/checkpoint/checkpoint.c:1392`),
* `sysTreesStartPtr` — applies to records targeting system trees
  (`src/checkpoint/checkpoint.c:1407`),
* `toastConsistentPtr` — applies to records that touch SK / require a
  consistent TOAST view (`src/checkpoint/checkpoint.c:1426-1428`).

These three LSNs differ. A crash mid-checkpoint may leave the on-disk
state with PK pages reflecting the snapshot up to `replayStartPtr` but
SK trees only reflecting the snapshot up to a strictly later (or
strictly earlier) point. Recovery has to use the correct LSN as the
gate for each kind of record.

### WAL byte sequence

```
   [CHKP_BEGIN]
       ^---- replayStartPtr
   [..pk-only modifies..]
       ^---- sysTreesStartPtr
   [..systree modifies..]
       ^---- toastConsistentPtr
   [..sk+toast modifies..]
   [CHKP_END]
        ^---- if missing or corrupt, recovery falls back to the
              previous checkpoint and re-derives all three pointers
   <<TAIL>> or <<bytes from next workload>>
```

If the crash happens after `replayStartPtr` was written but before
`toastConsistentPtr` was committed, recovery sees an *earlier*
checkpoint as the last-complete one. On disk, however, individual SK
pages may already carry post-`replayStartPtr` content because the
checkpointer wrote them in tree order before the control file got
finalised.

### Code path — three boundary checks

Each gate sits at a different point in the replay pipeline:

* **PK-data gate** at `src/recovery/recovery.c:1191`:
  `if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr) { ... replay_container(...); }`.
* **Systree gate** at `src/recovery/recovery.c:3941`:
  `if (ctx->sys_tree_num > 0 && ctx->xlogRecPtr >= checkpoint_state->controlSysTreesStartPtr)`
  selectively applies systree modifies. The systree may be on either
  side of `controlReplayStartPtr` since `sysTreesStartPtr` is recorded
  *after* `replayStartPtr` (`checkpoint.c:1407`).
* **TOAST/SK gate** at `src/recovery/recovery.c:1171-1189`:

  ```c
  if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr && !toast_consistent)
  {
      if (!recovery_single)
          workers_synchronize(record->ReadRecPtr, true);
      apply_pending_sk_fixups();
      toast_consistent = true;
      if (!recovery_single)
          workers_notify_toast_consistent();
  }
  ```

  Before crossing this boundary, workers apply modifies *only* to the
  PK and to TOAST. Secondary-index trees are deliberately untouched.
  `apply_modify_record()` in `src/recovery/worker.c:683` switches its
  behaviour on `toast_consistent`:

  ```c
  if (descr && toast_consistent)
      apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
  else
      apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
  ```

  Pre-toast-consistent it applies the record only to the *primary*
  B-tree (or to TOAST), never to SK; post-toast-consistent it applies
  to the full table descr so SKs are kept in sync.

### The `toastConsistentPtr` race and pending-SK-fix-ups

The checkpoint may have observed PK modifications that did not yet have
their SK counterparts written. To bridge the gap it dumps every
in-flight oxid that touched the PK pre-checkpoint into the xid file as
`XidRecPendingSkFixup` records.

At recovery start, the xid file is parsed in `src/recovery/recovery.c`
around the loop containing `record_pending_sk_fixup(...)` at
`src/recovery/recovery.c:905`. Each `PendingSkFixup` carries an
`(oxid, undoLocation)` pair pointing into the PK undo log. The list
sits idle until the recovery LSN cursor crosses `toastConsistentPtr`.
At that point `apply_pending_sk_fixups()`
(`src/recovery/recovery.c:600`, called from
`src/recovery/recovery.c:1184`) walks the list and for each entry calls
`apply_one_pending_sk_fixup()` (`src/recovery/recovery.c:326`), which
reads the PK undo record back, finds the current PK row, and emits the
matching DELETE-old/INSERT-new SK pair through the same recovery
worker queue as a normal modify.

The result: at the moment `toast_consistent` is set to true, the SK
trees catch up to the PK tree in one batched, deterministic pass — no
matter which side of the half-finished checkpoint they were left on.

### Correct behaviour without re-litigating the bug

The branch's *active investigation* is whether a particular race
between (a) the drained-pocket commit window and (b) the SK fix-up
boundary can leave the SK with one extra (or one missing) token under
chaos. That investigation is documented elsewhere. For the purposes of
Topic 4, the correct behaviour is the boundary protocol:

1. Replay only PK / TOAST until `controlToastConsistentPtr`.
2. Drain worker queues to a synchronisation barrier
   (`workers_synchronize(record->ReadRecPtr, true)`).
3. Replay the pending-SK-fix-up batch synthesised from the PK undo log.
4. Flip `toast_consistent = true` so workers start mirroring all
   subsequent modify records to SK as well.

Steps 2 and 3 are what makes the cross-tree consistency point atomic
from the redo dispatcher's perspective.

### Invariant preserved

> *At every moment after `toast_consistent = true`, every PK row that
> exists has exactly the corresponding SK tokens; recovery does not
> publish a half-synchronised cross-tree state to backends.*

Because the worker queues are drained before SK replay begins, no
backend can ever observe an SK-without-PK or PK-without-SK except
during the controlled pending-SK-fix-up pass which itself runs while
the recovery process holds the worker synchronisation barrier.

## 6. Out-of-Order Interleaved WAL from Multiple Backends

### Scenario

PostgreSQL WAL is a single byte stream, totally ordered by LSN. But the
*producers* are many concurrent backends, each running its own
transaction. The OrioleDB WAL container produced by a backend's
`flush_local_wal()` is atomic in the WAL stream — its records inside
one container are contiguous — but containers from different backends
interleave arbitrarily. A single oxid's records may therefore appear
across many non-contiguous WAL ranges, separated by other oxids'
containers.

```
... | container(oxid=17) | container(oxid=22) | container(oxid=17) |
        ^   [XID 17] [INS]      [XID 22] [INS] [INS]    [UPD] [DEL]
            [COMMIT was not flushed yet]
```

Recovery must reassemble each oxid's logical stream — its undo stack,
checkpoint undo stacks, retain location, and per-undo-log offsets —
from the interleaved input. A naïve "global current oxid" approach
would lose state every time the redo cursor crossed into another
backend's container.

### Per-oxid state and the switch protocol

OrioleDB recovery maintains one `RecoveryXidState` per encountered
oxid, keyed by oxid value in a hash table (`recovery_xid_state_hash`).
The single global `cur_recovery_xid_state` pointer
(`src/recovery/recovery.c:219`) tracks which oxid is "currently being
applied" by the dispatcher. The switch is performed by
`recovery_switch_to_oxid()` at `src/recovery/recovery.c:1716`.

Two crucial pieces of state are spilled to / restored from the per-oxid
hash entry across switches:

1. **Per-undo-log current location.** Before yielding,
   `get_cur_undo_locations(&cur_state->undo_stacks[i], (UndoLogType) i)`
   saves the per-undo-log cursor into the outgoing oxid's state
   (`src/recovery/recovery.c:1728-1741`).
2. **Per-undo-log retain location and the global retain-heap link.**
   Same loop reads `curRetainUndoLocations[i]` and updates the
   `retain_undo_queues[i]` pairing heap so that vacuuming and
   undo-buffer recycling do not advance past records the suspended
   oxid still needs.

On entry to the incoming oxid, the symmetric `set_cur_undo_locations`
call at `src/recovery/recovery.c:1756` re-installs the saved per-undo
cursors. If the oxid is being encountered for the first time, the
incoming-side branch at `recovery.c:1762-1796` initialises the state to
`COMMITSEQNO_INPROGRESS`, invalid undo locations, no flush requirements,
and adds the oxid to the xmin pairing heap.

### How records advertise their oxid

`WAL_REC_XID` records (`include/recovery/wal.h:67-74`) sit at the
beginning of every container that switches oxid. The dispatcher
processes them at `src/recovery/recovery.c:3688-3691`:

```c
case WAL_REC_XID:
    advance_oxids(rec->oxid);
    recovery_switch_to_oxid(rec->oxid, -1);
    break;
```

`advance_oxids()` bumps `nextOxid` in shared memory; `recovery_switch_
to_oxid()` performs the state spill/restore described above. Every
modify record after the switch is interpreted against the new oxid's
saved state — which means each per-oxid undo stack grows in the
correct order, even though the records are physically interleaved on
disk.

The runtime side guarantees a `WAL_REC_XID` is emitted whenever a
backend starts writing into its local buffer for a fresh oxid (see the
`local_wal_contains_xid` check in `wal_commit()` /
`wal_rollback()` / `add_finish_wal_record()` at
`src/recovery/wal.c:316, 413`). A container that lacks `WAL_REC_XID`
implies the recovery dispatcher should keep the previous oxid — which
only happens when the same backend's next container immediately
follows.

### Undo location pointer per record

Each modify record carries enough information to extend the oxid's
undo stack by one: the tuple (or key) bytes, plus the implicit
ordering. The actual undo *record* is reconstructed by
`apply_modify_record()` -> `apply_btree_modify_record()` /
`apply_tbl_modify_record()`, which walks the B-tree and calls
`make_undo_record()` to push a new undo item onto the current oxid's
in-memory undo log. The undo-log offset advanced as a side effect is
captured by `get_cur_undo_locations()` next time the oxid yields.

The `cur_undo_locations` array (one per `UndoLogType` —
`UndoLogRegular`, `UndoLogReserved`, `UndoLogSystem`) is the per-oxid
mutable state during application; the `RecoveryXidState.undo_stacks`
hash entry is the spill slot.

### Interaction with COMMIT and checkpoint stacks

`recovery_finish_current_oxid()`
(`src/recovery/recovery.c:1872`) flushes the per-oxid state to the CSN
slot when the oxid's `WAL_REC_COMMIT` or `WAL_REC_ROLLBACK` is
processed. Its first action is to call `set_oxid_csn(oxid, csn)` with
the post-commit CSN; its second is to walk the *checkpoint undo
stacks* attached to the state (`checkpoint_undo_stacks` list head at
`src/recovery/recovery.c:1781`) plus the current per-undo locations.
For an aborted oxid, `walk_checkpoint_stacks(cur_recovery_xid_state,
COMMITSEQNO_ABORTED, ...)` is the rollback driver. For a committed
oxid, the same call with the assigned CSN releases retain holds.

### Invariant preserved

> *Recovery applies the modifications of each oxid in the order they
> were appended to its local buffer on the primary, even though the PG
> WAL stream interleaves containers from many backends.*

Because the per-oxid undo cursor is saved on every switch and restored
on every re-entry, the redo dispatcher behaves as if it were processing
N independent serial streams whose union happens to be the actual
totally-ordered PG WAL stream.

## 7. `WAL_REC_ROLLBACK` Without Prior Data Records

### Scenario

A transaction calls `BEGIN; ROLLBACK;` with no intervening
modifications. Or it calls `BEGIN; SELECT ...; ROLLBACK;` (read-only).
Or its writes have all been confined to system catalogs / non-orioledb
tables. Either way, the OrioleDB local WAL buffer never received a
modify record, the `local_wal_has_material_changes` flag stayed
`false`, and yet `wal_rollback()` is invoked.

The runtime side handles this trivially via the early-return at the
top of `wal_rollback()` (`src/recovery/wal.c:389-395`):

```c
if (!local_wal_has_material_changes)
{
    local_wal_buffer_offset = 0;
    local_type = oIndexInvalid;
    ORelOidsSetInvalid(local_oids);
    return;
}
```

In that case no `WAL_REC_ROLLBACK` record is emitted at all — the
buffer is just zeroed. The interesting Case-7 scenario is the
*pathological* one: the buffer contains *only* a `WAL_REC_XID` record
plus a `WAL_REC_ROLLBACK` terminator (e.g. because a subtransaction
emitted the XID record but produced no data, or because of an
injection point firing between XID and the first modify).

### WAL byte sequence

```
... [XID 42] [ROLLBACK 42]   ## flushed
```

### Code path

The dispatcher's switch arm for `WAL_REC_ROLLBACK` is at
`src/recovery/recovery.c:3697-3751` (shared with `WAL_REC_COMMIT`,
`commit = (rec->type == WAL_REC_COMMIT)` at line 3710). For ROLLBACK
the call lands in `recovery_finish_current_oxid(COMMITSEQNO_ABORTED,
xlogPtr, -1, sync)` at `src/recovery/recovery.c:3739`.

`recovery_finish_current_oxid()` at `src/recovery/recovery.c:1872`
takes the *aborted-and-sync* branch at line 1913-1939:

```c
else
{
    if (flush_undo_pos)
        flush_current_undo_stack();
    for (i = 0; i < (int) UndoLogsCount; i++)
        apply_undo_stack((UndoLogType) i, oxid, NULL, true);
    walk_checkpoint_stacks(cur_recovery_xid_state, csn,
                           InvalidSubTransactionId, flush_undo_pos);
    ...
    set_oxid_csn(oxid, COMMITSEQNO_ABORTED);
    set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr);
}
```

For a no-op rollback the `apply_undo_stack()` calls walk an *empty*
per-oxid undo stack — `walk_undo_stack()`
(`src/transam/undo.c:1411`) iterates from the current undo location to
the stack start; if they are equal, it does nothing. Same for
`walk_checkpoint_stacks()`: the per-oxid `checkpoint_undo_stacks` list
is empty if no checkpoint snapshot ever recorded the oxid. The final
`set_oxid_csn(oxid, COMMITSEQNO_ABORTED)` is the only durable side
effect.

### Why this is a no-op and not an error

The recovery code path makes no assumption that a `WAL_REC_ROLLBACK`
must have been preceded by a modify record. The dispatcher only
requires `cur_recovery_xid_state != NULL`
(`src/recovery/recovery.c:3713` asserts `Assert(rec->oxid !=
InvalidOXid); Assert(cur_recovery_xid_state != NULL);`). That
non-NULL state was installed by `recovery_switch_to_oxid()` when the
preceding `WAL_REC_XID` record was processed (`recovery.c:3690`). The
state hash entry is created on first switch (`recovery.c:1745` with
`HASH_ENTER`), so even an XID that did nothing has a valid entry.

The `recovery_oxid = InvalidOXid` reset at
`src/recovery/recovery.c:1953` and the
`check_delete_xid_state()` call at `recovery.c:1970` clean up the empty
state entry afterwards.

### Invariant preserved

> *A WAL_REC_ROLLBACK record for an oxid with no prior modifications is
> equivalent to no record at all; recovery's only durable effect is
> setting the oxid's CSN slot to `COMMITSEQNO_ABORTED`.*

This also keeps the dispatcher trivial — there is no need for the redo
to know whether modifies preceded the terminator.

## 8. `WAL_REC_COMMIT` Without Prior Data Records

### Scenario

The previous checkpoint flushed *all* of oxid 42's modifications into
the on-disk page state (the modifies happened well before
`replayStartPtr`), and oxid 42 then performed its COMMIT *after* the
checkpoint redo pointer. Recovery starts from the checkpoint and sees
only `WAL_REC_XID 42` followed by `WAL_REC_COMMIT 42` — the modifies
are not in the replayed WAL range because they are already on disk.

This is the dual of Case 7 but for COMMIT, and it is the common case
for any long-running transaction that committed shortly after a
checkpoint.

### WAL byte sequence

```
... [CHECKPOINT redo=L_chkp] ... [XID 42] [COMMIT 42]   ## flushed
                                         ^^^^^^^^^^
                                         oxid 42's modifies happened
                                         before L_chkp and are already
                                         on disk
```

### Code path

Same dispatcher arm as Case 7
(`src/recovery/recovery.c:3697-3751`), but with `commit = true`. The
finish call becomes `recovery_finish_current_oxid(COMMITSEQNO_MAX_NORMAL -
1, xlogPtr, -1, sync)` at `src/recovery/recovery.c:3739`.

That hits the `!ABORTED && sync` branch at
`src/recovery/recovery.c:1883-1898`:

```c
set_oxid_csn(oxid, COMMITSEQNO_COMMITTING);
if (flush_undo_pos)
    flush_current_undo_stack();
for (i = 0; i < (int) UndoLogsCount; i++)
    precommit_undo_stack((UndoLogType) i, oxid, true);
for (i = 0; i < (int) UndoLogsCount; i++)
    on_commit_undo_stack((UndoLogType) i, oxid, true);
walk_checkpoint_stacks(cur_recovery_xid_state, csn,
                       InvalidSubTransactionId, flush_undo_pos);
csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
set_oxid_csn(oxid, csn);
set_oxid_xlog_ptr(oxid, XLOG_PTR_ALIGN(ptr));
```

The `precommit_undo_stack` / `on_commit_undo_stack` calls walk the
*per-oxid current* undo stack — which is empty because the oxid had no
records in the replayed range. So the walk is a no-op.

The `walk_checkpoint_stacks()` call walks the oxid's
`checkpoint_undo_stacks` list, which was populated at
`src/recovery/recovery.c:914` from the xid file:

```c
stack = (CheckpointUndoStack *) MemoryContextAlloc(TopMemoryContext,
                                                   sizeof(CheckpointUndoStack));
stack->kind = kind;
stack->undoStack = xidRec.undoLocation;
dlist_push_tail(&state->checkpoint_undo_stacks, &stack->node);
set_oxid_csn(xidRec.oxid, COMMITSEQNO_INPROGRESS);
```

If oxid 42 was in-flight at checkpoint time, the checkpoint wrote one
or more entries into the xid file representing the undo stacks the
oxid held at the checkpoint moment. Those entries point into the *
checkpointed* undo log on disk. `walk_checkpoint_stacks()` walks them
and runs the per-record commit callback, which finalises the on-disk
state for those records (writes the final CSN into the corresponding
undo headers, releases any locks the oxid still owned, etc.).

If oxid 42 was *not* in-flight at checkpoint time (its modifies and
its COMMIT both happened after the checkpoint, but its modifies pre-
date `replayStartPtr`), the `checkpoint_undo_stacks` list is empty and
the walk is a no-op. The only durable effect of a no-op COMMIT in
that case is `set_oxid_csn(oxid, csn)` at `recovery.c:1896` — the CSN
slot gets the freshly-incremented value. Any visibility check from a
post-recovery snapshot will see oxid 42's tuples (which are already on
disk in their post-modify form) as visible under that CSN.

### What about the modifies that are already on disk

They are visible because (a) they are physically present in the
on-disk B-tree pages (the checkpoint serialised them), and (b) their
per-tuple undo headers were written with `oxid = 42`. The visibility
check in `oxid_get_csn()` (`src/transam/oxid.c:1611`) reads the
xidBuffer slot for oxid 42 and finds the CSN written by
`recovery_finish_current_oxid` — so the tuples become visible at
exactly the right moment.

### Invariant preserved

> *Recovery does not need the modify records to be in the replayed WAL
> range in order to honour a COMMIT. The CSN assigned via
> `set_oxid_csn(oxid, csn)` is the sole gate that makes the (already
> on-disk) modifications visible.*

## 9. WAL Records the On-Disk Page Already Reflects (Idempotence)

### Scenario

Recovery is fundamentally restartable: a crash *during* recovery is
not a special case, it is just the next crash in the chain. If
recovery PANICs after replaying half the records, the postmaster
restarts the cluster and recovery starts over from the same checkpoint
redo pointer. Many records will be replayed twice.

This is also relevant during normal recovery: the on-disk page state
is the result of background-writer / checkpointer flushes that happen
*concurrently with* the WAL-producing backends. A page may already
contain the effect of a modify record that recovery is about to apply
again.

### WAL byte sequence

```
... [INS k=5 v=42] ...
                       ^ may or may not be reflected in the on-disk
                         leaf page when recovery applies this record
                         for the first time, second time, or N-th time
```

### Code path — idempotence of modify records

The B-tree modify operations are intentionally written to be
idempotent under recovery semantics. The driving function is
`apply_btree_modify_record` (`src/recovery/worker.c:690`), which calls
the same `o_btree_modify` / page-modification machinery as the
production path. The CSN is hard-coded to `COMMITSEQNO_INPROGRESS`
(`src/recovery/worker.c:686, 690`).

The key observations:

* **INSERT**: walks the tree to the target key. If the key already
  exists with the same primary-tuple bytes (because the modify was
  already merged into the page during the previous recovery cycle),
  the operation effectively rewrites the same bytes — the row is
  already present.
* **UPDATE**: same idea — locates the key, applies the post-image. If
  the post-image is already there, it stays there.
* **DELETE**: locates the key. If the key is already absent, the
  delete is a no-op (early return).

Each operation also pushes an undo record onto the current oxid's
undo log. Re-application produces a second undo record for the same
modification — but because the per-oxid undo cursor itself is
reconstructed from scratch during each recovery cycle (no durable
"undo log pointer per oxid" outside of the xid file's
`checkpoint_undo_stacks`), the cursor is consistent with the in-memory
undo log.

### Code path — the CSN/page-checkpointNum gate

A subtler idempotence concern: what if a previous recovery cycle ran
far enough to *checkpoint* the new state and write `checkpointNum =
N+1` pages to disk, then crashed? On the next cycle:

1. `checkpoint_state->lastCheckpointNumber` is loaded from the control
   file (`src/recovery/recovery.c:1042`); since the post-recovery
   checkpoint never completed, this still points at the *previous*
   checkpoint N.
2. WAL replay starts at the redo pointer of checkpoint N.
3. Pages loaded from disk that have `checkpointNum == N+1` would be
   inconsistent with `lastCheckpointNumber == N` — but
   `checkpoint.c`'s "complete or not at all" protocol ensures
   `checkpointNum == N+1` pages are only durable on disk *after* the
   control-file write that bumps `lastCheckpointNumber` to N+1. If
   that control-file write never happened, no such pages are on disk.

So the recovery cycle's idempotence boils down to: B-tree modifies are
purely state-based (locate the key, apply the post-state), the undo
log is reconstructed each cycle, and cross-checkpoint races are
prevented by the control-file atomicity in `checkpoint.c`.

### Invariant preserved

> *Replaying the same WAL record N times produces the same on-disk
> state as replaying it once.*

This is a precondition for crash-during-recovery to be safe; see
Case 10 below.

## 10. SIGQUIT Mid-Undo-Replay

### Scenario

Recovery is in progress. The startup process has read past the
`toastConsistentPtr` boundary, has applied many modify records, has
encountered a `WAL_REC_ROLLBACK` for oxid 17 (or has reached EOL with
oxid 17 still INPROGRESS), and is now inside
`recovery_finish_current_oxid()` driving the rollback's
`apply_undo_stack()` loop at `src/recovery/recovery.c:1917-1918`:

```c
for (i = 0; i < (int) UndoLogsCount; i++)
    apply_undo_stack((UndoLogType) i, oxid, NULL, true);
```

Each iteration pops an undo record and reverses it in the B-tree.
Halfway through, a peer process PANICs (or the startup process itself
PANICs because of an internal assertion). The postmaster cascades
SIGQUIT to all backends. `quickdie()` runs in the startup process and
`_exit(2)`'s. The half-rolled-back undo stack is abandoned.

This is the bug-class the active investigation in this branch is
trying to characterise — but for Topic 4 we describe the *correct*
restart behaviour.

### WAL byte sequence (after the SIGQUIT)

```
... [XID 17] [INS k1] [INS k2] [UPD k3] [ROLLBACK 17]   ## flushed
                                                       ^
                                                       startup process
                                                       PANIC'd while
                                                       inside the
                                                       rollback of
                                                       oxid 17
```

On disk, the leaf pages for keys k1, k2, k3 are in some intermediate
state — possibly k3's update reversed, k2's insert reversed, but k1's
insert not yet reversed. Or some other partial sequence.

### Code path — what restart does

Postmaster restarts the cluster. Startup process re-enters recovery.
The recovery cursor begins at the last *completed* checkpoint's redo
pointer. Crucially, this is the same redo pointer as before the
failed recovery attempt — because the in-progress recovery never
completed a checkpoint of its own.

The full sequence runs again:

1. `recovery_init()` (`src/recovery/recovery.c:1099`).
2. The xid file is re-read; `checkpoint_undo_stacks` for every
   in-flight oxid is rebuilt from the durable xid records.
3. WAL replay re-applies every modify record. Per Case 9 these
   modifications are idempotent: keys already inserted stay inserted,
   keys already deleted stay deleted.

   However, the *on-disk* state from the partial undo of the first
   cycle has interactions: k3's update may have been reversed, then
   the redo-of-update will reapply it. That is fine — the redo is
   idempotent on the post-image.

   The trickier case is k1's reversed-then-redone insert. The first
   cycle's recovery applied the WAL record (k1 inserted), then
   started undoing it (k1 deleted). The second cycle's recovery
   re-applies the WAL record (k1 inserted again). The net state at
   the end of step 3 is: k1, k2, k3 present (because the redo
   applied them all).

4. Recovery again reaches the rollback driver for oxid 17 and again
   runs `apply_undo_stack()`. Because the undo log was rebuilt from
   scratch (and the per-oxid undo stack always restarts in lockstep
   with replay — see Case 6), the undo records for k1/k2/k3 are
   present in memory and `apply_undo_stack()` undoes all three.

The final state is identical to what a single successful recovery
cycle would have produced. The intermediate states are invisible to
clients because the cluster does not open for connections until
recovery completes.

### What makes the undo stack restartable

* **The undo log is in-memory only, rebuilt from WAL**. No durable
  pointer says "undo for oxid 17 is half-applied". Every recovery
  cycle starts at the same checkpoint and reconstructs the in-memory
  undo state from the WAL stream.
* **`walk_undo_stack` is monotonic**. It pops from the top of the
  stack and applies the reverse modify, then advances the cursor
  *atomically with the page modification*. If a crash happens
  mid-pop, the next recovery cycle re-builds the stack including the
  popped record and re-pops it. There is no "half-popped" persistent
  state.
* **B-tree modify operations are idempotent** (Case 9). Re-applying
  the redo over an already-half-undone page produces the same
  post-image.

### Quickdie-vs-cleanup hazard

The active investigation on this branch concerns the *primary path*'s
behaviour during SIGQUIT, not the recovery path's. As called out in
`CLAUDE.md` "SIGQUIT mid-undo-replay" and "postmaster's `SIGQUIT`
(quickdie) bypasses `HOLD_INTERRUPTS`", a *primary* backend
SIGQUIT'd mid-abort can leave the in-memory undo stack abandoned
without running `wal_rollback()` or `apply_undo_stack()`. The
recovery-side correctness still holds: replay sees the (possibly
half-) durable WAL stream, finds either a `WAL_REC_ROLLBACK` or
no terminator at all, and reconstructs the abort effect from the
in-memory undo log it just built.

The hazard is the *production* side leaving the SK out of sync with
the PK after a primary backend's SIGQUIT — that is the SK-leak bug
under investigation. The recovery side's contract remains
"reconstruct everything from durable WAL + checkpoint xid file".

### Invariant preserved

> *Recovery is restartable: a crash mid-recovery loses no durable
> state. The next recovery cycle starts from the same checkpoint redo
> pointer, re-applies the same WAL records (idempotently), and reaches
> the same terminal state.*

This is the property that makes the postmaster's "restart on crash"
loop safe — without it, repeated crashes would compound into
corruption.
