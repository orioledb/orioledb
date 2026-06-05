# xid / oxid / csn in OrioleDB

This document explains OrioleDB's transaction-identity machinery: how the three
ID entities relate, what survives a postmaster restart, what is preserved (and
deliberately *not* preserved) between primary and replica, and where every piece
of state physically lives (memory / WAL / on-disk).

## Brief summary

OrioleDB carries **three independent transaction-ID sequences** (`src/transam/oxid.c:74-88`):
a 32-bit PostgreSQL heap `xid`, a 64-bit OrioleDB `OXid` (`include/orioledb.h:165`),
and a 32-bit *logical* `xid` for logical-decoding API compatibility. There is **no
conversion function** between heap xid and oxid — each allocates from its own
sequence. The `OXid` is the real MVCC identity; it is mapped to a **CSN
(CommitSeqNo)** through the in-memory `xidBuffer` circular buffer of `OXidMapItem`
records, indexed by `oxid % xid_circular_buffer_size` (`src/transam/oxid.c:142`,
`include/transam/oxid.h:19-23`). A transaction's CSN walks a three-state lifecycle
`IN_PROGRESS → CSN_COMMITTING → final-CSN` (or `→ ABORTED`); see
[§1](#1-how-xid-oxid-and-csn-relate).

| Question | One-line answer | Anchor |
|---|---|---|
| How do xid/oxid/csn relate? | 3 independent sequences; oxid→csn via `xidBuffer`; csn special-encodes state in high bits | [§1](#1-how-xid-oxid-and-csn-relate) |
| What survives restart? | `nextXid`←`control.lastXid`, `lastCSN`←`control.lastCSN`; xidBuffer CSNs *regenerated* by WAL replay | [§2](#2-invariant-preserved-across-a-postgresql-restart) |
| What holds primary↔replica? | oxid **numbers identical**; csn **independently re-assigned** on replica; globalXmin **diverges** | [§3](#3-invariant-preserved-between-primary-and-replica) |
| Where is it stored? | mem `xidBuffer` → WAL `WALRec{Xid,Finish,JointCommit}` → on-disk `.xidmap` + `control` | [§4](#4-where-they-are-stored-memory--wal--on-disk) |

**Live concern — globalXmin monotonicity differs by context.** In steady state,
`advance_global_xmin()` *only ever advances* globalXmin (`if (globalXmin >
prevGlobalXmin)`, `src/transam/oxid.c:1166`). During **recovery** it may move
**backward** when `update_run_xmin()` loads pre-checkpoint runXmin
(`src/recovery/recovery.c:2531-2534`), and `free_run_xmin()` may move it backward
one final time at recovery end (`recovery.c:1765`). On a **replica**, globalXmin is
computed from *local* active-transaction state, not from the primary's WAL-carried
values (`src/recovery/recovery.c:2531-2534`) — so primary and replica globalXmin are
**not** guaranteed to match, by design. Most important file:
`src/transam/oxid.c`.

## 1. How xid, oxid, and csn relate

**Three independent entities.** `src/transam/oxid.c:74-88` documents that OrioleDB
uses three distinct transaction ids, *each with its own allocation sequence*:

1. **Heap xid** — PostgreSQL's standard 32-bit `TransactionId`, allocated by the PG
   mechanism.
2. **OXid** — `typedef uint64 OXid` (`include/orioledb.h:165`), allocated
   sequentially via `pg_atomic_fetch_add_u64(&xid_meta->nextXid, 1)` inside
   `get_current_oxid()` (`src/transam/oxid.c:1306`). This is OrioleDB's MVCC
   identity.
3. **Logical xid** — a 32-bit `TransactionId` for logical-decoding PG-API
   compatibility, acquired via `acquire_logical_xid()`, used in `SWITCH_LOGICAL_XID`
   records for H2O (heap→oriole) and O2H (oriole→heap) transitions
   (`src/transam/oxid.c:74-88`, `:445`, `:524`).

There is **no mapping function** between heap xid and oxid; they are independent
sequences (`src/transam/oxid.c:74-88`).

**oxid → csn mapping.** Each oxid's commit state is held in `xidBuffer`, a circular
array of `OXidMapItem { pg_atomic_uint64 csn; pg_atomic_uint64 commitPtr; }`
(`include/transam/oxid.h:19-23`, `src/transam/oxid.c:142`), indexed by `oxid %
xid_circular_buffer_size`. `map_oxid()` (`src/transam/oxid.c:724-780`) reads it.

**CSN encoding (the high-bit special form).** A `CommitSeqNo` is a `uint64` that is
either a *normal* commit sequence number or a *special* value:

- **`COMMITSEQNO_SPECIAL_BIT` = bit 63** (`src/transam/oxid.c:40`).
  `COMMITSEQNO_IS_SPECIAL(csn)` tests it (`:44`).
- When special, the value encodes: **procnum** in bits 15-30
  (`COMMITSEQNO_GET_PROCNUM`, `:49-50`), **nesting level** in bits 31-62
  (`COMMITSEQNO_GET_LEVEL`, `:47-48`), and a **status** field in bit 0 (`:45-46`).
  `COMMITSEQNO_MAKE_SPECIAL(procnum, level, status)` builds it (`:51-53`).
- `COMMITSEQNO_RETAINED_FOR_REWIND` = **bit 62** (`:41`) — an orthogonal flag set on
  commit when `enable_rewind` is on (`:1544`); masked out by `map_oxid()` when
  `getRawCsn=false` (`:743`, `:777`); checked by `csn_is_retained_for_rewind()`
  (`:794-796`).

**Status sub-values** inside the special form:
`COMMITSEQNO_STATUS_IN_PROGRESS = 0x0` (`:42`) and
`COMMITSEQNO_STATUS_CSN_COMMITTING = 0x1` (`:43`).

**PostgreSQL-defined CSN constants** (from PG `access/transam.h`):
`COMMITSEQNO_INPROGRESS=0x0`, `COMMITSEQNO_NON_DELETED=0x1`,
`COMMITSEQNO_ABORTED=0x2`, `COMMITSEQNO_FROZEN=0x3`,
`COMMITSEQNO_FIRST_NORMAL=0x5` (`transam.h:201-206`). Normal CSNs are `>= 0x5` with
bit 63 clear. xidBuffer slots are initialized to `COMMITSEQNO_FROZEN`
(`src/transam/oxid.c:302`).

### CSN lifecycle (state machine)

| Phase | Function | Writes CSN value | Anchor |
|---|---|---|---|
| Allocation | `get_current_oxid()` | `MAKE_SPECIAL(MYPROCNUMBER, level, IN_PROGRESS)` | `src/transam/oxid.c:1306-1351` |
| Precommit | `current_oxid_precommit()` | `MAKE_SPECIAL(…, CSN_COMMITTING)` (before WAL flush, in crit section) | `:1441-1452` |
| Commit | `current_oxid_commit(csn)` | normal `csn \| RETAINED_FOR_REWIND` | `:1533-1544` |
| Abort | `current_oxid_abort()` | `COMMITSEQNO_ABORTED` | `:1576-1615` (set at `:1599`) |
| Abort-after-precommit | `current_oxid_clear_committing()` | reverts back to `MAKE_SPECIAL(…, IN_PROGRESS)` | `:1635-1675` |

Writes go through `set_oxid_csn()` (`:579-650`): a CAS on
`xidBuffer[oxid % size].csn` when `oxid >= writeInProgressXmin` (`:606-626`),
otherwise an `o_buffers_write` to the on-disk xidmap (`:634-649`). `pg_write_barrier`
ordering is applied after the precommit COMMITTING write (`:1454`).

**Readers** call `oxid_get_csn()` (`:1682-1738`). It:
1. fast-paths to `COMMITSEQNO_FROZEN` if `oxid < globalXmin` (`:1706-1712`) — no
   buffer consult;
2. **spin-waits** (`perform_spin_delay()`) while the status bit shows
   `CSN_COMMITTING` (`:1715-1721`); the analogous spin in `oxid_get_xlog_ptr()` is at
   `:1762-1767`;
3. returns `COMMITSEQNO_INPROGRESS` if still IN_PROGRESS-special, else the normal
   CSN.

`csn_committing_set` is a per-backend flag toggled true at precommit and false at
commit/abort (`:1452`, `:1551`, `:1605`).

**Wait-free backend lookup.** Because the special CSN embeds procnum + level,
`wait_for_oxid()` (`:902-942`) can find the backend running a given oxid
(`COMMITSEQNO_GET_PROCNUM`, then `oProcData[procnum].vxids[level]`) with no global
table.

**Recovery override.** During WAL replay `get_current_oxid()` returns the global
`recovery_oxid` if valid (`include/recovery/recovery.h:58`,
`src/transam/oxid.c:1301-1302`) instead of allocating a fresh one.

## 2. Invariant preserved across a PostgreSQL restart

**Invariant: oxids are never reused, and CSN generation resumes monotonically — but
the xidBuffer CSN contents are rebuilt from WAL, not persisted.**

**nextXid restoration.** `checkpoint_shmem_init()` initializes
`xid_meta->nextXid` from `control.lastXid` (the nextXid captured at the last
checkpoint), preventing oxid reuse (`src/checkpoint/checkpoint.c:356`). It also seeds
`writtenXmin`, `checkpointRetainXmin/Xmax` from control-file values (`:356-366`,
specifically `:360`, `:362`).

**nextXid advanced further during WAL recovery.** `read_xids()` reads `XidFileRec`
records from the checkpoint's xid file and, per record, calls `advance_oxids()`
(`src/recovery/recovery.c:880`), which fills `xidBuffer` slots with
`COMMITSEQNO_INPROGRESS` and bumps `xid_meta->nextXid` (`src/transam/oxid.c:1285-1286`).

**CSN persisted via control file, regenerated in buffer.** At checkpoint,
`control.lastCSN` is written from PG's `TRANSAM_VARIABLES->nextCommitSeqNo`
(`src/checkpoint/checkpoint.c:1495`). At startup `control.lastCSN` is loaded into
PG's `startupCommitSeqNo` (`:368`); PG core then initializes `nextCommitSeqNo` from
it (mechanism lives in PG core, not this repo). The **per-oxid CSNs in `xidBuffer`
are NOT persisted**: recovery initializes them to `COMMITSEQNO_INPROGRESS` via
`advance_oxids()` (`src/transam/oxid.c:1286`), then `advance_global_xmin()` stamps
old committed oxids as `COMMITSEQNO_FROZEN` (`:1190`); the actual final CSNs are
re-applied by `set_oxid_csn()` as WAL replay re-commits each transaction.

**Control-file contents** (`CheckpointControl`): `lastCSN`, `lastXid`,
`checkpointRetainXmin/Xmax` (`include/checkpoint/control.h:47-48`, `:57-58`) — exactly
the values needed to restore xid allocation and CSN generation.

### globalXmin monotonicity across restart — NOT monotonic during recovery

This is the point most relevant to the SK-leak/recovery investigation.

- **During recovery, globalXmin may move BACKWARD.** `recovery_xmin` is loaded from
  `xid_meta->runXmin` (itself initialized from `control.lastXid`,
  `src/recovery/recovery.c:1554`). `update_run_xmin()` computes
  `min(xmin_queue, recovery_xmin)` and writes it as `runXmin` (`:2531-2532`); if that
  value is below the current `globalXmin` (which was initialized to `control.lastXid`
  at `checkpoint.c:358`), globalXmin is **allowed to move backward** (`:2533-2534`).
  This is safe: those oxids are freshly loaded from the checkpoint and represent
  currently-retained-for-undo transactions.
- **One final backward move is possible at recovery end** via `free_run_xmin()`
  (`src/recovery/recovery.c:1765`), which resets `runXmin` based on the actual nextXid
  at recovery completion.
- **In steady state (post-recovery), globalXmin is strictly monotonic.**
  `advance_global_xmin()` runs on every oxid allocation and at checkpoint; it
  contains an explicit `if (globalXmin > prevGlobalXmin)` guard and **never moves
  backward** (`src/transam/oxid.c:1159-1167`), even though individual per-process
  xmins can fluctuate.

## 3. Invariant preserved between primary and replica

**Invariant: oxid NUMBERS are identical on primary and replica; CSN values are NOT
carried through physical recovery (the replica re-assigns its own); xmin IS carried;
globalXmin diverges by design.**

**Oxid numbers identical.** During `WAL_REC_XID` parsing the replica calls
`advance_oxids(rec->oxid)` and `recovery_switch_to_oxid(rec->oxid, -1)`
(`src/recovery/recovery.c:3841-3842`), and `advance_oxids()` advances the replica's
`nextXid` to match the primary's oxid (`src/transam/oxid.c:1243-1291`). So a given
logical transaction has the *same oxid number* on both sides.

**WAL carries oxid, xmin, AND csn.** Both `WALRecFinish` (`include/recovery/wal.h:159-164`)
and `WALRecJointCommit` (`:151-157`) carry `xmin` and `csn`; `WALRecRelation` carries
them too since WAL version 17 (`:83-96`). The primary writes them:
`add_finish_wal_record` memcpys `rec->xmin`/`rec->csn` (`src/recovery/wal.c:563-565`);
`add_rel_wal_record` writes `xmin` from `xid_meta->runXmin` and `csn` from
`nextCommitSeqNo` (`:682-686`). The reader parses both fields
(`src/recovery/wal_reader.c:92-93`).

**Replica IGNORES the primary's csn for physical recovery.** This is the key
architectural distinction. In the physical-recovery path, `recovery.c` does **not**
reference `rec->u.finish.csn` or `rec->u.joint_commit.csn`. Instead
`recovery_finish_current_oxid()` is called with a *temporary marker*
`COMMITSEQNO_MAX_NORMAL-1`, not the WAL csn (`src/recovery/recovery.c:3891`). The csn
fields are parsed (`wal_reader.c:93,107`) but discarded by physical recovery.

**Deferred CSN finalization on the replica.** Committing transactions enter a
`finished_list` with the temporary marker (`in_finished_list=true`,
`dlist_push_tail(&finished_list, …)`, `src/recovery/recovery.c:2041`, `:2048-2049`).
The recovery **leader** (worker_id < 0) later assigns the real csn in
`update_proc_retain_undo_location()`: set `COMMITSEQNO_COMMITTING`, then
`pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1)`, then
`set_oxid_csn()` with the new csn (`:2636-2642`). Therefore any apparent
oxid→csn agreement between primary and replica (e.g. oxid 610 → csn 461 on both) is
**coincidental** — it holds only when the two sides' finalization schedules and
nextCommitSeqNo advancement happen to align.

**xmin IS carried and used.** The replica reads the WAL xmin to advance its
`recovery_xmin`: `recovery_xmin = Max(recovery_xmin, rec->u.finish.xmin)`
(`src/recovery/recovery.c:3858`; same pattern for `WAL_REC_JOINT_COMMIT` at `:3912`).
The primary supplies it from `xid_meta->runXmin` (`src/recovery/wal.c:682-683`).

**globalXmin DIVERGES (by design).** The replica computes globalXmin from its own
active-transaction state — `pg_atomic_write_u64(&xid_meta->globalXmin, xmin)` where
`xmin` comes from `pairingheap_first(xmin_queue)` or `nextXid`
(`src/recovery/recovery.c:2531-2534`). It is **not** derived from the primary's
WAL-carried values, because globalXmin represents locally-active transactions, which
legitimately differ between primary and standby.

**csn IS used for logical decoding** (the exception to "csn is ignored"). In the
logical-replication path the WAL csn *is* consumed:
`csnSnapshot->snapshotcsn = rec->u.finish.csn` (`src/recovery/logical.c:753`) and
`= rec->u.joint_commit.csn` (`:922`). The csn fields are architecturally meaningful
for logical decoding, *not* for physical recovery.

## 4. Where they are stored (memory / WAL / on-disk)

Three storage tiers, plus the checkpoint metadata anchor.

### (a) In-memory: the `xidBuffer` circular buffer

- Array of `OXidMapItem { pg_atomic_uint64 csn; pg_atomic_uint64 commitPtr; }`
  (`include/transam/oxid.h:19-23`), declared at `src/transam/oxid.c:142`, assigned in
  `oxid_init_shmem` (`:288`), indexed by `oxid % xid_circular_buffer_size`
  (`:606,675,743,854`).
- **Size:** `((xid_buffers_guc * BLCKSZ) / 2) / ORIOLEDB_BLCKSZ * (ORIOLEDB_BLCKSZ /
  sizeof(OXidMapItem))` (`src/orioledb.c:1131-1134`); GUC-controlled
  `xid_circular_buffer_size` (`include/orioledb.h:437`).
- **Init:** every slot `csn = COMMITSEQNO_FROZEN`, `commitPtr =
  FirstNormalUnloggedLSN` (`src/transam/oxid.c:300-304`).
- **Window boundaries** in `XidMeta` (all `pg_atomic_uint64`): `nextXid` (alloc
  frontier, `include/transam/oxid.h:27`), `globalXmin` (FROZEN threshold, `:30`),
  `writeInProgressXmin` (write frontier, `:32`), `writtenXmin` (durable frontier,
  `:33`). `writeInProgressXmin` is raised before a flush, `writtenXmin` after it
  (`src/transam/oxid.c:835`, `:870`; advance_global_xmin fast path `:1175-1199`).
- **Write path:** `set_oxid_csn()` CAS when `oxid >= writeInProgressXmin`, else
  `o_buffers_write` fallback (`:606-650`).
- **Eviction:** when `newOxid >= writtenXmin + xid_circular_buffer_size - 1`
  (`:1323`), `write_xidsmap()` (`:802-874`) exchanges aged slots out to o_buffers,
  advancing `writtenXmin` in chunks of `xid_circular_buffer_size/8` (`:817`); the
  write is serialized by `xid_meta->xidMapWriteLock` (LW_EXCLUSIVE, `:1328-1331`,
  `:884-889`).
- **FROZEN-fill** for `[writtenXmin, globalXmin)` happens in `advance_global_xmin()`
  (`:1187-1193`); **INPROGRESS-fill** for the new range in `advance_oxids()` during
  recovery (`:1272-1289`).

### (b) WAL records

| Record | Fields | Anchor |
|---|---|---|
| `WALRecXid` | oxid(8) + logicalXid(4) + heapXid(4) | `include/recovery/wal.h:62-74` |
| `WALRecFinish` | xmin(8) + csn(8) (commit/rollback) | `:159-164` |
| `WALRecJointCommit` | xid(4) + xmin(8) + csn(8) (heap↔oriole) | `:151-157` |
| `WALRecRelation` | xmin + csn (since WAL v17) | `:83-96` |

Written by `wal_commit()` / `wal_joint_commit()` into the `local_wal` buffer with the
CSN at commit time (`src/recovery/wal.c:304-385`, `:387-480`), then pushed to the PG
WAL stream by `flush_local_wal()` (`include/recovery/wal.h:211`). On recovery,
`recovery_xid_state_hash` holds per-oxid state (`state->csn` set from the WAL finish
record, `src/recovery/recovery.c:2086`; looked up by `recovery_map_oxid_csn`,
`:1485-1497`).

### (c) On-disk: `.xidmap` files (the paged-out buffer)

- Multi-file store `ORIOLEDB_DATA_DIR/%02X%08X.xidmap`, tag `OXID_BUFFERS_TAG=0`,
  `XID_FILE_SIZE = 0x1000000` (16 MB/file) (`src/transam/oxid.c:37-38`, `:163-165`).
- Record = `OXidMapItem` (csn + commitPtr) at `offset = oxid * sizeof(OXidMapItem)`,
  via the `o_buffers` API (`:647-650`).
- **Read-back:** `map_oxid()` reads the circular buffer when `oxid >=
  writeInProgressXmin`, else waits on `xidMapWriteLock` and calls `o_buffers_read()`
  (`:724-780`, esp. `:749-764`); returns `COMMITSEQNO_FROZEN` if `oxid < globalXmin`
  (`:767-774`).
- **Reclamation:** `unlink_unretained_o_buffers()` is called twice in
  `advance_global_xmin()` (`:1218-1230`) to delete `.xidmap` files no longer needed
  (4-case partition logic in `src/utils/o_buffers.c:589-663`).
- **Recovery path** prefers the recovery-local hash table: `map_oxid()` routes to
  `recovery_map_oxid_csn()` for the recovery process (`:724-738`), which consults
  `recovery_xid_state_hash` and returns `COMMITSEQNO_ABORTED` when `wal_xid` is false
  (`src/recovery/recovery.c:1485-1497`). So on-disk `.xidmap` is primarily a
  normal-path visibility store; recovery leans on the hash table.

### (d) Checkpoint metadata anchor

- **`%u.xid` files** hold the `XidFileRec` queue — `{ oxid, kind, undoLocation,
  retainLocation }` (`include/checkpoint/checkpoint.h:160-166`) — written per
  checkpoint number, old files unlinked after the new control file is written
  (`unlink_xids_file`, `src/checkpoint/checkpoint.c:659-665`; `read_xids()` consumes
  them at recovery, `src/recovery/recovery.c:848-918`).
- **`ORIOLEDB_DATA_DIR/control`** (fixed 8192 B, `CHECKPOINT_CONTROL_FILE_SIZE`)
  persists `lastCSN`, `lastXid`, `checkpointRetainXmin/Xmax`
  (`include/checkpoint/control.h:42-63`). Read at startup by
  `get_checkpoint_control_data()` (CRC-validated, `src/checkpoint/control.c:31-73`);
  written CRC'd + synced by `write_checkpoint_control()` (`:127-152`). At checkpoint
  completion `control.lastXid = nextXid`, `control.lastCSN = nextCommitSeqNo`
  (`src/checkpoint/checkpoint.c:1495-1496`, control write at `:1517`); `xid_meta`
  retention atomics are synced at `:1559-1560`. This is the **per-checkpoint summary
  anchor**, as distinct from the **per-oxid** detail in `.xidmap`.

## Caveats / unverified

These items are flagged in the source findings as not fully proven inside this
repository, or as explicitly *not* invariant:

- **PG core boundary — nextCommitSeqNo init.** `control.lastCSN` is loaded into PG's
  `startupCommitSeqNo` (`src/checkpoint/checkpoint.c:368`), but the mechanism by which
  PG core initializes `nextCommitSeqNo` from `startupCommitSeqNo` lives in PostgreSQL
  core and is **not visible** in this codebase.
- **PG-defined CSN constants** (`COMMITSEQNO_FROZEN`, `_INPROGRESS`, `_ABORTED`,
  `_FIRST_NORMAL`, `COMMITSEQNO_IS_NORMAL/_IS_COMMITTED/_NON_DELETED`) come from PG
  headers (`access/transam.h`), not OrioleDB source — cited from `transam.h:201-206`
  but not independently verified in this repo.
- **primary↔replica csn agreement is NOT an invariant.** Any matching oxid→csn pair
  across primary and replica is *coincidental* (deferred-finalization schedules and
  `nextCommitSeqNo` rates happening to align), not guaranteed
  (`src/recovery/recovery.c:2636-2642`).
- **globalXmin is NOT guaranteed equal across primary↔replica**, and is **not
  monotonic during recovery** (backward moves at
  `src/recovery/recovery.c:2531-2534` and the final `free_run_xmin()` at `:1765`).
  Only steady-state, post-recovery globalXmin is strictly monotonic
  (`src/transam/oxid.c:1166`).
- **`flush_local_wal()`** is cited only by its declaration
  (`include/recovery/wal.h:211`) — confidence **medium**; its body was not inspected.
- **Minor line-number drift.** Findings note the `oxid_get_xlog_ptr()` spin-wait is at
  `src/transam/oxid.c:1762-1767` (one finding originally mis-cited `1716-1721`, which
  is the `oxid_get_csn` spin); INPROGRESS-fill is precisely `:1285-1288` within the
  `advance_oxids` span `:1242-1292`. Line numbers may shift with edits.
- **`XidMeta` has more fields than the four window boundaries** discussed here
  (also `checkpointRetainXmin/Xmax`, `cleanedXmin`, `cleanedCheckpointXmin/Xmax`,
  `lastXidWhenUpdatedGlobalXmin`, and lock/tranche fields) — see the full struct in
  `include/transam/oxid.h:25-53`.
- **Recovery use of on-disk `.xidmap`.** Recovery consults the recovery-local
  `recovery_xid_state_hash` rather than reading `.xidmap` from disk, suggesting
  `.xidmap` serves the normal-path visibility role and only a secondary/fallback role
  during replay (`src/transam/oxid.c:724-738`,
  `src/recovery/recovery.c:1485-1497`).

---

## Appendix A — the `XidMeta` global variables

These are the shared-memory ordering watermarks in `include/transam/oxid.h` (struct `XidMeta`, the singleton `xid_meta`), all `pg_atomic_uint64`, plus the per-oxid `OXidMapItem` slot.

| Variable | Stands for | Meaning (1 line) | Written by | Read by (main) | Key invariant |
|---|---|---|---|---|---|
| nextXid | Next OXid to allocate | Allocation frontier of the xidmap circular buffer; next OXid to be assigned | get_current_oxid fetch_add `oxid.c:1323`; advance_oxids `oxid.c:1307`; init `oxid.c:284`/`checkpoint.c:356`; persist `checkpoint.c:1496` | get_current_oxid `oxid.c:1331`; advance_oxids `oxid.c:1265,1272,1287`; recovery `recovery.c:2529,2566` | Monotonic; `runXmin <= nextXid` |
| runXmin | Run xid minimum | Lowest OXid still needing a CSN lookup; below it = finished | advance_run_xmin `oxid.c:1507`; update_run_xmin `recovery.c:2532`; free_run_xmin `recovery.c:2567`; init `checkpoint.c:285,357` | advance_global_xmin `oxid.c:1156`; xid_is_finished `oxid.c:1938`; do_checkpoint `checkpoint.c:1375`; WAL `wal.c:354,401` | `runXmin <= globalXmin`; normally `<= writtenXmin` (recovery violates) |
| globalXmin | Global xid minimum (MVCC horizon) | Visibility horizon; OXid below it is frozen/invisible | advance_global_xmin `oxid.c:1184`; init `checkpoint.c:286,358`; recovery regress `recovery.c:2556,2582` | oxid_get_csn fast-path `oxid.c:1723`; map_oxid `oxid.c:784`; `oxid.c:1775,1826` | `writeInProgressXmin <= globalXmin <= runXmin`; `globalXmin >= writtenXmin` |
| writeInProgressXmin | Write-in-progress minimum | Upper bound of xidmap range actively being flushed to o_buffers | write_xidsmap `oxid.c:852`; advance_global_xmin `oxid.c:1200`; init `checkpoint.c:289,361` | set_oxid_csn `oxid.c:625`; map_oxid `oxid.c:766`; advance_global_xmin `oxid.c:1190,1197` | `writtenXmin <= writeInProgressXmin <= globalXmin` |
| writtenXmin | Written xid minimum | Floor of xids flushed to durable o_buffers (slots FROZEN, buffer cleared) | write_xidsmap `oxid.c:887`; advance_global_xmin `oxid.c:1214`; init `checkpoint.c:288,360` | set_oxid_csn `oxid.c:657`; map_oxid `oxid.c:772`; advance_global_xmin `oxid.c:1189,1198`; recovery `recovery.c:2538,2573` | `writtenXmin <= writeInProgressXmin`; `writtenXmin <= globalXmin` (livelock floor) |
| lastXidWhenUpdatedGlobalXmin | Last xid when updated globalXmin | Throttle threshold: last oxid at which advance_global_xmin ran | advance_global_xmin `oxid.c:1154`; init `checkpoint.c:2345` | get_current_oxid `oxid.c:1331`; SQL `oxid.c:1967` | Monotonic; `<= nextXid`, `<= globalXmin` |
| checkpointRetainXmin | Checkpoint retain xmin | Low end of xid range the latest checkpoint must retain in xidmap | o_perform_checkpoint `checkpoint.c:1559`; init `checkpoint.c:290,362` | advance_global_xmin `oxid.c:1221`; SQL `oxid.c:1970` | `>= writtenXmin` (implicit); `<= checkpointRetainXmax` |
| checkpointRetainXmax | Checkpoint retain xmax | High end (nextXid at checkpoint start) of retained xidmap range | o_perform_checkpoint `checkpoint.c:1560`; init `checkpoint.c:291,363` | advance_global_xmin `oxid.c:1222`; SQL `oxid.c:1971` | `checkpointRetainXmin <= checkpointRetainXmax <= nextXid` |
| cleanedXmin | Cleaned xid minimum | How far xidmap/o_buffers storage has been unlinked/cleaned | advance_global_xmin `oxid.c:1230`; init `checkpoint.c:292,364` | advance_global_xmin `oxid.c:1218`; SQL `oxid.c:1972` | `cleanedXmin <= globalXmin <= writtenXmin` (follows globalXmin) |
| cleanedCheckpointXmin | Cleaned checkpoint retain xmin | Cached checkpointRetainXmin from last cleanup pass | advance_global_xmin `oxid.c:1228`; init `checkpoint.c:293,365` | advance_global_xmin `oxid.c:1219,1237` | Monotonic; tracks prior checkpoint boundary |
| cleanedCheckpointXmax | Cleaned checkpoint retain xmax | Cached checkpointRetainXmax from last cleanup pass | advance_global_xmin `oxid.c:1229`; init `checkpoint.c:294,366`; checkpoint `checkpoint.c:1560` | advance_global_xmin `oxid.c:1220,1239`; SQL `oxid.c:1971` | `>= cleanedCheckpointXmin`; `== checkpointRetainXmax` at write |

### Ordering invariant

Normal operation: `cleanedXmin <= writtenXmin <= writeInProgressXmin <= globalXmin <= runXmin <= nextXid`, with the cleanup pair satisfying `cleanedCheckpointXmin <= cleanedCheckpointXmax <= nextXid` and `checkpointRetainXmin <= checkpointRetainXmax <= nextXid` (relationship of the checkpointRetain/cleaned pairs to `writtenXmin` is only *implicit* — uncertain link). Recovery can VIOLATE the chain by regressing `runXmin`/`globalXmin` below `writtenXmin`.

### `nextXid`

**What it is** — A monotonically increasing 64-bit high-water mark: the next OrioleDB xid to allocate, the frontier of the xidmap circular buffer; persisted across restart via the checkpoint control file (`control.lastXid`).
**Writers & readers** — Written by get_current_oxid `oxid.c:1323` (fetch_add), advance_oxids `oxid.c:1307`, init `oxid.c:284`/`checkpoint.c:356`, persisted `checkpoint.c:1496`. Read by get_current_oxid `oxid.c:1331`, advance_oxids `oxid.c:1265,1272,1287`, write_xidsmap `oxid.c:837`, checkpoint `checkpoint.c:1443`, recovery `recovery.c:2529,2566`, SQL `oxid.c:1964`, diag `orioledb.c:2226`.
**Invariants** — Monotonic (never decreases); `nextXid <= writtenXmin + xid_circular_buffer_size`; `runXmin <= nextXid`. Atomic itself (not under xminMutex).
**Livelock** — In standby update_run_xmin (`recovery.c:2529`), when xmin_queue is empty `nextXid` is used as the fallback xmin; if it has run far ahead, `runXmin = nextXid` can push globalXmin past the frozen writtenXmin range, re-exposing frozen slots (csn-trace logs nextXid at `recovery.c:2547`).

### `runXmin`

**What it is** — The lowest OXid for which a CSN lookup is still required; any `xid < runXmin` is presumed finished. Seeded in recovery from WAL, advanced as transactions complete.
**Writers & readers** — Written by advance_run_xmin `oxid.c:1507` (CAS), update_run_xmin `recovery.c:2532`, free_run_xmin `recovery.c:2567`, init `checkpoint.c:285,357`. Read by advance_global_xmin `oxid.c:1156`, advance_run_xmin `oxid.c:1507`, xid_is_finished `oxid.c:1938`, xid_is_finished_for_everybody `oxid.c:1994`, do_checkpoint `checkpoint.c:1375`, recovery `recovery.c:1554`, WAL `wal.c:354,401,453,496,682,811`, SQL `oxid.c:1965`.
**Invariants** — `runXmin <= globalXmin`; `runXmin <= nextXid`; normally `runXmin <= writtenXmin`. Monotonic in normal op (CAS); recovery breaks monotonicity.
**Livelock** — CRITICAL root cause: update_run_xmin `recovery.c:2532` can regress runXmin below writtenXmin (logged `recovery.c:2536-2556`), re-exposing FROZEN xidmap slots that oxid_get_csn mis-resolves as IN_PROGRESS; free_run_xmin `recovery.c:2567` (`runXmin = nextXid`) can also regress it.

### `globalXmin`

**What it is** — The monotonically advancing MVCC visibility horizon; all OXid `< globalXmin` are COMMITSEQNO_FROZEN and invisible. Gates the oxid_get_csn fast path.
**Writers & readers** — Written by advance_global_xmin `oxid.c:1184`, init `checkpoint.c:286,358`, recovery regress `recovery.c:2556,2582`. Read by oxid_get_csn `oxid.c:1723`, map_oxid `oxid.c:784`, oxid_get_xlog_ptr `oxid.c:1775`, oxid_match_snapshot `oxid.c:1826`, advance_global_xmin `oxid.c:1176`, oxid_debug_raw_slot `oxid.c:186`, SQL `oxid.c:1966`.
**Invariants** — Monotonic in normal op (`oxid.c:1183`); `globalXmin <= runXmin`; `globalXmin >= writeInProgressXmin` and `>= writtenXmin` (asserted `oxid.c:1197-1198`); `<= nextXid`. Guarded by xminMutex (`oxid.c:1151-1233`).
**Livelock** — Recovery regression at `recovery.c:2556` / `recovery.c:2582` can drop globalXmin below writtenXmin, re-exposing frozen slots (dangerous case flagged `recovery.c:2541-2545`).

### `writeInProgressXmin`

**What it is** — Intermediate watermark: upper-bound (exclusive) of xidmap entries actively being flushed (FROZEN-filled) from the circular buffer to o_buffers. Slots `>= writeInProgressXmin` are still safe to modify in the buffer.
**Writers & readers** — Written by write_xidsmap `oxid.c:852` (under xminMutex), advance_global_xmin `oxid.c:1200` (under xidMapWriteLock), init `checkpoint.c:289,361`. Read by set_oxid_csn `oxid.c:625,651`, set_oxid_xlog_ptr_internal `oxid.c:694,709`, map_oxid `oxid.c:766`, advance_global_xmin `oxid.c:1190,1197`, SQL `oxid.c:1968`.
**Invariants** — `writtenXmin <= writeInProgressXmin <= globalXmin` (asserted `oxid.c:1197`); monotonic; defines the `[writtenXmin, writeInProgressXmin)` in-flight range.
**Livelock** — Defines the contract violated when recovery regresses globalXmin below writtenXmin (`recovery.c:2536-2556`): does not itself prevent the regression, but slots below it should never re-appear as IN_PROGRESS.

### `writtenXmin`

**What it is** — The floor of xids already flushed to durable o_buffers; their slots hold COMMITSEQNO_FROZEN and the xidBuffer entries are cleared. The hard floor of recoverable xid history.
**Writers & readers** — Written by write_xidsmap `oxid.c:887`, advance_global_xmin `oxid.c:1214`, init `checkpoint.c:288,360`. Read by set_oxid_csn `oxid.c:657,663`, set_oxid_xlog_ptr_internal `oxid.c:715,721`, map_oxid `oxid.c:772,778`, write_xidsmap `oxid.c:833,834,886`, fsync_xidmap_range `oxid.c:899`, advance_global_xmin `oxid.c:1189,1192,1198,1204`, advance_oxids `oxid.c:1277,1288`, get_current_oxid `oxid.c:1340,1343`, oxid_debug_raw_slot `oxid.c:187`, recovery `recovery.c:2538,2573`, modify.c:606, SQL `oxid.c:1969`.
**Invariants** — `writtenXmin <= writeInProgressXmin`; `writtenXmin <= globalXmin` (asserted `oxid.c:1198`, the livelock-preventing invariant); monotonic (`oxid.c:886-887`); all oxids `< writtenXmin` must be FROZEN in o_buffers.
**Livelock** — The hard floor: when standby update_run_xmin regresses globalXmin (`recovery.c:2556`) below writtenXmin, the frozen `[globalXmin, writtenXmin)` region is re-exposed and read as IN_PROGRESS; csn-trace logs `belowWritten` at `recovery.c:2547,2575`.

### `lastXidWhenUpdatedGlobalXmin`

**What it is** — A throttle threshold recording the oxid at which advance_global_xmin last ran, used to decide whether to re-run the advance (roughly once per 1/10 of the buffer).
**Writers & readers** — Written by advance_global_xmin `oxid.c:1154` (under xminMutex), init `checkpoint.c:2345`. Read by get_current_oxid `oxid.c:1331` (unlocked, benign stale read), SQL `oxid.c:1967`.
**Invariants** — Monotonically non-decreasing; `<= nextXid`; `<= globalXmin` (logical, not asserted). Write under xminMutex; throttle read is lock-free.

### `checkpointRetainXmin`

**What it is** — The low end of the xid range the most recent completed checkpoint must retain in the xidmap; protects those regions from cleanup unlink.
**Writers & readers** — Written by o_perform_checkpoint `checkpoint.c:1559` (snapshot of runXmin, under xminMutex), init `checkpoint.c:290,362`. Read by advance_global_xmin `oxid.c:1221`, SQL `oxid.c:1970`.
**Invariants** — `checkpointRetainXmin <= checkpointRetainXmax`; `>= writtenXmin` (implicit, via runXmin snapshot — uncertain); monotonic. Guarded by xminMutex at all sites.
**Livelock** — Indirect: constrains cleanup so retained `[checkpointRetainXmin, checkpointRetainXmax]` buffers are not unlinked, preventing a backward runXmin update from making oxid_get_csn misread stale/garbage-collected buffers (`recovery.c:2532-2556`).

### `checkpointRetainXmax`

**What it is** — The high end (`nextXid` captured at checkpoint start) of the xidmap range a checkpoint must retain; with checkpointRetainXmin bounds the non-collectible `[Xmin, Xmax)` window.
**Writers & readers** — Written by o_perform_checkpoint `checkpoint.c:1560` (= checkpoint_xmax read at `checkpoint.c:1443`, under xminMutex), init `checkpoint.c:291,363`. Read by advance_global_xmin `oxid.c:1222` (→ unlink_unretained_o_buffers), SQL `oxid.c:1971`.
**Invariants** — `checkpointRetainXmin <= checkpointRetainXmax`; `checkpointRetainXmax <= nextXid`; `>= runXmin` at checkpoint start; monotonically non-decreasing. xminMutex-guarded.
**Livelock** — Primary participant: standby recovery regressing globalXmin below writtenXmin (`recovery.c:2533-2558`) re-exposes frozen slots in the prior checkpoint's `[checkpointRetainXmin, checkpointRetainXmax)` range, mis-read as IN_PROGRESS; fix floors globalXmin at writtenXmin (commit 8ce12585).

### `cleanedXmin`

**What it is** — Tracks how far the .xidmap circular buffer / o_buffers have been physically unlinked; the oldest xid whose storage has not yet been removed.
**Writers & readers** — Written by advance_global_xmin `oxid.c:1230` (under xminMutex, only when globalXmin or checkpoint-retain fields change), init `checkpoint.c:292,364`. Read by advance_global_xmin `oxid.c:1218` (oldCleanedXmin → unlink_unretained_o_buffers), SQL `oxid.c:1972`.
**Invariants** — `cleanedXmin <= globalXmin`; `cleanedXmin <= writtenXmin`; `>= FirstNormalTransactionId`; monotonic. Written only inside xminMutex (`oxid.c:1151-1233`); cleanup is conditional on change.

### `cleanedCheckpointXmin`

**What it is** — Caches the checkpointRetainXmin value used during the last xidmap cleanup pass; compared against the current checkpointRetainXmin to detect a shifted checkpoint boundary needing cleanup.
**Writers & readers** — Written by advance_global_xmin `oxid.c:1228` (under xminMutex, when oldCheckpointXmin != newCheckpointXmin), init `checkpoint.c:293,365`. Read by advance_global_xmin `oxid.c:1219`, used as oldCheckpointXmin arg `oxid.c:1237-1241`.
**Invariants** — Monotonically non-decreasing (`>= FirstNormalTransactionId`); read/written atomically with cleanedCheckpointXmax/cleanedXmin under xminMutex (`oxid.c:1151-1233`); guards `[oldCheckpointXmin, oldCheckpointXmax]` buffers from reuse before checkpointRetainXmin advances.

### `cleanedCheckpointXmax`

**What it is** — Caches the checkpointRetainXmax value from the last cleanup pass; with cleanedCheckpointXmin defines the previously-seen retention window, avoiding redundant cleanup when bounds are unchanged.
**Writers & readers** — Written by advance_global_xmin `oxid.c:1229` (under xminMutex), complete_checkpoint `checkpoint.c:1560`, init `checkpoint.c:294,366`. Read by advance_global_xmin `oxid.c:1220` (compare), `oxid.c:1239` (→ unlink_unretained_o_buffers), SQL `oxid.c:1971`.
**Invariants** — `cleanedCheckpointXmax >= cleanedCheckpointXmin`; `== checkpointRetainXmax` at the moment of write (mirrors read at `oxid.c:1222`); written only when the cleanup condition fires; xminMutex-guarded.

### Low-confidence / unverified

- `checkpointRetainXmin >= writtenXmin` and `checkpointRetainXmax`'s relationship to `writtenXmin` are only *implicit* (via the runXmin snapshot at checkpoint start), not explicitly asserted in code.
- `lastXidWhenUpdatedGlobalXmin <= globalXmin` is logical only, not asserted.
- All 11 field findings were individually marked `"confidence": "high"`; the orderings above tying the checkpointRetain/cleaned pairs into the main watermark chain are the least certain links.
