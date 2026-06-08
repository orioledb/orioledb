# OrioleDB recovery — investigation report

This report consolidates a four-topic deep-dive into the WAL crash-recovery
path for OrioleDB and the patched PostgreSQL it links against. It mirrors the
structure of the companion file
[`checkpoint_investigation.md`](./checkpoint_investigation.md), which covers
the *checkpoint* side of the same flow.

All line numbers refer to repositories on the host machine:
- PG: `/home/user/work/postgres/` (orioledb's `patches17_6` PG17 fork)
- Orioledb: `/home/user/work/orioledb/`

> **Provenance:** sections 1–4 below were produced by four parallel research
> agents on 2026-05-20, each given a non-overlapping slice of the recovery
> problem. The originals (kept alongside this file as
> `recovery_report_topic{1,2,3,4}_*.md`) are the per-topic deliverables.
> This document concatenates them under a shared introduction.

## Table of contents

1. **PostgreSQL baseline recovery model** — control file, checkpoints, WAL
   record format, the `StartupXLOG` loop, page-LSN idempotence, FPI / backup
   blocks, the recovery → production handshake.
2. **PG ↔ OrioleDB interface boundary** — custom rmgr registration, the seven
   `patches17_6` hooks that the orioledb fork relies on, the strict
   producer-only role of the TableAM during redo, and the bridge sequence
   from postmaster crash through to per-record dispatch.
3. **OrioleDB recovery internals (deep-dive)** — row-level WAL records, the
   `orioledb_redo` dispatcher, PK/SK fanout, the four-layer idempotence
   ladder (and the deliberate absence of a per-page LSN compare), oxid/CSN
   rebuild, parallel recovery workers, `toastConsistentPtr` and
   `PendingSkFixup`.
4. **WAL-stream edge cases** — ten scenarios the recovery code must (and
   does) survive, including unfinished-transaction WAL, mid-COMMIT crashes,
   torn tail records, cross-tree partial-checkpoint metadata, interleaved
   multi-backend records, and SIGQUIT-mid-undo-replay.
5. **Streaming replication recovery** — how a *standby* differs from crash
   recovery: perpetual recovery fed by the WAL stream, the parallel-recovery
   leader/workers, deferred-commit finalization and the `listPtr` drain, the
   per-standby xmin horizon (and why it can move *backward*), eager-WAL +
   the deferred recovery-finish ROLLBACK, the primary↔standby invariants, and
   the streaming-recovery livelock found and fixed on this branch.

---

## 1. PostgreSQL baseline recovery model

*(Source: `recovery_report_topic1_pg_baseline.md`; original title: "Topic 1: Vanilla PostgreSQL Crash Recovery — Baseline Reference")*
> Scope: the stock PostgreSQL 17 (patched, branch `patches17_6`) crash-recovery
> machinery, with **no** OrioleDB layer in the picture. Topic 2 covers the
> PG <-> custom-AM interface during recovery; Topic 3 is the OrioleDB-specific
> deep-dive; Topic 4 is WAL-sequence edge cases. This document is the floor
> the others build on.
>
> All source references point at `/home/user/work/postgres`, which is the
> patched PG 17 tree at branch `patches17_6`. File paths are written relative
> to that root.

### 1. The Big Picture

```
                +---------------------------------------------------+
                |                  postmaster                        |
                +-----+----------------------------+----------------+
                      | fork() startup process     |
                      v                            |
        +-----------------------------+            |
        |    StartupProcessMain()     |            |
        |    -> StartupXLOG()         |            |
        +-----------------------------+            |
            |     1. ReadControlFile()             |
            |     2. determine REDO LSN            |
            |     3. PerformWalRecovery()          |
            |        - XLogReadRecord loop         |
            |        - RmgrTable[r].rm_redo(rec)   |
            |     4. CreateEndOfRecoveryRecord()   |
            |     5. CreateCheckPoint(SHUTDOWN)    |
            |     6. UpdateControlFile() -> PRODUCTION
            v
        +-----------------------------+
        | postmaster opens conns      |
        +-----------------------------+
```

The startup process is the *only* process that touches the WAL during redo;
once it transitions the cluster to `DB_IN_PRODUCTION` it exits and normal
backends are accepted. The control file (`global/pg_control`) is the
anchor: it tells the next startup where to begin and what state it left in.

### 2. The Control File (`pg_control`)

The control file lives at `$PGDATA/global/pg_control`. The path is hardcoded
as `XLOG_CONTROL_FILE` (`src/include/access/xlog_internal.h:150`). It is the
single authoritative on-disk record of "where the cluster left off." Every
backend that runs `pg_controldata`, every replica that wants to know
`system_identifier`, and every startup process that needs to decide where to
begin redo reads this file.

#### 2.1 What it stores

The on-disk layout is `struct ControlFileData`
(`src/include/catalog/pg_control.h:104-233`). Logically it groups into:

- **Identity**: `system_identifier`, `pg_control_version`,
  `catalog_version_no`.
- **Status**: `state` (`DBState`), `time`.
- **Last checkpoint**: `checkPoint` (LSN of the most recent completed
  checkpoint's WAL record) and `checkPointCopy` (a verbatim copy of the
  `CheckPoint` payload — see Section 3.2).
- **Recovery bookkeeping**: `minRecoveryPoint`, `minRecoveryPointTLI`,
  `backupStartPoint`, `backupEndPoint`, `backupEndRequired`. Used by archive
  / PITR recovery only; for plain crash recovery `minRecoveryPoint` is held
  at `Invalid` (see `xlog.c:7264` setting it to `InvalidXLogRecPtr` on each
  completed checkpoint).
- **Unlogged-relation fake LSN counter**: `unloggedLSN`. Restored on clean
  shutdown, reset to `FirstNormalUnloggedLSN` on crash recovery
  (`xlog.c:5602-5607`).
- **GUC consistency snapshot**: `wal_level`, `wal_log_hints`,
  `MaxConnections`, `max_worker_processes`, `max_wal_senders`,
  `max_prepared_xacts`, `max_locks_per_xact`, `track_commit_timestamp`.
  These let the startup process refuse to start if a parameter required by
  the WAL (e.g. `wal_level=replica` records that need hot-standby support)
  has been downgraded.
- **ABI checks**: `maxAlign`, `floatFormat`, `blcksz`, `relseg_size`,
  `xlog_blcksz`, `xlog_seg_size`, `nameDataLen`, `indexMaxKeys`,
  `toast_max_chunk_size`, `loblksize`, `float8ByVal`,
  `data_checksum_version`.
- **Auth nonce**: `mock_authentication_nonce` (32 bytes).
- **Integrity**: `crc` (must be last; see comment at `pg_control.h:231`).

#### 2.2 The `DBState` enum

```
DB_STARTUP                0  // initdb in progress (bootstrap)
DB_SHUTDOWNED             1  // clean shutdown, no recovery needed
DB_SHUTDOWNED_IN_RECOVERY 2  // standby/PITR shut down cleanly mid-recovery
DB_SHUTDOWNING            3  // checkpointer is writing the shutdown ckpt
DB_IN_CRASH_RECOVERY      4  // startup process is replaying WAL after crash
DB_IN_ARCHIVE_RECOVERY    5  // standby/PITR replaying archived WAL
DB_IN_PRODUCTION          6  // open for backends
```
(`src/include/catalog/pg_control.h:89-98`)

Lifecycle for a non-crashing primary:

```
 boot -> STARTUP -> IN_PRODUCTION -> SHUTDOWNING -> SHUTDOWNED
                          (CreateCheckPoint with CHECKPOINT_IS_SHUTDOWN sets
                           SHUTDOWNING then SHUTDOWNED, xlog.c:6929,7260)
```

Lifecycle when the primary crashes:

```
   crash mid-write              next startup
 IN_PRODUCTION  --(power off)--> IN_PRODUCTION   <-- last on-disk state
                                       |
                                       | (StartupXLOG sees != SHUTDOWNED
                                       |  and != SHUTDOWNED_IN_RECOVERY)
                                       v
                                IN_CRASH_RECOVERY
                                       |
                                       | (PerformWalRecovery completes,
                                       |  end-of-recovery checkpoint runs)
                                       v
                                  SHUTDOWNED (transiently, during the
                                              shutdown-style end-of-recovery
                                              checkpoint at xlog.c:7260)
                                       |
                                       | (xlog.c:6158)
                                       v
                                 IN_PRODUCTION
```

The state-machine logic is `StartupXLOG`'s switch at `xlog.c:5425-5476` —
each pre-existing state is logged as a different errmsg, and
`DB_IN_CRASH_RECOVERY` specifically warns "you will have to use the last
backup for recovery" because we already aborted a recovery cycle once.

`InitWalRecovery` sets the state to `DB_IN_CRASH_RECOVERY` (or
`DB_IN_ARCHIVE_RECOVERY`) before redo begins — see
`xlogrecovery.c:939-957`.

#### 2.3 Atomic update discipline

`UpdateControlFile()` (`xlog.c:4518-4521`) is a thin wrapper around the
shared `update_controlfile()` in
`src/common/controldata_utils.c:188-269`. The discipline is:

1. Update `ControlFile->time` (`controldata_utils.c:197`).
2. Recompute the CRC over everything up to but excluding the CRC field
   (`controldata_utils.c:199-204`).
3. Zero a 8192-byte buffer (`PG_CONTROL_FILE_SIZE`,
   `pg_control.h:250`) and `memcpy` the live struct into it.
4. `BasicOpenFile(O_RDWR)` + single `write()` of the full 8192 bytes
   (`controldata_utils.c:222,237`). On error PANIC.
5. `pg_fsync(fd)` if `do_sync` (`controldata_utils.c:260`).

The atomicity guarantee is the file-size constraint: `sizeof(ControlFileData)`
must fit in `PG_CONTROL_MAX_SAFE_SIZE = 512 bytes`
(`pg_control.h:241,255-258`), which is what most disks consider an atomic
sector write. A torn write past the 512-byte boundary touches only
zero-padding and so cannot corrupt the live struct.

Concurrent updates from multiple processes are serialised via
`ControlFileLock` (an LWLock) — see e.g.
`xlog.c:6157,6928,7258,7424`. Any callsite that flips state takes
`ControlFileLock LW_EXCLUSIVE`, mutates the in-memory struct, calls
`UpdateControlFile()`, releases the lock.

#### 2.4 The Redo pointer

The Redo pointer (`checkPoint.redo`, copied to `ControlFile->checkPointCopy.redo`)
is **the LSN at which redo will begin on next startup**. It is **not** the
LSN of the most recent checkpoint *record* — those are two different LSNs:

```
   ... [some record] [some record] [CHECKPOINT_REDO]
                                   ^ Redo pointer
                                              ... [more records] ...
                                                                  [CHECKPOINT_ONLINE]
                                                                  ^ ControlFile->checkPoint
                                                                    (LSN of the ckpt record itself)
```

For **shutdown** checkpoints the two coincide: no WAL can be inserted
between fixing the redo pointer and writing the checkpoint record, so
`checkPoint.redo == ControlFile->checkPoint` (`xlog.c:7011`,
`xlog.c:7038-7042`).

For **online** checkpoints the redo pointer is set by inserting a special
`XLOG_CHECKPOINT_REDO` record *before* flushing buffers
(`xlog.c:7043-7057`); the actual `XLOG_CHECKPOINT_ONLINE` record is written
later, after the dirty buffers have been flushed. This split is required
because anything written to a page *after* the redo pointer must be
recoverable from WAL — but the checkpoint itself must persist that the
flush happened. The redo pointer marks "everything before this is already
on disk in heap/index files," not "everything before this is in the
checkpoint record."

Startup picks up the redo pointer at `xlog.c:5651`:
```
RedoRecPtr = XLogCtl->RedoRecPtr = XLogCtl->Insert.RedoRecPtr = checkPoint.redo;
```
and `PerformWalRecovery` uses `RedoStartLSN` (= `ControlFile->checkPointCopy.redo`,
set at `xlogrecovery.c:776`) as the very first record to read
(`xlogrecovery.c:1712-1729`). If the redo pointer is earlier than the
checkpoint record (the online-checkpoint case), startup explicitly seeks
backward to it and verifies the first record there is `XLOG_CHECKPOINT_REDO`
(`xlogrecovery.c:1717-1728`).

### 3. Checkpoints

A checkpoint is a synchronization point: by the time it completes, every
dirty buffer that existed when the checkpoint started has been flushed to
disk, and `pg_control` has been updated to point at it.

#### 3.1 Full checkpoints vs restart points

There are two flavors:

- **Full checkpoints** (`CreateCheckPoint`, `xlog.c:6875`) run on the primary
  during normal operation, on shutdown, and at end-of-recovery. They
  *insert* a `XLOG_CHECKPOINT_ONLINE` or `XLOG_CHECKPOINT_SHUTDOWN` WAL
  record (`xlog.c:7228` for the actual XLogInsert call, roughly).
- **Restart points** (`CreateRestartPoint`, `xlog.c:7614`) run on a
  *replaying* server (standby or PITR) and do not insert WAL — they only
  flush dirty buffers and update `pg_control` so that if the replica
  crashes, the next startup begins replay from the latest replayed
  checkpoint rather than from the original primary's last checkpoint.

A restart point is essentially a no-op if no new checkpoint record has been
replayed since the last one (`xlog.c:7663-7679`). On a clean shutdown of a
standby it sets `ControlFile->state = DB_SHUTDOWNED_IN_RECOVERY`
(`xlog.c:7674`).

#### 3.2 The `CheckPoint` record payload

The body of an `XLOG_CHECKPOINT_*` record is `struct CheckPoint`
(`pg_control.h:35-65`). Fields:

| Field | Purpose |
| --- | --- |
| `redo` | redo start LSN (see Section 2.4) |
| `ThisTimeLineID` / `PrevTimeLineID` | TLI of this and the previous epoch |
| `fullPageWrites` | whether FPW was on when the checkpoint started |
| `wal_level` | the `wal_level` GUC value |
| `nextXid` | next free XID (full-XID-epoch encoded) |
| `nextOid` | next free OID |
| `nextMulti`, `nextMultiOffset` | next free MXID and offset |
| `oldestXid` / `oldestXidDB` | cluster-wide minimum frozen XID and its DB |
| `oldestMulti` / `oldestMultiDB` | same for MultiXact |
| `oldestCommitTsXid` / `newestCommitTsXid` | commit-ts range |
| `oldestActiveXid` | only meaningful for online ckpt when wal_level=replica |
| `time` | wall-clock timestamp |

A literal copy of this struct is also stored in `pg_control` as
`checkPointCopy` (`pg_control.h:135`); that lets startup recover all the
XID / OID / MultiXact horizons before opening the WAL at all
(`xlog.c:5530-5543`).

#### 3.3 The two-phase checkpoint protocol

For an online checkpoint:

```
CreateCheckPoint(0):
  SyncPreCheckpoint()                                  // xlog.c:6919
  START_CRIT_SECTION()                                 // xlog.c:6924
  WALInsertLockAcquireExclusive()                      // xlog.c:6988
  WALInsertLockRelease()                               // xlog.c:7032
  // ---- pivot point ----
  XLogBeginInsert()
  XLogRegisterData(&wal_level, ...)
  XLogInsert(RM_XLOG_ID, XLOG_CHECKPOINT_REDO)          // xlog.c:7048
    -> this LSN becomes checkPoint.redo
  checkPoint.redo = RedoRecPtr                          // xlog.c:7056
  // ---- now flush every dirty buffer (CheckPointGuts, not shown) ----
  CheckPointGuts(checkPoint.redo, flags)
  // ---- now write the visible ckpt record ----
  XLogInsert(RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE)        // ~xlog.c:7228
  // ---- pg_control update under ControlFileLock ----
  LWLockAcquire(ControlFileLock, LW_EXCLUSIVE)         // xlog.c:7258
    ControlFile->checkPoint = ProcLastRecPtr            // xlog.c:7261
    ControlFile->checkPointCopy = checkPoint            // xlog.c:7262
    ControlFile->minRecoveryPoint = InvalidXLogRecPtr   // xlog.c:7264
    UpdateControlFile()                                 // xlog.c:7274
  LWLockRelease(ControlFileLock)
  END_CRIT_SECTION()
  KeepLogSeg(); RemoveOldXlogFiles()                    // xlog.c:7326,7350
```

For a shutdown checkpoint (`CHECKPOINT_IS_SHUTDOWN`):

```
LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
ControlFile->state = DB_SHUTDOWNING;       // xlog.c:6929
UpdateControlFile();
LWLockRelease(ControlFileLock);
// ... fix redo pointer from current insert position (xlog.c:6993-7026) ...
// ... no XLOG_CHECKPOINT_REDO record because no concurrent WAL ...
// ... write XLOG_CHECKPOINT_SHUTDOWN ...
ControlFile->state = DB_SHUTDOWNED;         // xlog.c:7260
UpdateControlFile();
```

The same `CreateCheckPoint` is also invoked at the end of crash recovery
with `CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE` (`xlog.c:6632`) — the
recovery process exits through a shutdown-style checkpoint so the next
startup sees `DB_SHUTDOWNED`, not `DB_IN_CRASH_RECOVERY` again. The
sanity-check at `xlog.c:7245-7247` (`shutdown && checkPoint.redo !=
ProcLastRecPtr`) PANICs if anything else managed to insert WAL between the
redo-pointer fixing and the checkpoint record — for shutdown there must be
nothing in between.

### 4. WAL Record Structure

#### 4.1 `XLogRecord` header

Every WAL record begins with a fixed-size `XLogRecord` header
(`src/include/access/xlogrecord.h:41-53`):

```c
typedef struct XLogRecord
{
    uint32        xl_tot_len;  /* total len of entire record */
    TransactionId xl_xid;      /* xact id */
    XLogRecPtr    xl_prev;     /* ptr to previous record in log */
    uint8         xl_info;     /* flag bits, see below */
    RmgrId        xl_rmid;     /* resource manager for this record */
    /* 2 bytes padding */
    pg_crc32c     xl_crc;      /* CRC for this record */
} XLogRecord;
```

`SizeOfXLogRecord` is `offsetof(xl_crc) + sizeof(pg_crc32c)` = 24 bytes
(`xlogrecord.h:55`).

`xl_info`'s low 4 bits are reserved for the framework
(`XLR_INFO_MASK = 0x0F`); the high 4 bits are owned by the rmgr
(`XLR_RMGR_INFO_MASK = 0xF0`), and that's how `xlog_redo` distinguishes
`XLOG_CHECKPOINT_SHUTDOWN` (`0x00`) from `XLOG_CHECKPOINT_ONLINE` (`0x10`)
from `XLOG_END_OF_RECOVERY` (`0x90`), etc. (`pg_control.h:67-82`).

The overall record layout (per the comment block at `xlogrecord.h:20-39`):

```
+------------------+
| XLogRecord       |  24 bytes, MAXALIGNed
+------------------+
| XLogRecordBlockHeader   |  0 or more, one per referenced block
| XLogRecordBlockHeader   |
| ...                     |
+------------------+
| XLogRecordDataHeader[Short|Long] |  describes the main-data length
+------------------+
| block 0 data     |  per-block payload (full page image or delta)
| block 1 data     |
| ...              |
+------------------+
| main rmgr data   |  unstructured payload owned by the rmgr
+------------------+
```

`XLogRecordBlockHeader` (`xlogrecord.h:103`) and the block image / compress
sub-headers (`:141`, `:173`) describe each referenced page; the flag bits
that matter most for recovery are `BKPBLOCK_HAS_IMAGE` (this record carries
a full page image), `BKPBLOCK_WILL_INIT` (the page will be zeroed and rebuilt
by redo), and on the image header `BKPIMAGE_APPLY` (the image should be
restored even if the page on disk looks newer) — see `xlogrecord.h:158`,
`:198-201`.

#### 4.2 LSN as a position

An `XLogRecPtr` is a 64-bit byte offset into the conceptual contiguous WAL
stream (`src/include/access/xlogdefs.h`, typedefed to `uint64`). It's
printed as `%X/%X` (high 32 / low 32). The byte offset directly maps to a
segment file via `XLByteToSeg`:
```
segno = lsn / wal_segment_size
```
(`xlog_internal.h:117`). With the default 16 MB segment size, segment file
`000000010000000000000007` is the segment whose LSN range is
`[0/07000000, 0/08000000)`.

Two LSNs are key to crash recovery:

- `ReadRecPtr`: the LSN at which the **current** record begins.
- `EndRecPtr`: the LSN immediately after the current record. This is what
  gets stamped onto pages during redo so subsequent reads can decide
  idempotently whether to skip a record.

`XLogReaderState` exposes both. The redo loop reads them at
`xlogrecovery.c:1773-1774`.

#### 4.3 Written / flushed / inserted distinctions

PG tracks three notions of "how far we are in the WAL stream" at any moment
(`xlog.c:326-336`):

```c
typedef struct XLogwrtRqst { XLogRecPtr Write; XLogRecPtr Flush; } XLogwrtRqst;
typedef struct XLogwrtResult { XLogRecPtr Write; XLogRecPtr Flush; } XLogwrtResult;
```

| Concept | What it means | Function to read |
| --- | --- | --- |
| **Inserted** | Backend has copied bytes into the WAL buffers in shared memory | `GetXLogInsertRecPtr()` |
| **Written** | `write(2)` has been issued to the segment file (probably still in OS page cache) | tracked in `LogwrtResult.Write`, read via `GetXLogWriteRecPtr` |
| **Flushed** | `fdatasync()`/`fsync()` has returned — bytes are durable on disk | `GetFlushRecPtr()` / `LogwrtResult.Flush` |

For crash recovery the only LSN that matters is **flushed**: anything
written but not flushed disappears in a power-cut. The commit path
(`RecordTransactionCommit` -> `XLogFlush`) blocks until the commit
record's EndLSN is flushed; this is the durability boundary. For
this report's purposes it's enough to remember: redo can only replay
records that were durable at crash time.

#### 4.4 Per-page header and segment files

WAL files live in `$PGDATA/pg_wal` (`xlog_internal.h:149`). Each segment is
a fixed-size file (`wal_segment_size`, default 16 MB) named with a 24-hex
filename containing the timeline ID, the high 32 bits of LSN / segment-count,
and the low 32 bits:
```
00000001 00000000 00000007   <- timeline 1, log 0, seg 7
```
Format helpers at `xlog_internal.h:165-176`.

Within a segment, every `XLOG_BLCKSZ`-byte page (default 8 KB) carries an
`XLogPageHeaderData` (`xlog_internal.h:36-50`):
```c
uint16     xlp_magic;
uint16     xlp_info;   // XLP_FIRST_IS_CONTRECORD, XLP_LONG_HEADER, ...
TimeLineID xlp_tli;
XLogRecPtr xlp_pageaddr;
uint32     xlp_rem_len;
```
On the first page of each segment a long header
(`XLogLongPageHeaderData`, `:61-67`) follows, carrying the system
identifier and the segment / block sizes — so that an arbitrary segment
file can be validated against `pg_control`.

Records do not respect page boundaries. When a record runs past the
end of a page, the next page header has `XLP_FIRST_IS_CONTRECORD` set
(`xlog_internal.h:74`) and `xlp_rem_len` holds the number of bytes still
to come.

#### 4.5 Resource managers and the `RmgrTable`

WAL records are dispatched to a resource manager (rmgr) for both
description and replay. `RmgrData` (`xlog_internal.h:349-360`):

```c
typedef struct RmgrData
{
    const char *rm_name;
    void  (*rm_redo)    (XLogReaderState *record);
    void  (*rm_desc)    (StringInfo buf, XLogReaderState *record);
    const char *(*rm_identify)(uint8 info);
    void  (*rm_startup) (void);
    void  (*rm_cleanup) (void);
    void  (*rm_mask)    (char *pagedata, BlockNumber blkno);
    void  (*rm_decode)  (struct LogicalDecodingContext *, struct XLogRecordBuffer *);
} RmgrData;
extern PGDLLIMPORT RmgrData RmgrTable[];
```

`RmgrTable[]` is indexed by `RmgrId`. The built-in entries are listed in
`src/include/access/rmgrlist.h:28-49`:

```
RM_XLOG_ID     "XLOG"         xlog_redo       (checkpoints, FPI, ...)
RM_XACT_ID     "Transaction"  xact_redo       (COMMIT, ABORT, PREPARE)
RM_SMGR_ID     "Storage"      smgr_redo       (file creation/truncation)
RM_CLOG_ID     "CLOG"         clog_redo
RM_DBASE_ID    "Database"     dbase_redo
RM_TBLSPC_ID   "Tablespace"   tblspc_redo
RM_MULTIXACT_ID "MultiXact"   multixact_redo
RM_RELMAP_ID   "RelMap"       relmap_redo
RM_STANDBY_ID  "Standby"      standby_redo
RM_HEAP2_ID    "Heap2"        heap2_redo
RM_HEAP_ID     "Heap"         heap_redo
RM_BTREE_ID    "Btree"        btree_redo
RM_HASH_ID     "Hash"         hash_redo
RM_GIN_ID      "Gin"          gin_redo
RM_GIST_ID     "Gist"         gist_redo
RM_SEQ_ID      "Sequence"     seq_redo
RM_SPGIST_ID   "SPGist"       spg_redo
RM_BRIN_ID     "BRIN"         brin_redo
RM_COMMIT_TS_ID "CommitTs"    commit_ts_redo
RM_REPLORIGIN_ID "ReplicationOrigin" replorigin_redo
RM_GENERIC_ID  "Generic"      generic_redo
RM_LOGICALMSG_ID "LogicalMessage" logicalmsg_redo
```

Custom rmgr ids (used by extensions) register through `RegisterCustomRmgr`
(`xlog_internal.h:366`). This is precisely the hook OrioleDB uses; see
Topic 2 for that interface.

Dispatch is a one-liner inside `ApplyWalRecord` (`xlogrecovery.c:2020`):
```c
GetRmgr(record->xl_rmid).rm_redo(xlogreader);
```
where `GetRmgr` (`xlog_internal.h:375-381`) does a bounds-check via
`RmgrIdExists()` and calls `RmgrNotFound` (FATAL) on a stale custom id.

### 5. The Startup Process and `StartupXLOG`

#### 5.1 Entry point and high-level flow

The postmaster forks a dedicated "startup" auxiliary process whose main is
`StartupProcessMain`. Its single duty is to drive `StartupXLOG`
(`xlog.c:5389`). On success, the startup process exits and the postmaster
opens for connections; on failure it raises `FATAL` (typical message at
`xlog.c:5421` for a bad control file).

`StartupXLOG` is ~800 lines (`xlog.c:5389-6193`). Reading top-to-bottom:

```
1.  Sanity-check ControlFile->checkPoint (xlog.c:5420)
2.  switch (ControlFile->state):
       log appropriate message                              (xlog.c:5425-5476)
3.  ValidateXLOGDirectoryStructure()                        (xlog.c:5489)
4.  If state != SHUTDOWNED and != SHUTDOWNED_IN_RECOVERY:
       RemoveTempXlogFiles() + SyncDataDirectory()         (xlog.c:5510-5516)
       didCrash = true
5.  InitWalRecovery() -- in-memory ControlFile mutated      (xlog.c:5528)
       (sets DB_IN_CRASH_RECOVERY or DB_IN_ARCHIVE_RECOVERY)
6.  Initialize TransamVariables (nextXid, nextOid, ...)     (xlog.c:5533-5543)
       from checkPointCopy
7.  RelationCacheInitFileRemove(),
    StartupReplicationSlots(),
    StartupReorderBuffer(),
    StartupCLOG(), StartupMultiXact(), StartupCommitTs(),
    StartupReplicationOrigin()                              (xlog.c:5557-5595)
8.  RedoRecPtr = checkPoint.redo                            (xlog.c:5651)
9.  if (InRecovery):
       UpdateControlFile()    -- flushes the IN_CRASH_RECOVERY (xlog.c:5673)
       PerformWalRecovery()                                 (xlog.c:5810)
10. FinishWalRecovery()  -- returns EndOfLog, EndOfLogTLI   (xlog.c:5819)
11. (if needed) write missing-contrecord, switch TLI, etc.  (~xlog.c:5970)
12. Initialise XLOG insert position for new writes          (xlog.c:5984-6028)
13. PerformRecoveryXLogAction()
       -> usually CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN |
                                   CHECKPOINT_END_OF_RECOVERY)
       -> or, if a promotion, write XLOG_END_OF_RECOVERY only
                                                            (xlog.c:6120,
                                                             xlog.c:6632 for ckpt,
                                                             xlog.c:7416 for the EOR rec)
14. LWLockAcquire(ControlFileLock, EXCLUSIVE)               (xlog.c:6157)
    ControlFile->state = DB_IN_PRODUCTION                   (xlog.c:6158)
    SharedRecoveryState = RECOVERY_STATE_DONE
    UpdateControlFile()                                     (xlog.c:6164)
    LWLockRelease()
15. ShutdownRecoveryTransactionEnvironment()                (xlog.c:6177)
16. (postmaster takes over, opens for connections)
```

#### 5.2 Picking the recovery starting LSN

`InitWalRecovery` (`xlogrecovery.c:512`) is the function that decides
*where* to start. Two cases:

**(a)** A `backup_label` file is present (recovering from a base backup;
PITR or fresh standby). `read_backup_label` (`xlogrecovery.c:391` decl)
loads `CheckPointLoc`, `CheckPointTLI`, `backupEndRequired`,
`backupFromStandby`, and a `backup_method` string. The redo start LSN is
taken from the backup label, not pg_control. (See `xlogrecovery.c:592` for
the entry into this branch.)

**(b)** No `backup_label`. This is the crash-recovery case
(`xlogrecovery.c:773-809`):
```c
CheckPointLoc = ControlFile->checkPoint;
CheckPointTLI = ControlFile->checkPointCopy.ThisTimeLineID;
RedoStartLSN  = ControlFile->checkPointCopy.redo;
RedoStartTLI  = ControlFile->checkPointCopy.ThisTimeLineID;
record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc, CheckPointTLI);
```
If the checkpoint record can't be read or its CRC fails, the process
PANICs at `xlogrecovery.c:794-796`. There is **no** fallback to a secondary
checkpoint anymore; the comment at `xlogrecovery.c:789-793` is explicit
that the older "secondary checkpoint" mechanism has been removed.

After reading the checkpoint record, `wasShutdown` is recorded
(`xlogrecovery.c:799`) from `xl_info & ~XLR_INFO_MASK == XLOG_CHECKPOINT_SHUTDOWN`.
If `checkPoint.redo < CheckPointLoc` (the online-checkpoint case), startup
seeks back to `redo` and verifies it can read forward from there
(`xlogrecovery.c:801-809`).

The decision to actually replay (`InRecovery = true`) is at
`xlogrecovery.c:915-928`:
- if `checkPoint.redo < CheckPointLoc` -> online checkpoint, replay is
  required.
- else if state was not `DB_SHUTDOWNED` -> not a clean shutdown, replay.
- else if archive recovery was requested -> replay even from a clean
  shutdown.

#### 5.3 `PerformWalRecovery` — the redo loop

`PerformWalRecovery` (`xlogrecovery.c:1662`) is the redo loop proper.
Skeleton:

```c
void PerformWalRecovery(void)
{
    /* set lastReplayedReadRecPtr, lastReplayedEndRecPtr to the redo point */
    /* tell postmaster we've started redo */
    /* (archive-recovery) check if we're already consistent */

    /* Pick first record: it might physically precede the ckpt record */
    if (RedoStartLSN < CheckPointLoc)
        record = ReadRecord(/* start at RedoStartLSN */);   // 1717
        /* verify it is XLOG_CHECKPOINT_REDO */
    else
        record = ReadRecord(/* start at after ckpt record */); // 1735

    RmgrStartup();                                          // 1747

    do {
        HandleStartupProcInterrupts();
        if (recoveryStopsBefore(xlogreader)) break;
        ApplyWalRecord(xlogreader, record, &replayTLI);     // 1832
        if (recoveryStopsAfter(xlogreader)) break;
        record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
    } while (record != NULL);                               // 1843

    RmgrCleanup();                                          // 1901
    ereport(LOG, "redo done at %X/%X system usage: ...");   // 1903
}
```

`RmgrStartup`/`RmgrCleanup` call `rm_startup` / `rm_cleanup` on every rmgr
with a non-NULL slot — that's how `btree_xlog_startup`, `gin_xlog_startup`,
`gist_xlog_startup`, `spg_xlog_startup` initialize their per-recovery work
state (`rmgrlist.h:39-44`).

`recoveryStopsBefore` / `recoveryStopsAfter` (`xlogrecovery.c:2602, 2775`)
are the PITR target-check hooks; for plain crash recovery
(`recoveryTarget == RECOVERY_TARGET_UNSET`) they always return false.

#### 5.4 `XLogReadRecord` — how a record is decoded

Reading is delegated to `ReadRecord` (`xlogrecovery.c:3180`), which in turn
calls `XLogPrefetcherReadRecord` (line `:3200`). At its core sit
`XLogReadRecord` and the `XLogReader` machinery in
`src/backend/access/transam/xlogreader.c` (not quoted here in full, but the
flow is: read the next `XLOG_BLCKSZ` page, validate the page header, read
`XLogRecord` header, validate `xl_tot_len`, gather any continuation bytes
from following pages, CRC-check the entire record, parse out the block
references and main data into the `XLogReaderState`).

CRC failure or a torn record at end-of-WAL is the *normal* way redo ends
in crash recovery: `ReadRecord` returns NULL, the do-while loop exits, and
the `errmsg("redo done at %X/%X")` line names the LSN of the last
successfully-replayed record. If the WAL turns out to be torn before
`minRecoveryPoint`, that's the FATAL at `xlog.c:5867`
("WAL ends before consistent recovery point").

#### 5.5 Per-rmgr dispatch

`ApplyWalRecord` (`xlogrecovery.c:1936`) is what actually runs the record.
It:

1. Pushes an error-context callback so a redo crash reports the LSN
   (`xlogrecovery.c:1942-1946`).
2. Advances `TransamVariables->nextXid` past `record->xl_xid`
   (`xlogrecovery.c:1951`).
3. Handles `XLOG_CHECKPOINT_SHUTDOWN` and `XLOG_END_OF_RECOVERY` early to
   compute the new replay TLI before they're dispatched
   (`xlogrecovery.c:1961-1993`).
4. Updates the shared `replayEndRecPtr` so `XLogFlush` can know how far
   redo has progressed (`xlogrecovery.c:2000-2003`).
5. For `RM_XLOG_ID` records, invokes the internal helper `xlogrecovery_redo`
   (`xlogrecovery.c:2016-2017`) which centralizes things like
   `XLOG_FPW_CHANGE`, `XLOG_PARAMETER_CHANGE`, etc.
6. Then dispatches to the rmgr (`xlogrecovery.c:2020`):
   ```c
   GetRmgr(record->xl_rmid).rm_redo(xlogreader);
   ```
7. If `record->xl_info & XLR_CHECK_CONSISTENCY`, calls
   `verifyBackupPageConsistency` (`xlogrecovery.c:2027-2028`) — this is
   the `wal_consistency_checking` GUC's mechanism.
8. Updates `lastReplayedReadRecPtr` / `lastReplayedEndRecPtr` to the just-
   replayed record (`xlogrecovery.c:2037-2041`).
9. Wakes up walsenders so a cascaded standby learns of new data
   (`xlogrecovery.c:2058-2069`).

### 6. Page-LSN Idempotence

The redo loop is **idempotent** at the page level. The same WAL records
can be replayed multiple times (e.g. crash during recovery -> replay from
scratch again) without corrupting data. The mechanism is dead simple: every
heap/index page on disk carries the LSN of the last WAL record that
modified it, and redo skips a record whose `EndRecPtr` is `<=` the on-disk
LSN.

#### 6.1 The LSN-on-page rule

Standard PostgreSQL pages have an `XLogRecPtr` in the page header
(`PageGetLSN` / `PageSetLSN`, defined in
`src/include/storage/bufpage.h`). The invariant is:

> A page on disk reflects all WAL records up to and including the one whose
> EndLSN equals the page's stored LSN.

This invariant is enforced by **two coordinated rules**:

1. **Forward path (normal operation)**: whenever a buffer is modified
   under `XLogInsert`, the redo routine — *and* the original
   `XLogInsert` caller via `MarkBufferDirty` + `PageSetLSN` — stamps the
   page with the inserted record's EndLSN.
2. **Buffer-flush ordering**: the buffer manager refuses to flush a dirty
   page to disk before the WAL up to that page's LSN has been flushed
   (this is the WAL-before-data rule, sometimes called "WAL log-ahead").
   Implementation: `XLogFlush(PageGetLSN(page))` inside `FlushBuffer`
   (`src/backend/storage/buffer/bufmgr.c`).

The combination guarantees: if a page on disk shows LSN `L`, then the WAL
record whose EndLSN is `L` was flushed to disk **before** that page version
landed. So during redo, the first time we see a record `R` with EndLSN
`<= L`, we know the on-disk page already reflects `R`.

#### 6.2 How `XLogReadBufferForRedo` enforces it

The redo path's read function is `XLogReadBufferForRedo` (and
`XLogReadBufferForRedoExtended`, `src/backend/access/transam/xlogutils.c:313, 351`).
Its return value tells the rmgr what to do:

```c
typedef enum XLogRedoAction
{
    BLK_NEEDS_REDO,  // pg has the page, but it's older than the record
    BLK_DONE,        // page on disk is already at or past record EndLSN
    BLK_RESTORED,    // record had a FPI; the page was restored verbatim
    BLK_NOTFOUND     // page was truncated away by a later record
} XLogRedoAction;
```
(`xlogutils.c:288-296`)

The decision logic, paraphrased from `xlogutils.c:384-434`:

```c
XLogRecPtr lsn = record->EndRecPtr;   // line 356

if (XLogRecBlockImageApply(record, block_id)) {
    // Record carries a full page image and it must apply
    // (used when the on-disk page might be torn).
    *buf = XLogReadBufferExtended(... RBM_ZERO_AND_LOCK ...);
    RestoreBlockImage(record, block_id, page);
    if (!PageIsNew(page))
        PageSetLSN(page, lsn);              // line 403
    MarkBufferDirty(*buf);
    return BLK_RESTORED;
}
else {
    *buf = XLogReadBufferExtended(... mode ...);
    if (BufferIsValid(*buf)) {
        LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
        if (lsn <= PageGetLSN(BufferGetPage(*buf)))
            return BLK_DONE;                // line 431-432
        else
            return BLK_NEEDS_REDO;
    }
    ...
}
```

The rmgr's redo function pattern (e.g. `heap_redo`) checks the return:
```c
action = XLogReadBufferForRedo(record, 0, &buffer);
if (action == BLK_NEEDS_REDO) {
    /* apply the delta, then PageSetLSN, MarkBufferDirty */
}
else if (action == BLK_RESTORED) {
    /* full-page image was applied, nothing more to do */
}
/* BLK_DONE -> nothing */
```

#### 6.3 Why this lets redo be re-run

Two scenarios:

1. **Crash during redo, then redo restarts from the same checkpoint.** A
   page may have been re-stamped with a high LSN, then flushed, then redo
   crashed. Next time, when redo re-reads the WAL from `checkPoint.redo`,
   it hits the same records and `BLK_DONE`s past them.

2. **Crash where the buffer was modified but not yet flushed.** The page on
   disk still has its old, lower LSN; the record will be applied, the page
   re-stamped with `lsn`, and on next flush will land correctly.

Note that `BKPIMAGE_APPLY` (`xlogrecord.h:158`) deliberately violates the
`lsn <= PageGetLSN` shortcut: when the record carries a full image *and*
wants to apply it unconditionally, the image is restored even if the
on-disk page looks newer. This is the "torn page" protection — the page
on disk might *look* newer but be partially-overwritten garbage, so we
trust the (CRC-checked) WAL image over the (potentially torn) page. See
the comment block at `xlogutils.c:304-311`.

### 7. `minRecoveryPoint`, Full-Page Writes, Backup Blocks

#### 7.1 `minRecoveryPoint`

`minRecoveryPoint` is an *archive-recovery* device. The comment block at
`pg_control.h:139-148` is the canonical explanation:

> minRecoveryPoint is updated to the latest replayed LSN whenever we flush
> a data change during archive recovery. That guards against starting
> archive recovery, aborting it, and restarting with an earlier stop
> location. If we've already flushed data changes from WAL record X to
> disk, we mustn't start up until we reach X again.

For **crash recovery**, `minRecoveryPoint` is held at `InvalidXLogRecPtr`
(`xlog.c:5719-5721` sets `LocalMinRecoveryPoint = InvalidXLogRecPtr` when
not in archive recovery; `xlog.c:7264` clears it on every completed
checkpoint). The reason: crash recovery always replays all WAL up to the
end of the available log; there is no "we may stop mid-stream" risk to
guard against.

For archive recovery the value is advanced inside `XLogFlush` whenever a
data buffer is flushed during replay (see `UpdateMinRecoveryPoint`,
referenced from e.g. `xlog.c:7670`). The crash-vs-archive distinction is
explicit at `xlog.c:5712-5721`.

#### 7.2 Full-page writes (`XLOG_FPI`)

Full-page writes serve two distinct purposes:

1. **Torn-page protection at checkpoint boundary**. The first time a page
   is modified after a checkpoint, `XLogInsert` attaches a full page image
   (FPI) to that record. If the system crashes mid-write of that page, the
   image in the WAL can be restored. The page-attach decision is governed
   by `XLogCtl->Insert.RedoRecPtr` versus the page's LSN.

2. **Standalone FPI** (`XLOG_FPI`, `XLOG_FPI_FOR_HINT`,
   `pg_control.h:78-79`). When something other than a normal page update
   wants to record an image — e.g. hint-bit changes on a checksum-enabled
   cluster — an `XLOG_XLOG`-rmgr record of info `XLOG_FPI` /
   `XLOG_FPI_FOR_HINT` is written. Its body is just one or more backup
   blocks. Replay handler (`xlog.c:8534-8560`) is little more than
   `XLogReadBufferForRedo` on each registered block (which restores from
   the image and `BLK_RESTORED`s). The PANIC check at `xlog.c:8558-8559`
   verifies the image was actually present:
   ```c
   if (info == XLOG_FPI)
       elog(ERROR, "XLOG_FPI record did not contain a full-page image");
   ```

`full_page_writes` is a GUC; turning it off saves WAL volume but breaks
torn-page protection. Its current value is recorded in each checkpoint
(`checkPoint.fullPageWrites`, `pg_control.h:42`) and in
`pg_control.fullPageWrites` indirectly via the checkpoint copy. A
`XLOG_FPW_CHANGE` record (`pg_control.h:76`) is written whenever the GUC
toggles, so that hot-standby replays preserve the right behavior.

#### 7.3 Backup blocks attached to ordinary records

Most non-`XLOG_FPI` records carry *zero or more* backup blocks rather than
one dedicated to a full image. A heap update record, for example, references
the heap page being modified; the rmgr supplies a delta in main-data
(`XLogRecordDataHeader`, `xlogrecord.h:213-221`), and `XLogInsert`
optionally attaches a full image of that block depending on FPW state and
how stale the page is. From the replay side these are interchangeable:
`XLogReadBufferForRedo` checks `XLogRecBlockImageApply(record, block_id)`
(`xlogutils.c:385`) without caring whether the wrapper info byte was
`XLOG_FPI` or something else.

The per-block header structures (`xlogrecord.h:103-201`) hold:

- `id` (the block reference number).
- `fork_flags` containing `BKPBLOCK_HAS_IMAGE`, `BKPBLOCK_WILL_INIT`,
  `BKPBLOCK_SAME_REL` etc.
- `data_length` (length of rmgr-supplied delta, if any).
- If `HAS_IMAGE`: an `XLogRecordBlockImageHeader` with `length`, `hole_offset`,
  `bimg_info` (carrying `BKPIMAGE_HAS_HOLE`, `BKPIMAGE_APPLY`, and any
  compression flags).
- If not `SAME_REL`: a `RelFileLocator`.
- Always: a `BlockNumber`.

The "hole" is the gap between `pd_lower` and `pd_upper` in a heap page;
PG omits zero-bytes within the hole from the image and the replay path
reconstructs them via `RestoreBlockImage` (`xlogutils.c:392`).

### 8. Transition from Recovery to Production

#### 8.1 End-of-recovery checkpoint

After the redo loop exits (record == NULL), `FinishWalRecovery`
(`xlog.c:5819`) wraps things up: it returns an `EndOfWalRecoveryInfo`
struct that carries the final LSN at which WAL was successfully read
(`endOfLog`) and the timeline. Then `StartupXLOG` initializes its own WAL
insert position to begin writing the post-recovery WAL stream
(`xlog.c:5984-6028`).

`PerformRecoveryXLogAction` (`xlog.c:6274`) is the choice of how to mark
end-of-recovery in the WAL:

- For a crash-recovery shutdown (the common case) it calls
  `CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE)`
  (`xlog.c:6632`). This is a *full* shutdown-style checkpoint: it
  flushes all dirty buffers, writes a `XLOG_CHECKPOINT_SHUTDOWN` record,
  and updates `pg_control` with `state = DB_SHUTDOWNED` and the new
  checkpoint LSN. Then `StartupXLOG` flips the state to `DB_IN_PRODUCTION`
  in step 14.

- For a *promotion* from standby (archive recovery + `PromoteIsTriggered()`),
  the lighter `CreateEndOfRecoveryRecord` (`xlog.c:7394`) is used instead.
  This writes only a tiny `XLOG_END_OF_RECOVERY` record:
  ```c
  XLogBeginInsert();
  XLogRegisterData(&xlrec, sizeof(xl_end_of_recovery));
  recptr = XLogInsert(RM_XLOG_ID, XLOG_END_OF_RECOVERY);  // 7416
  XLogFlush(recptr);
  ```
  The full checkpoint is deferred (`RequestCheckpoint(CHECKPOINT_FORCE)` at
  `xlog.c:6192`) so the promoted primary can start accepting writes
  immediately. The `xl_end_of_recovery` payload (`xlog_internal.h:300-306`)
  carries the new and previous TLI.

The shutdown-style end-of-recovery checkpoint also serves as the place
where the cluster picks a new timeline ID if archive recovery requested
one. The rationale comment is at `xlog.c:6267-6273`: "since we may be
assigning a new TLI, using a shutdown checkpoint allows us to have the
rule that TLI only changes in shutdown checkpoints."

#### 8.2 `pg_control` state flip

The actual transition to production is `xlog.c:6157-6165`:

```c
LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
ControlFile->state = DB_IN_PRODUCTION;

SpinLockAcquire(&XLogCtl->info_lck);
XLogCtl->SharedRecoveryState = RECOVERY_STATE_DONE;
SpinLockRelease(&XLogCtl->info_lck);

UpdateControlFile();
LWLockRelease(ControlFileLock);
```

Note that two pieces of state are flipped together under
`ControlFileLock`:

- The on-disk `pg_control.state`.
- The shared-memory `XLogCtl->SharedRecoveryState`, the value
  `RecoveryInProgress()` consults.

The comment at `xlog.c:6149-6151` warns that there is *still* a small
window where backends might write WAL while the on-disk control file is
still saying `DB_IN_CRASH_RECOVERY`; this is acceptable because the
shared-memory flag (which is what backends consult, not the on-disk file)
flipped together with the lock.

After this point `RecoveryInProgress()` returns false and backends can
start inserting WAL.

#### 8.3 Opening for connections

The postmaster has already started, but it gates new backend connections
on the startup process's signal. When the startup process exits cleanly,
the postmaster transitions to the normal "running" state and `pg_hba.conf`
becomes effective for new connections. `WalSndWakeup(true, true)`
(`xlog.c:6183`) prods any cascaded standby walsenders so they notice the
state change.

For a promotion path, `RequestCheckpoint(CHECKPOINT_FORCE)`
(`xlog.c:6192`) asks the checkpointer (which is already running, alongside
the startup process during recovery) to write a full checkpoint as soon as
it can — without blocking the startup process or the accepting of
connections.

### 9. Sequence Diagrams

#### 9.1 Crash + recovery cycle (clean view)

```
   ----- normal operation -----                ------- crash ------    ------- recovery -------
postmaster                                     SIGKILL/power-off       postmaster starts
   |                                                  X                    |
   | fork(checkpointer)                                                    | fork(startup)
   v                                                                       v
checkpointer ---------- N seconds ------------+                       StartupProcessMain
   |                                          |                            |
   | CreateCheckPoint(0):                     |                            v
   |   XLogInsert(CHECKPOINT_REDO)            |                       StartupXLOG()
   |     -> Redo = X                          |                            |
   |   CheckPointGuts -> flush all bufs       |                            | ReadControlFile
   |     dirty before X                       |                            | state = DB_IN_PRODUCTION
   |   XLogInsert(CHECKPOINT_ONLINE)          |                            | -> SyncDataDirectory
   |   pg_control:                            |                            | -> InitWalRecovery
   |     state = IN_PRODUCTION                |                            |    CheckPointLoc = pg_control.checkPoint
   |     checkPoint = <ckpt LSN>              |                            |    RedoStartLSN = ckptCopy.redo = X
   |     checkPointCopy = <body>              |                            |    state = DB_IN_CRASH_RECOVERY
   |                                          |                            |    UpdateControlFile()  // persisted
backend (multiple)                            |                            | -> PerformWalRecovery
   |  XLogInsert(...heap update at LSN Y)     |                            |    for record from X .. EOL:
   |  XLogFlush(Y) on commit                  |                            |       ApplyWalRecord -> GetRmgr(...).rm_redo
   |  buffer dirtied at LSN Y                 |                            |    until ReadRecord returns NULL
   |                                          |                            |
   |  background: bgwriter / checkpointer     |                            | -> PerformRecoveryXLogAction
   |    flushes dirty pages w/ PageGetLSN(p)  |                            |    -> CreateCheckPoint(SHUTDOWN|END_OF_RECOVERY)
   |    >= already-flushed WAL                |                            |       state = DB_SHUTDOWNED
   |                                          |                            |       UpdateControlFile()
   v                                          v                            |
   X (crash)                                  X                            | -> ControlFile->state = DB_IN_PRODUCTION
                                                                           |    UpdateControlFile()
                                                                           v
                                                                       postmaster opens for connections
```

#### 9.2 Per-record redo dispatch

```
PerformWalRecovery loop iteration:

  record = ReadRecord()
      |
      | XLogReader reads next page from pg_wal,
      | reassembles continuation pieces,
      | CRC-checks the record
      v
  if record == NULL -> exit redo loop
      |
      v
  ApplyWalRecord(record):
    AdvanceNextFullTransactionIdPastXid(xl_xid)
    if RM_XLOG_ID and CHECKPOINT_SHUTDOWN / END_OF_RECOVERY:
        compute newReplayTLI, switch TLI if needed
    Update XLogRecoveryCtl->replayEndRecPtr = record.EndRecPtr
    if RM_XLOG_ID: xlogrecovery_redo()   // handle FPW_CHANGE, PARAMETER_CHANGE, etc
    GetRmgr(record->xl_rmid).rm_redo(xlogreader)
        |
        | rmgr opens each referenced buffer:
        |   action = XLogReadBufferForRedo(record, blkid, &buf)
        |     -> if BLK_RESTORED: image already applied, skip
        |     -> if BLK_DONE:     PageGetLSN >= EndRecPtr, skip
        |     -> if BLK_NEEDS_REDO: apply rmgr-specific delta,
        |                           PageSetLSN(page, record.EndRecPtr),
        |                           MarkBufferDirty
        v
    if record.xl_info & XLR_CHECK_CONSISTENCY:
        verifyBackupPageConsistency()
    update lastReplayedReadRecPtr/EndRecPtr to record's LSNs
```

#### 9.3 pg_control state diagram

```
                       initdb
                         |
                         v
       +-----------> DB_STARTUP
       |                 |  (bootstrap finished)
       |                 v
       |        DB_SHUTDOWNED <-----------------------+
       |                 |                            |
       |     (StartupXLOG sees clean shutdown)        |
       |                 |                            |
       |                 v                            |
       |        DB_IN_PRODUCTION                      |
       |             |       \                        |
       |   pg_ctl    |        \  immediate-shutdown   |
       |   stop -m   |         \  / power loss        |
       |   fast      |          v                     |
       |             v       (crash on disk)          |
       |    DB_SHUTDOWNING       |                    |
       |             |           v                    |
       |   (clean    |   DB_IN_CRASH_RECOVERY         |
       |    ckpt)    |     (next startup)             |
       |             v           |                    |
       +--- DB_SHUTDOWNED        |                    |
                                 |                    |
                                 |  ckpt at end       |
                                 |  of recovery       |
                                 v                    |
                          DB_SHUTDOWNED               |
                                 |                    |
                          flip to PRODUCTION ---------+
```

Archive-recovery side (used for standbys and PITR):

```
DB_IN_ARCHIVE_RECOVERY  <-- InitWalRecovery if archive recovery requested
        |
        | clean shutdown of standby
        v
DB_SHUTDOWNED_IN_RECOVERY  <-- via restart point with CHECKPOINT_IS_SHUTDOWN
                                (xlog.c:7674)
```

### 10. Cross-References for the Other Topics

This is intentionally the *baseline* document. Where vanilla PG bottoms out
and OrioleDB picks up:

- **Custom rmgr registration** (`RegisterCustomRmgr`,
  `xlog_internal.h:366`). Topic 2 (`recovery_report_topic2_pg_oriole_interface.md`)
  describes how OrioleDB registers its own `rm_redo` slot and what arrives
  there.

- **The `RmgrTable[r].rm_redo(record)` call site** at
  `xlogrecovery.c:2020`. The exact dispatch point where a generic
  `XLogReaderState` becomes OrioleDB's per-record bookkeeping. Topic 3
  (orioledb deep-dive) starts immediately past this line.

- **`rm_startup` / `rm_cleanup` lifecycle hooks** in `RmgrData`
  (`xlog_internal.h:355-356`), called by `RmgrStartup()`/`RmgrCleanup()`
  in `PerformWalRecovery` (`xlogrecovery.c:1747, 1901`). OrioleDB uses
  this for things like in-memory undo-buffer setup at recovery start.

- **Page-LSN idempotence in `XLogReadBufferForRedo`**
  (`xlogutils.c:431-432`). This concept does **not** translate directly
  to OrioleDB because OrioleDB pages do not live in PG's shared-buffer
  pool — Topic 3 will explain how OrioleDB achieves an equivalent
  guarantee through a different mechanism (page-state versioning in
  `src/btree/page_state.c`).

- **`minRecoveryPoint`** (`pg_control.h:168`) is *unused* in plain crash
  recovery (held at `Invalid`). OrioleDB's row-level WAL doesn't need it
  either — see Topic 4 for the WAL-sequence edge cases this raises.

- **End-of-recovery shutdown checkpoint** (`xlog.c:6632`,
  `PerformRecoveryXLogAction`). OrioleDB hooks the recovery completion
  via the `after_checkpoint_cleanup_hook` (`xlog.c:6139-6140` in this
  patched build); see Topic 2 for the hook contract.

- **`xact_redo` for COMMIT / ABORT records** (`rmgrlist.h:29`,
  `RM_XACT_ID`). These records *also* drive OrioleDB undo handling
  during recovery — OrioleDB's `o_xact_callback` and `xact_redo`
  interact in subtle ways covered in Topics 2 and 3.

For the active SK-leak investigation specifically, the relevant
baseline-PG behaviour is:

- A backend's `XLOG_XACT_COMMIT` record must be flushed before the
  transaction is reported committed to the client (this is the
  `RecordTransactionCommit` -> `XLogFlush` ordering).
- Crash recovery replays each rmgr's `rm_redo` once for each successfully
  CRCd record, in LSN order. There is no "second pass" or out-of-order
  application in the baseline.
- The on-disk page LSN is the only piece of recovery state that survives
  a crash without going through the WAL. Everything else — open
  transactions, undo state, locks — is reconstructed *from* the WAL by
  the rmgrs.

When reading the crash logs in `test/t/crash/results/`, the lines from
PG itself ("redo starts at X/Y", "redo done at X/Y system usage",
"database system was interrupted while in recovery", etc.) all originate
from the functions referenced above. The exact `errmsg("redo starts at
%X/%X")` is at `xlogrecovery.c:1750`, "redo done at %X/%X system usage"
at `xlogrecovery.c:1904`.

---

## 2. PG ↔ OrioleDB interface boundary

*(Source: `recovery_report_topic2_pg_oriole_interface.md`; original title: "Topic 2 — The PG ↔ OrioleDB Interface Boundary During WAL Recovery")*
> Scope: the *interface boundary* between PostgreSQL and the OrioleDB extension
> during crash / WAL recovery. What hooks orioledb registers, how vanilla PG
> finds its way into orioledb's redo code, what stays in PG-land vs.
> orioledb-land, and what the patches17_6 fork of PostgreSQL adds to make the
> bridge possible.
>
> Sibling topics (handled by other agents): Topic 1 (baseline PG recovery),
> Topic 3 (orioledb-specific recovery internals — undo, csn, page-state),
> Topic 4 (WAL-sequence edge cases). This document deliberately stops at the
> boundary; it does not narrate what happens inside orioledb's per-record
> handlers beyond identifying them.

### Three categories, one rule for every citation

Every concrete claim below is tagged as one of:

- **[VANILLA]** — code that exists in upstream PostgreSQL with no orioledb
  awareness. The custom-rmgr mechanism, the `RmgrData` struct, the
  `XLogReadRecord` loop. This part of the bridge is generic.
- **[PATCH17_6]** — code added to PostgreSQL by the orioledb fork's
  `patches17_6` branch. These are extension-point hooks (function pointers,
  callbacks) that vanilla PG does not expose. The fork keeps them
  source-compatible — none of these *require* orioledb, they just allow it.
- **[ORIOLEDB]** — code inside the `orioledb` extension (the `.so` loaded via
  `shared_preload_libraries`). This is where orioledb registers callbacks
  against the [VANILLA] and [PATCH17_6] hooks.

The full list of patches17_6 commits above PG 17.0 (tag `d7ec59a63d`) is
56 commits — see Appendix B for the relevant subset.

### H2 — Custom Resource Manager: how PG knows about orioledb during redo

OrioleDB uses PostgreSQL's [VANILLA] **custom WAL resource manager**
mechanism. The mechanism predates orioledb (PG 15 introduced it). Every WAL
record carries an `xl_rmid` byte; PG looks up `RmgrTable[xl_rmid]` and
calls `rm_redo` on the entry.

#### Reserved ID

OrioleDB hard-codes its custom rmgr ID:

- `/home/user/work/orioledb/include/orioledb.h:109` —
  `#define ORIOLEDB_RMGR_ID (129)`

Vanilla PG defines the legal range:

- `/home/user/work/postgres/src/include/access/rmgr.h:35-36` —
  `RM_MIN_CUSTOM_ID = 128`, `RM_MAX_CUSTOM_ID = UINT8_MAX (255)`
- `/home/user/work/postgres/src/include/access/rmgr.h:60` — `RM_EXPERIMENTAL_ID = 128`

`129` is one past `RM_EXPERIMENTAL_ID`, i.e. it is a **reserved** ID — orioledb
has gone through the wiki-coordinated reservation process referenced in
`/home/user/work/postgres/src/backend/access/transam/rmgr.c:100-104`.

#### Registration call

The orioledb extension registers its rmgr inside `_PG_init`:

- `/home/user/work/orioledb/src/orioledb.c:392-402` — `static RmgrData rmgr` declaration
- `/home/user/work/orioledb/src/orioledb.c:1262` —
  `RegisterCustomRmgr(ORIOLEDB_RMGR_ID, &rmgr);`

The struct shape is dictated by [VANILLA] PG:

- `/home/user/work/postgres/src/include/access/xlog_internal.h:349-360` — full
  `RmgrData` definition (8 function-pointer slots).

OrioleDB populates seven of the eight slots (no `rm_mask` — see below):

| Slot          | OrioleDB callback                          | When PG calls it                                          |
|---------------|--------------------------------------------|------------------------------------------------------------|
| `rm_name`     | `"OrioleDB resource manager"` (string)     | At log/diagnostic time                                     |
| `rm_startup`  | `o_recovery_start_hook` (recovery.c:1025)  | Once, from `RmgrStartup()` at top of `PerformWalRecovery`  |
| `rm_cleanup`  | `o_recovery_cleanup` (orioledb.c:387)      | Once, from `RmgrCleanup()` at the end of `PerformWalRecovery` |
| `rm_redo`     | `orioledb_redo` (recovery.c:1106)          | Per WAL record whose `xl_rmid == 129`                      |
| `rm_desc`     | `orioledb_rm_desc`                         | pg_waldump, log lines                                      |
| `rm_identify` | `orioledb_rm_identify`                     | pg_waldump, log lines                                      |
| `rm_mask`     | `NULL`                                     | Only used by `wal_consistency_checking`; orioledb opts out |
| `rm_decode`   | `orioledb_decode` (logical.c:1310)         | Logical decoding (snapbuild)                               |

`rm_mask = NULL` is deliberate: vanilla `wal_consistency_checking` compares
*pages* between primary and standby after replaying a record. OrioleDB does
not log page images — its WAL is row-level (see Topic 4) — so there is nothing
to mask and the feature is N/A.

#### Registration constraints

[VANILLA] `RegisterCustomRmgr` (rmgr.c:107-146) enforces: (1) non-empty
`rm_name`, (2) ID in the custom range, and (3)
`process_shared_preload_libraries_in_progress == true` — registration is
only legal from `_PG_init`. Hence orioledb's `_PG_init` early-out at
`orioledb.c:468-469`. Without preloading, the rmgr is never registered,
and PG's redo loop hits `RmgrNotFound` + `ereport(ERROR)` on the first
orioledb record (rmgr.c:91-95).

#### Where redo dispatch happens

[VANILLA] redo loop is in `xlogrecovery.c`:

- `/home/user/work/postgres/src/backend/access/transam/xlogrecovery.c:1662` —
  `PerformWalRecovery()`
- Line 1747 — `RmgrStartup()` — calls every registered rmgr's `rm_startup`
  callback. **For orioledb this is the first time control crosses the boundary**:
  vanilla PG → `o_recovery_start_hook()` (sets up worker pool, opens checkpoint
  state).
- Lines 1760-1843 — the main redo apply loop calls `ApplyWalRecord` per record.
- `xlogrecovery.c:2020` — the actual dispatch line:
  `GetRmgr(record->xl_rmid).rm_redo(xlogreader);`
- For orioledb records (`xl_rmid == 129`) this calls
  `orioledb_redo(XLogReaderState *record)` at
  `/home/user/work/orioledb/src/recovery/recovery.c:1106`.
- Line 1901 — `RmgrCleanup()` — calls `o_recovery_cleanup` →
  `o_recovery_finish_hook(cleanup=true)`.

So the *entire interaction* between PG's main redo loop and orioledb's
recovery code, in steady state, is: PG hands an `XLogReaderState*` to
`orioledb_redo()` and gets nothing back. All side effects (state machine
transitions, worker dispatch, undo replay) happen inside orioledb's process
memory.

#### The WAL container format

When orioledb emits a record, the payload is a self-describing **container** —
multiple internal "WAL_REC_*" sub-records concatenated. The header carries
`ORIOLEDB_WAL_VERSION` and per-container flags. PG sees only the `xl_rmid` and
the opaque data buffer.

Container build & emit site:

- `/home/user/work/orioledb/src/recovery/wal.c:864-912` —
  `log_logical_wal_container()`. Calls vanilla `XLogBeginInsert()`,
  `XLogRegisterData(...)`, `XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_CONTAINER)`.
- `/home/user/work/orioledb/include/orioledb.h:110` —
  `#define ORIOLEDB_XLOG_CONTAINER (0x00)` — orioledb's `xl_info` discriminator
  (only one value is used today, leaving the other 7 bits for future
  expansion).
- `/home/user/work/orioledb/src/recovery/recovery.c:1112` — recovery-side
  assert that the info byte matches.

Inner record-type list:

- `/home/user/work/orioledb/include/recovery/wal_record.h:42-60` — the X-macro
  `ORIOLE_WAL_RECORDS(X)` enumerates every per-tx-event record type
  (`WAL_REC_XID=1`, `WAL_REC_COMMIT=2`, `WAL_REC_ROLLBACK=3`, ...,
  `WAL_REC_RELREPLIDENT=18`).

### H2 — Table Access Method (TableAM): what gets called *outside* recovery

This is the section most likely to be misunderstood. OrioleDB does register a
full `TableAmRoutine` so that `CREATE TABLE ... USING orioledb` works and so
that normal-time queries can touch orioledb relations. **But almost none of
the `TableAmRoutine` slots are exercised during WAL redo.**

#### Where the TableAM is wired

`tableam/handler.c:2480-2542` — `static const TableAmRoutine
orioledb_am_methods` (60+ slots populated); `handler.c:2553` —
`orioledb_tableam_handler` (the function exported by `CREATE ACCESS METHOD
orioledb TYPE TABLE HANDLER ...`).

#### TableAM is producer-only; the redo path bypasses it entirely

The redo path does **not** loop over `RmgrTable` *and* `TableAmRoutine`. The
only entry into the extension during redo is via `rm_redo`:
`ApplyWalRecord` calls `GetRmgr(xl_rmid).rm_redo(record)` (xlogrecovery.c:2020)
— there is no `table_am->redo_modify(...)` slot in the upstream contract.

Inside `orioledb_redo` → `replay_container` → `replay_on_record`
(recovery.c:3678-3829), dispatch is by inner `WAL_REC_*` type, calling
orioledb-private primitives (`recovery_switch_to_oxid`, `apply_modify_record`,
`apply_sys_tree_modify_record`, `spread_idx_modify`, `o_truncate_table`,
`o_tables_meta_lock_no_wal`, `recovery_savepoint`). None of these go through
the `TableAmRoutine` vtable.

TableAM slots **never** called during redo include:
`orioledb_tuple_insert/update/delete` (handler.c:480+) — they *produce* the
WAL on the normal-ops side; `orioledb_index_build_range_scan` (handler.c:1138)
— recovery-side index builds use the worker pool instead; all `scan_*` slots
— no SQL scans run during redo; `orioledb_relation_size` (handler.c:1282).

The TableAM-to-rmgr mapping is **producer-side only**:

```
Normal ops:    SQL → executor → TableAm.tuple_insert → orioledb_tuple_insert
                                                          ↓
                                                      o_wal_insert (wal.c:917)
                                                          ↓
                                                      add_modify_wal_record(WAL_REC_INSERT, ...)
                                                          ↓
                                                      flush_local_wal → XLogInsert(rmid=129)

Crash & redo:  PG main loop → GetRmgr(129).rm_redo → orioledb_redo
                                                          ↓
                                                      replay_container → replay_on_record
                                                          ↓
                                                      case WAL_REC_INSERT: apply_modify_record (internal)
```

The TableAM slot **does not appear on the recovery side at all**.

#### Why this split exists

The TableAM is a per-backend, per-relation, per-tuple API tied to live SQL
execution (`Relation*`, `TupleTableSlot*`, `Snapshot*`). The redo path runs
in the startup process before any backend exists, before
`reachedConsistency` is true, before backends can open relations. The only
contract the redo path has with the extension is "here is a byte buffer
tagged with your rmid, apply it." Note also that
`tuple_complete_modification` ([PATCH17_6], commit `15967dbe43`) is a
*normal-ops* slot despite living in `TableAmRoutine` — it hooks the
executor's tuple-completion phase, not redo.

### H2 — patches17_6 hooks orioledb relies on

These are the additions PostgreSQL needs to carry to make the extension work.
They split into roughly four buckets: recovery-side, transaction-side,
checkpoint-side, and general extensibility (snapshot, locale, etc.). I list
the recovery-relevant ones first.

#### Recovery-side hooks (this is the Topic 2 core)

##### `RedoShutdownHook` — [PATCH17_6] (commit `c13889ae21`)

Decl `xlog.h:322`; called from `xlogrecovery.c:1886-1887`; orioledb binds
at `orioledb.c:1263` → `o_recovery_shutdown_hook` (orioledb.c:381) →
`o_recovery_finish_hook(false)`. Fires *only* on
`RECOVERY_TARGET_ACTION_SHUTDOWN` — orioledb drains its worker pool and
writes its undo-position checkpoint before PG's `proc_exit(3)`.
Distinct from `rm_cleanup` (which fires on every `PerformWalRecovery` exit).

##### `GetReplayXlogPtrHook` — [PATCH17_6] (commit `0a0631719b`)

Decl `xlogrecovery.h:51,100`; called at `xlogrecovery.c:4632-4633`;
orioledb binds at `orioledb.c:1280` → `recovery_get_effective_replay_ptr`
(recovery.c:1316). Overrides the LSN walsenders report — default is
`lastReplayedEndRecPtr`; orioledb returns `min(worker_ptrs[i].commitPtr)`
across parallel recovery workers, so walsenders don't claim a record is
replayed before all workers have consumed it.

##### `RecoveryStopsBeforeHook` — [PATCH17_6] (commit `85db5468bb`)

Decl `xlogrecovery.h:106`; called at `xlogrecovery.c:2647-2666`; orioledb
binds at `orioledb.c:1284` → `orioledb_recovery_stops_before_hook`
(recovery.c:1378). PG vanilla `recoveryStopsBefore` only understands
`RM_XACT_ID` as PITR stop boundaries; this hook is invoked *only* for
`RmgrIdIsCustom(xl_rmid)` records (xlogrecovery.c:2648). OrioleDB parses
its container header (`WAL_CONTAINER_HAS_XACT_INFO`) and returns true if
`xactTime` crosses `recoveryTargetTime`.

##### `RecoveryTargetReachedHook` — [PATCH17_6] (commit `e11d3deedc`)

Decl `xlogrecovery.h:113`; called at `xlogrecovery.c:1867-1876`. orioledb
does not currently register this — listed for completeness as the
symmetric "after stop" hook with `recoveryStopAfter/recordPtr/recordEndPtr`
metadata.

##### `xact_redo_hook` — [PATCH17_6] (commits `c13889ae21` + `2e2347e9db`)

Decl `xact.h:543-544`; called at `xact.c:6126-6127` (commit) and
`6280-6281` (abort); orioledb binds at `orioledb.c:1273` →
`o_xact_redo_hook` (recovery.c:4139). The hook for **joint commits**: a
heap-and-orioledb transaction emits both a vanilla `XLOG_XACT_COMMIT` and
a `WAL_REC_JOINT_COMMIT`; redo of the former triggers this hook, which
matches the pending oxid on `joint_commit_list` and finishes the
orioledb-side undo.

##### `HandleStartupProcInterrupts_hook` and `base_init_startup_hook` — [PATCH17_6] (commit `f316acfe92`)

The interrupt hook (registered at `recovery.c:1570`) piggy-backs
worker-queue draining onto the startup process's interrupt poll. The
base-init hook (registered at `orioledb.c:1275-1276`; called from
`postinit.c:662-663`) fires from `BaseInit()` for every backend type; for
`MyBackendType == B_STARTUP` orioledb cleans up stale `*.tmp`/`*.map`
checkpoint temp files before recovery sees them (orioledb.c:1293-1310).

#### Checkpoint-side hooks (recovery-adjacent)

##### `CheckPoint_hook` — [PATCH17_6] (commit `c13889ae21` + ordering fix `7563680371`)

Decl `xlog.h:314-315`; called from `xlog.c:7548-7549` inside
`CreateCheckPoint` *after* `CheckPointBuffers`; orioledb binds at
`orioledb.c:1259` → `o_perform_checkpoint` (src/checkpoint/checkpoint.c).
Drives orioledb's CoW checkpoint (atomic page-pointer swap, undo
truncation, file-extent flush). Writes `controlReplayStartPtr` and
`controlToastConsistentPtr`, which `orioledb_redo` (recovery.c:1171-1199)
consumes on the next startup.

##### `after_checkpoint_cleanup_hook` — [PATCH17_6] (same commit)

Decl `xlog.h:317-320`; called at `xlog.c:6139-6140` (end-of-recovery) and
`xlog.c:7370-7371` (per-checkpoint); orioledb binds at `orioledb.c:1260`.
The end-of-recovery site is **the only PG → orioledb call between "WAL
replay finished" and "cluster opens for writes"** — orioledb flushes
post-recovery state and (if S3 mode is on) signals replay caught up.

#### Transaction-side hooks

##### `get_xidless_commit_lsn_hook` — [PATCH17_6] (commit `66616e11b1`)

Decl `xact.h:160-162`; called at `xact.c:1403-1405` (xidless branch of
`CommitTransaction` — orioledb-only transactions without a top heap xid
that participated in logical replication). Orioledb binds at
`orioledb.c:1257` → `orioledb_get_xidless_commit_lsn` (undo.c:238).

##### `getRunningTransactionsExtension` / `waitSnapshotHook` — [PATCH17_6] (commit `8aae2edc37`)

Decls `standby.h:105` / `snapbuild.h`; called at `procarray.c:2884-2886`
and `snapbuild.c:1627-1628`. Let orioledb attach its CSN-snapshot state to
the `xl_running_xacts` stream used by hot-standby and logical decoding.

##### `RegisterXactCallback(undo_xact_callback, NULL)` — [VANILLA] API

`xact.h`; called at `xact.c:2401-2402` inside `CommitTransaction`.
Orioledb registers at `orioledb.c:1255`. This is the load-bearing piece
for the commit-time SK-leak investigation: WAL emission (`wal_commit`,
`add_finish_wal_record`, the `WAL_REC_COMMIT` write) happens in
`XACT_EVENT_PRE_COMMIT`; undo cleanup happens in `XACT_EVENT_COMMIT`.
Impl in `src/transam/undo.c`.

#### General extensibility (not redo-time; consolidated)

Other [PATCH17_6] hooks orioledb registers in `_PG_init` but which are
not called from the redo loop: `AcceptInvalidationMessagesHook`
(inval.c:906-907), `CustomErrorCleanupHook` (elog.c:3789-3790),
`snapshot_register_hook` / `snapshot_deregister_hook` (snapmgr.c),
`IndexAMRoutineHook` (amapi.c:33-35), `skip_tree_height_hook`
(plancat.c:489), `database_size_hook`, `set_plain_rel_pathlist_hook`,
`VacuumHorizonHook` (heapam_visibility.c:132-134, procarray.c),
`pg_newlocale_from_collation_hook`. See Appendix A for the full
registration/call-site table and Appendix B for commit IDs.

#### A non-hook: PG's smgr / md.c is NOT modified by patches17_6 for orioledb

Grep of `src/backend/storage/smgr/` and `src/include/storage/smgr.h` finds
**zero** orioledb references. OrioleDB does not register a custom smgr; it
bypasses the buffer manager and manages its own files under
`orioledb_data/` and `orioledb_undo/` (`include/orioledb.h:107-108`). PG-side
`varatt.h` does carry `VARTAG_ORIOLEDB = 34` (varatt.h:109,122,309,330) for
toast-pointer-format recognition, but that's data-format extension, not smgr.
The closest smgr-adjacent change is the checkpointer patch
(`checkpointer.c:218-232`, comment line 67) which adds
`InitializeTimeouts`, `InitDeadLockChecking`, `RelationCacheInitialize`,
`InitCatalogCache`, `SharedInvalBackendInit` to `CheckpointerMain` so
orioledb's CoW-checkpoint code can use PG-level locks and the catalog cache
from inside the checkpointer.

### H2 — Custom WAL container / overflow record types

OrioleDB does **not** use PG's main/FSM/VM-fork smgr overflow record types.
All orioledb WAL is a single `xl_rmid=129`, `xl_info=ORIOLEDB_XLOG_CONTAINER=0x00`
record carrying an opaque payload of inner `WAL_REC_*` sub-records (Topic 4
covers wire format). Container header (in record data, not PG's
`XLogRecord` header): `wal_version` (uint16) =
`ORIOLEDB_WAL_VERSION=17` (wal.h:31); `flags` (uint8) =
`WAL_CONTAINER_HAS_XACT_INFO (1<<0)` / `WAL_CONTAINER_HAS_ORIGIN_INFO (1<<1)`
(wal.h:19-20); optional `WALRecXactInfo { xactTime, xid }` (needed for PITR
stop-time); optional `WALRecOriginInfo { origin_id, origin_lsn }` (logical
origin tracking); then concatenated inner records walked by the X-macro
`ORIOLE_WAL_RECORDS` (wal_record.h:42-60). When an inner buffer fills up
mid-transaction, orioledb emits a fresh PG-level record with a fresh
container; the next transaction's `WAL_REC_XID` re-establishes the active
oxid (`flush_local_wal_if_needed` at wal.c:830-862).

### H2 — Shared-memory startup wiring

Sequence at process startup (from `shared_preload_libraries='orioledb.so'`):

1. Postmaster's `process_shared_preload_libraries()` ([VANILLA]) sets
   `process_shared_preload_libraries_in_progress = true`, `dlopen`s
   `orioledb.so`, and calls its `_PG_init` symbol.
2. `_PG_init` (orioledb.c:462) early-outs if not preloading (line 468).
3. Declares GUCs, opens `orioledb_data/` and `orioledb_undo/` (lines 471-473).
4. Saves previous hook-chain pointers, installs its own hooks into the
   global function-pointer slots (lines 1244-1290) — most are [PATCH17_6].
5. Registers bgworkers via vanilla `RegisterBackgroundWorker`:
   `register_bgwriter` × N (workers/bgwriter.c:46);
   `register_s3worker` × N if `orioledb_s3_mode` (s3/worker.c:114);
   `register_rewind_worker` if `enable_rewind` (rewind/rewind.c:973).
6. `RegisterCustomRmgr(ORIOLEDB_RMGR_ID, &rmgr)` (line 1262) — plugs
   orioledb into the redo loop.
7. `RegisterXactCallback(undo_xact_callback, NULL)` (line 1255).
8. Catalog cache callbacks via `CacheRegisterUsercacheCallback` (line 1258).

When the postmaster forks any backend (including the startup process),
vanilla `BaseInit()` calls `base_init_startup_hook` (postinit.c:662-663)
and the `shmem_startup_hook` chain runs — orioledb's link is
`orioledb_shmem_startup` (orioledb.c:1454).

#### Recovery worker pool startup

`o_recovery_start_hook` (the rmgr's `rm_startup` slot) at recovery.c:1025
is where the recovery worker pool comes into existence:
`recovery_worker_register` (worker.c:165) builds a `BackgroundWorker`
struct with `bgw_function_name = "recovery_worker_main"` and calls vanilla
`RegisterDynamicBackgroundWorker`; each worker attaches to its shm queue
(`shm_mq_attach`) and the startup process dispatches modify ops by table-OID
hash via `worker_send_msg`. **Recovery workers are bgworkers, started
lazily from inside the rmgr startup callback**, unlike the bgwriter workers
(registered in `_PG_init`, started at postmaster start). Recovery workers
shut down when redo completes.

### H2 — WAL replay dispatch: the full call graph

End-to-end, from PG's startup process calling `PerformWalRecovery()` down to
orioledb's per-record handler:

```
PerformWalRecovery                              [VANILLA]   xlogrecovery.c:1662
  RmgrStartup                                   [VANILLA]   rmgr.c:58
    rm_startup (rmid=129)  → o_recovery_start_hook          recovery.c:1025
                              ├ RegisterDynamicBackgroundWorker × N         (worker.c:165)
                              └ recovery_init(-1)                           (recovery.c:1476)

  MAIN REDO LOOP                                [VANILLA]   xlogrecovery.c:1760-1843
    ApplyWalRecord                              [VANILLA]   xlogrecovery.c:1937
      GetRmgr(xl_rmid).rm_redo                              xlogrecovery.c:2020
        ├ rmid=RM_XACT_ID  → xact_redo_commit/abort         xact.c:6117 / 6275
        │                      └ xact_redo_hook             xact.c:6126 / 6280   [PATCH17_6]
        │                         └ o_xact_redo_hook        recovery.c:4139      [ORIOLEDB]
        │
        └ rmid=129         → orioledb_redo                  recovery.c:1106      [ORIOLEDB]
                              ├ if past controlToastConsistentPtr:
                              │   workers_synchronize + apply_pending_sk_fixups
                              │   workers_notify_toast_consistent
                              └ replay_container                                 (recovery.c:4103)
                                  └ replay_on_record                             (recovery.c:3678)
                                      switch (rec->type):
                                        WAL_REC_XID, _COMMIT, _ROLLBACK,
                                        _JOINT_COMMIT, _RELATION,
                                        _INSERT/UPDATE/DELETE/REINSERT,
                                        _O_TABLES_META_LOCK/_UNLOCK,
                                        _TRUNCATE, _SAVEPOINT,
                                        _ROLLBACK_TO_SAVEPOINT, _BRIDGE_ERASE
                                        — dispatches via apply_modify_record /
                                          spread_idx_modify (to recovery workers)

  if reachedRecoveryTarget:
    RecoveryTargetReachedHook                              [PATCH17_6, unused by orioledb]
    if SHUTDOWN action: RedoShutdownHook                   [PATCH17_6]
                          └ o_recovery_shutdown_hook        (orioledb.c:381)
                              └ o_recovery_finish_hook(false)

  RmgrCleanup                                  [VANILLA]   rmgr.c:74
    rm_cleanup (rmid=129)  → o_recovery_cleanup             (orioledb.c:387)
                              └ o_recovery_finish_hook(true) (recovery.c:1209)
                                  ├ workers_send_finish, worker_wait_shutdown × N
                                  └ recovery_finish(-1)
```

### H2 — End-of-recovery handshake

After `PerformWalRecovery()` returns, vanilla `StartupXLOG` continues:
`PerformRecoveryXLogAction()` writes `XLOG_END_OF_RECOVERY` or a
shutdown-checkpoint, then `CleanupAfterArchiveRecovery` runs (xlog.c:6120-6131).
`xlog.c:6139` then calls **`after_checkpoint_cleanup_hook(EndOfLog, 0)`** —
the *one* PG → orioledb call between "WAL replay finished" and "cluster
opens for writes" (`o_after_checkpoint_cleanup_hook` in
`src/checkpoint/checkpoint.c` handles S3 reconciliation and on-disk sync).
Finally `ControlFile->state = DB_IN_PRODUCTION` at xlog.c:6157-6158.

OrioleDB's *internal* end-of-recovery work (draining post-toast-consistent
worker queues, applying remaining `joint_commit_list`, flushing undo
positions) happens earlier, inside `rm_cleanup` / `o_recovery_finish_hook`.
S3-mode reconciliation also runs early: `s3_check_control` at
`orioledb.c:1213` is invoked from `_PG_init` before recovery starts; S3
bgworkers (registered via `register_s3worker`) handle uploads asynchronously
once the cluster is open.

### H2 — ASCII sequence diagram: PG postmaster crash → recovery → orioledb hooks

```
TIME ────────────────────────────────────────────────────────────────────►

 T0  postmaster sends SIGQUIT to all backends [VANILLA postmaster.c quickdie()].
     orioledb backends die WITHOUT finishing undo cleanup.
       │
       ▼
 T1  postmaster restarts (restart_after_crash=on), forks startup process.   [VANILLA]
       │
       ▼
 T2  BaseInit() → base_init_startup_hook                                    [PATCH17_6]
       └─ o_base_init_startup_hook  (orioledb.c:1293)                       [ORIOLEDB]
            → recovery_cleanup_old_files (stale .tmp / .map removal)
       │
       ▼
 T3  shmem_startup_hook chain                                               [VANILLA+chain]
       └─ orioledb_shmem_startup  (orioledb.c:1454)                         [ORIOLEDB]
            → ShmemInitStruct("orioledb_enigne", ...) + LWLock tranches
       │
       ▼
 T4  InitWalRecovery → PerformWalRecovery → RmgrStartup                     [VANILLA]
       └─ RmgrTable[129].rm_startup = o_recovery_start_hook                 [ORIOLEDB]
            (recovery.c:1025)
            → RegisterDynamicBackgroundWorker × recovery_pool_size_guc
            → recovery_init(-1) (xid hash, syscache hooks, read_xids)
            → recovery_worker_main per worker  (worker.c:200)
       │
       ▼
 T5  main redo loop:  do { ApplyWalRecord(...); } while (record);           [VANILLA]
       per record:
         xl_rmid==RM_XACT_ID → xact_redo_commit/abort → xact_redo_hook      [PATCH17_6]
                                  → o_xact_redo_hook (joint commits)        [ORIOLEDB]
         xl_rmid==129        → orioledb_redo  (recovery.c:1106)             [ORIOLEDB]
                                  → replay_container → replay_on_record
                                  → workers apply INSERT/UPDATE/DELETE in parallel
       │
       ▼
 T6  redo loop ends → RmgrCleanup                                           [VANILLA]
       └─ RmgrTable[129].rm_cleanup = o_recovery_cleanup                    [ORIOLEDB]
            → o_recovery_finish_hook(true)  (recovery.c:1209)
              workers_send_finish + worker_wait_shutdown × N
              recovery_finish(-1) (flush undo retain locations)
       │
       ▼
 T7  PG writes XLOG_END_OF_RECOVERY / shutdown checkpoint                   [VANILLA]
       └─ after_checkpoint_cleanup_hook(EndOfLog, 0)                        [PATCH17_6]
            → o_after_checkpoint_cleanup_hook  (checkpoint.c)               [ORIOLEDB]
              post-recovery file sync, S3 reconciliation start
       │
       ▼
 T8  ControlFile->state = DB_IN_PRODUCTION → cluster opens for writes.      [VANILLA]
```

The hooks that fire are, in order:
`base_init_startup_hook` → `shmem_startup_hook` → `rm_startup` →
(per-XACT record) `xact_redo_hook` + (per-orioledb record) `rm_redo` →
`rm_cleanup` → `after_checkpoint_cleanup_hook`. Six bridge points total.

### H2 — Summary

- **Single redo entry point**: `rm_redo` at `xlogrecovery.c:2020`. Everything
  else is (a) lifecycle hooks ([VANILLA] `rm_startup`/`rm_cleanup`; [PATCH17_6]
  `RedoShutdownHook`, `xact_redo_hook`, `GetReplayXlogPtrHook`,
  `RecoveryStopsBeforeHook`, `RecoveryTargetReachedHook`), (b) the
  end-of-recovery handshake `after_checkpoint_cleanup_hook`, or (c)
  general-purpose hooks (`base_init_startup_hook`, etc.) that also fire in
  the startup process.
- **TableAM is producer-only** — it generates WAL via `o_wal_insert/...`,
  but is not consulted on the replay side.
- **No smgr modifications** in patches17_6 for orioledb — orioledb manages
  its own storage outside the `RelFileLocator` namespace.
- **No custom WAL overflow record types** — orioledb uses one
  `xl_rmid=129`/`xl_info=0x00` record carrying a self-describing container.
- **rmgr ID `129` is globally reserved**; orioledb *must* be in
  `shared_preload_libraries` to recover a cluster containing any orioledb
  tables (otherwise `RmgrNotFound` ereport at first orioledb record).

### Appendix A — The full hooks table (registration site → call site)

| Hook                                  | Registered at (orioledb.c)  | Vanilla call site                                                       | Category   |
|---------------------------------------|-----------------------------|--------------------------------------------------------------------------|------------|
| `RegisterCustomRmgr(129, &rmgr)`      | orioledb.c:1262             | xlogrecovery.c:2020 (`rm_redo`), rmgr.c:65/81 (`rm_startup`/`_cleanup`) | [VANILLA]  |
| `RedoShutdownHook`                    | orioledb.c:1263             | xlogrecovery.c:1886-1887                                                | [PATCH17_6]|
| `GetReplayXlogPtrHook`                | orioledb.c:1280             | xlogrecovery.c:4632-4633                                                | [PATCH17_6]|
| `RecoveryStopsBeforeHook`             | orioledb.c:1284             | xlogrecovery.c:2647-2666                                                | [PATCH17_6]|
| `xact_redo_hook`                      | orioledb.c:1273             | xact.c:6126-6127 (commit), 6280-6281 (abort)                            | [PATCH17_6]|
| `get_xidless_commit_lsn_hook`         | orioledb.c:1257             | xact.c:1403-1405                                                        | [PATCH17_6]|
| `CheckPoint_hook`                     | orioledb.c:1259             | xlog.c:7548-7549                                                        | [PATCH17_6]|
| `after_checkpoint_cleanup_hook`       | orioledb.c:1260             | xlog.c:6139-6140 (end-of-recovery), 7370-7371 (per-ckpt)                | [PATCH17_6]|
| `AcceptInvalidationMessagesHook`      | orioledb.c:1252             | inval.c:906-907                                                         | [PATCH17_6]|
| `CustomErrorCleanupHook`              | orioledb.c:1265             | elog.c:3789-3790                                                        | [PATCH17_6]|
| `snapshot_register/deregister_hook`   | orioledb.c:1266-1267        | snapmgr.c (multiple sites)                                              | [PATCH17_6]|
| `IndexAMRoutineHook`                  | orioledb.c:1277             | amapi.c:33-35                                                           | [PATCH17_6]|
| `getRunningTransactionsExtension`     | orioledb.c:1278             | procarray.c:2884-2886                                                   | [PATCH17_6]|
| `waitSnapshotHook`                    | orioledb.c:1279             | snapbuild.c:1627-1628                                                   | [PATCH17_6]|
| `skip_tree_height_hook`               | orioledb.c:1271-1272        | plancat.c:489                                                           | [PATCH17_6]|
| `VacuumHorizonHook` (if rewind)       | orioledb.c:1287             | heapam_visibility.c:132-134, procarray.c:2032-2034                      | [PATCH17_6]|
| `pg_newlocale_from_collation_hook`    | orioledb.c:1274             | (patch-added in `pg_locale.c`)                                          | [PATCH17_6]|
| `base_init_startup_hook`              | orioledb.c:1275-1276        | postinit.c:662-663                                                      | [PATCH17_6]|
| `HandleStartupProcInterrupts_hook`    | recovery.c:1570             | (patch-added in startup.c)                                              | [PATCH17_6]|
| `RegisterXactCallback(undo_xact_cb)`  | orioledb.c:1255             | xact.c:2401-2402                                                        | [VANILLA]  |
| `CacheRegisterUsercacheCallback`      | orioledb.c:1258             | (patch-added)                                                           | [PATCH17_6]|

### Appendix B — Patches17_6 commits this report depends on

Filtered from `git log --oneline patches17_6 ^d7ec59a63d` (above PG 17.0).
Recovery-relevant:

- `c13889ae21` Recovery and checkpointer hooks (RedoShutdownHook, CheckPoint_hook, after_checkpoint_cleanup_hook, xact_redo_hook)
- `0a0631719b` GetReplayXlogPtrHook
- `85db5468bb` RecoveryStopsHook for custom WAL records
- `e11d3deedc` Recovery target reached hook with stop-boundary metadata
- `2e2347e9db` Call xact_redo_hook on replaying transactions
- `66616e11b1` Replication origin advance for xid-less commits
- `7563680371` Move CheckPoint_hook() call after CheckPointBuffers() (ordering fix)
- `8aae2edc37` Rework handling of running transactions by extensions

Startup / lifecycle / misc:
- `f316acfe92` base_init_startup_hook + HandleStartupProcInterrupts_hook
- `21d97b84ce` AcceptInvalidationMessagesHook
- `9e8e7d2259` Hook for custom error cleanup
- `a19e20cb7e` Allow locks in checkpointer (InitDeadLockChecking patch)
- `118de6f329` Don't cancel recovery processes because of deadlocks
- `ae09f7059c` Snapshot extension and hooks; `cb493eb6df` SnapshotData split
- `188c810728` pg_newlocale_from_collation_hook
- `6f82958259` database_size hook; `6b6eb10f5c` set_plain_rel_pathlist_hook
- `803fc3febc` added injection points (crash-harness dependency)

TableAM / IndexAM (producer-side):
- `b9a4b5d93f` Improvements to TableAM API
- `15967dbe43` tuple_complete_modification TableAM callback
- `7d93efcb98` Extend tableam->relation_size()
- `6a6e9ffa7e` Hook to override index AM routine
- `db45c025b1` Methods for index update and delete
- `333f7e3fd3` Make index insert compatible with outside callers

---

## 3. OrioleDB recovery internals — PK, SK, undo, CSN

*(Source: `recovery_report_topic3_orioledb_deepdive.md`; original title: "Topic 3: OrioleDB Internal Recovery Deep Dive")*
This topic picks up where Topic 2 leaves off: PostgreSQL's startup process has
opened the WAL stream, identified an `RM_ORIOLEDB_ID` record, and dispatched it
to the orioledb resource-manager handler `orioledb_redo`. From that call onward
the heap-side machinery is irrelevant — every byte of state that has to land in
a B-tree leaf, an undo record, an oxid/CSN map entry, or a TOAST tree, is the
responsibility of orioledb's own code under `src/recovery/`, `src/btree/`,
`src/transam/`, and `src/checkpoint/`.

This deep dive maps that code path: the on-disk structures redo must mutate,
the row-level WAL record types it consumes, how a single UPDATE record fans out
into one PK mutation plus N SK deltas, how page LSNs decide whether a record
is a no-op, how oxid/CSN/undo state is rebuilt, how parallel recovery workers
shard work by oxid hash, and how the consistent point is reached for both the
main trees and TOAST.

### 1. On-disk model recap (what redo is mutating)

Before tracing the redo path it helps to fix the storage objects redo touches.
OrioleDB has no PostgreSQL heap; every relation is a set of B-trees rooted in
orioledb's own catalog and described by `OTableDescr` /`OIndexDescr`.

The trees a single user table can have, in dependency order:

- **PK tree** (`oIndexPrimary`): leaf tuples are the row payload, indexed by
  the user's PRIMARY KEY (or a synthetic ctid if no PK was declared).
  This tree IS the table. Any seqscan of the table is a walk of this
  B-tree.
- **SK trees** (`oIndexRegular` / `oIndexUnique`): leaf tuples contain the
  indexed columns followed by the PK key. The PK key is the row's
  "location"; there is no `ctid` indirection layer.
- **TOAST tree** (`oIndexToast`): rows whose values exceeded the inline
  threshold are sliced into chunks under a synthetic key. The TOAST
  tree is loaded into the standard PG TOAST mechanism via orioledb's
  wrappers.
- **Bridge tree** (`oIndexBridge`): present when the table opted into
  `ctid` emulation; it maps emulated `ctid` -> PK key for callers that
  need a stable item pointer (e.g. SP-GiST). Bridge trees consume
  WAL_REC_BRIDGE_ERASE records and have their own per-bridge `recovery_rec_*`
  callback path in `src/recovery/recovery.c:2235` (`replay_erase_bridge_item`).

Aside from the per-table trees, recovery also touches a small fleet of
**system trees** (`IS_SYS_TREE_OIDS()` in `src/recovery/recovery.c:2168`).
These are persistent btrees under fixed pseudo-OIDs (`SYS_TREES_O_TABLES`,
`SYS_TREES_O_INDICES`, typecache subtrees, etc.). They share the redo
machinery but are *always* applied by the leader, never sharded onto
workers — see `apply_sys_tree_modify_record` at
`src/recovery/recovery.c:4662`. The redo leader holds the OTablesMetaLock
across the records that mutate them, just as the primary does at runtime.

Per-page header on disk is `OrioleDBPageHeader` (defined in
`include/orioledb.h:356`), wrapping a transient `OrioleDBOndiskPageHeader` of
the same size. Notably:

```c
typedef struct {
    pg_atomic_uint64 state;
    uint32           pageChangeCount;
    uint32           checkpointNum;       /* <-- crucial for redo */
} OrioleDBPageHeader;
```

**There is no per-page `lsn` field stored on disk.** This is critical: unlike
the heap, orioledb does **not** carry a `pd_lsn`-equivalent in its page
header. The redo loop therefore cannot ask "has this page already absorbed
this WAL record?" by simple LSN comparison. Instead, idempotence is enforced
*through the row-level identity of the change itself*: the modify path always
goes through `o_btree_modify()` with a tuple key, and the
`recovery_*_callback`s under `src/recovery/recovery.c:2015-2156` use
`xactInfo` and `deleted` flags to decide whether the same change is already
present. The `checkpointNum` field above is the *checkpoint generation* the
page belongs to — used by I/O and CoW, not by redo skip logic. See
`src/orioledb.c:1396` and `src/utils/ucm.c:251` for the few sites that
re-stamp it.

Page-level resilience to a re-applied record is therefore a property of the
**tuple model**, not of an `lsn>=record_lsn` test. The per-tree
`lastCheckpointLSN` (Section 6) controls *whether the tree is replayed at
all*, not which individual records skip.

Underpinning all trees:

- **Per-tree data file** (`<prefix>/<datoid>_<reloid>.<chkpnum>`), produced by
  the CoW checkpointer in `src/checkpoint/checkpoint.c`. Once a tree finishes
  checkpoint N, its file is closed at version N+1 (or freed if obsoleted).
- **Free space map** (`SeqBufDescShared freeBuf` inside `BTreeMetaPage`,
  `include/btree/page_contents.h:24-64`) — a per-tree append-only journal of
  free extents, walked by writers and rebuilt on recovery from the saved
  sequence buffer.
- **Undo log** (`src/transam/undo.c`) — an in-memory ring of undo records
  keyed by `UndoLocation`; the per-row tuple header's `undoLocation` field
  (`BTreeLeafTuphdr.undoLocation`, `include/btree/page_contents.h:283`)
  is the back-pointer into it. Section 7 details how the undo log is
  re-populated during redo.
- **xidBuffer** (`src/transam/xidbuf.c`, opaque to redo logic) — the on-disk
  CSN map for committed/aborted oxids. Recovery rebuilds it lazily by
  observing WAL_REC_XID/COMMIT/ROLLBACK records and replaying them through
  `current_oxid_*` helpers — Section 7.
- **Checkpoint metadata files** under `ORIOLEDB_DATA_DIR` — `chkp_<n>.xid`,
  `chkp_<n>.bmp`, `chkp_<n>.tmp`, `chkp_<n>.meta` plus per-tree segments.
  Recovery loads `chkp_<n>.xid` via `read_xids()` at
  `src/recovery/recovery.c:820` to seed any oxids that committed before the
  last completed checkpoint but whose WAL stream may still mention them.

### 2. CoW checkpoint architecture and its consequences for redo

#### 2.1 What a checkpoint actually writes

OrioleDB's checkpoints are **copy-on-write**: dirty pages are *not* overwritten
in place. The checkpointer (`o_perform_checkpoint`,
`src/checkpoint/checkpoint.c:1305`) walks every tree, snapshots each dirty
page's contents into a *new* file segment for the next checkpoint generation
(`<file>.<chkpNum+1>` or, internally, an extent inside the per-tree segment),
then **atomically rewires the downlink in the parent page** to point at the
new on-disk extent (`MAKE_ON_DISK_DOWNLINK`,
`include/btree/page_contents.h:301`). The previous-generation file is held
until the new control file is fsynced (Section 2.3), then deleted by
`recovery_cleanup_old_files()` (`src/orioledb.c:1300-1303`,
`src/recovery/recovery.c:recovery_cleanup_old_files`).

Two consequences:

1. **No torn pages.** After a crash the old extent is fully intact because it
   was never overwritten; the new extent is either fully written or it never
   existed (its downlink was never published). PG-style "full page image"
   records (FPI) are therefore unnecessary and orioledb's WAL contains *only*
   row-level records (Section 3).
2. **No per-page LSN.** Because each page extent on disk belongs to exactly
   one checkpoint generation, recovery decides "do I have to replay onto this
   tree?" at *tree granularity*, by checking when this tree's pages were last
   stable — not by per-page LSN. Section 6 details this.

#### 2.2 The control file as the recovery anchor

After the CoW phase finishes for every tree, the checkpointer fsyncs the
new-generation tree segments, fsyncs the undo log range, fsyncs the xidmap,
and only *then* writes the new `CheckpointControl` to
`orioledb_data/control` (`src/checkpoint/checkpoint.c:1474-1517`). The
control file write is itself wrapped (`write_checkpoint_control` in
`src/checkpoint/control.c`) so a torn write is detectable.

On startup, `checkpoint_shmem_init` reads the previous-good control file
(`src/checkpoint/checkpoint.c:325-368`) and rehydrates
`checkpoint_state->control{Replay,SysTrees,ToastConsistent}Ptr`, the per-undo
log positions, and the xid water-marks. From this point everything redo does
is anchored against the LSN positions in this control file.

#### 2.3 The three checkpoint-control consistent points

The control file (`CheckpointControl`, `include/checkpoint/control.h:42-63`)
publishes three WAL positions. These are the **only** consistent points
recovery has to anchor on:

| Field             | Meaning                                                              | Set at                                                                |
|-------------------|----------------------------------------------------------------------|-----------------------------------------------------------------------|
| `replayStartPtr`  | LSN of WAL at the moment checkpoint snapshotted the sys/table state. | `src/checkpoint/checkpoint.c:1392` (under oTablesMetaLock + oSysTreesLock). |
| `sysTreesStartPtr`| LSN at which checkpointer finished checkpointing system trees and unlocked oTablesMetaLock. | `src/checkpoint/checkpoint.c:1407`. |
| `toastConsistentPtr` | LSN at which the checkpointer crossed from PK trees to SK trees. | `src/checkpoint/checkpoint.c:1428` / `src/checkpoint/checkpoint.c:5114`. |

The control file copies of these are loaded back into
`checkpoint_state->controlReplayStartPtr` etc. at startup
(`src/checkpoint/checkpoint.c:328-332`), and the redo loop reads them
directly (`src/recovery/recovery.c:1171-1199`).

Why three? Because the checkpointer crosses three internally distinct
phases:

1. **Sys-tree phase.** While `oTablesMetaLock` is held, system trees are
   walked and written. `replayStartPtr` = "WAL position at which the
   checkpointer began this phase". The sys-tree image on disk is
   consistent up to `sysTreesStartPtr`.
2. **PK-tree phase.** Lock released, every user table's PK tree is
   checkpointed in turn. Each tree's on-disk image absorbs its own
   in-memory state but the in-progress oxids' UNDO log still references
   tuples in older positions, so the *next* recovery has to apply WAL
   records from `replayStartPtr` forward to those PK trees too.
3. **SK-tree phase.** When the checkpointer hits its first SK
   (`td->type >= oIndexUnique`), it captures `toastConsistentPtr` and
   simultaneously snapshots every backend's PK-applied / SK-pending
   marker (`checkpoint_write_pending_sk_fixups()`,
   `src/checkpoint/checkpoint.c:976`). The PendingSkFixup records
   written to the xids file tell the *next* recovery: "between
   `replayStartPtr` and `toastConsistentPtr` there were N in-progress
   PK inserts whose SK side may not have been emitted to the on-disk SK
   tree — when SK replay reaches `toastConsistentPtr` you must apply
   these fix-ups."

#### 2.4 The recovery-side view

In `orioledb_redo` (`src/recovery/recovery.c:1106`) these three fields
control the dispatch:

```c
if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr
    && !toast_consistent)
{
    if (!recovery_single)
        workers_synchronize(record->ReadRecPtr, true);  // drain PK redo
    apply_pending_sk_fixups();                          // emit missing SK rows
    toast_consistent = true;
    if (!recovery_single)
        workers_notify_toast_consistent();              // tell workers
}

if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
{
    if (!replay_container(msg_start, msg_start + msg_len, ...))
        elog(ERROR, "...");
}
```

So:
- Records with `ReadRecPtr < controlReplayStartPtr` are **skipped entirely**
  — the on-disk image already absorbed them.
- Records between `controlReplayStartPtr` and `controlToastConsistentPtr` go
  through `replay_container` and reach PK trees, but the workers
  short-circuit SK trees in `apply_modify_record`
  (`src/recovery/worker.c:683`):

  ```c
  if (descr && toast_consistent)
      apply_tbl_modify_record(descr, ...);    // PK + all SKs
  else
      apply_btree_modify_record(&id->desc, ...);  // PK only
  ```

  i.e. before TOAST consistency, only the index identified in the
  immediately preceding `WAL_REC_RELATION` is touched — not the full SK
  fanout.
- At the boundary, workers drain (`workers_synchronize`), the leader walks
  the saved `PendingSkFixup` xids-file records and emits the missing SK
  rows (`apply_pending_sk_fixups()`,
  `src/recovery/recovery.c:600`), then flips `toast_consistent` and
  tells workers (`workers_notify_toast_consistent()`,
  `src/recovery/recovery.c:4619`).
- Records past `controlToastConsistentPtr` exercise the full PK + SK
  fanout in `apply_tbl_insert/update/delete` (`src/recovery/worker.c:818`).

For sys-tree mutations the leader applies them only when
`ctx->xlogRecPtr >= checkpoint_state->controlSysTreesStartPtr`
(`src/recovery/recovery.c:3941`).

### 3. WAL record taxonomy and the redo dispatch table

All orioledb records are emitted inside a single PG WAL container of resource
manager `RM_ORIOLEDB_ID`. The bytes that follow the container header are an
orioledb-private stream of records. The full set is enumerated by the X-macro
`ORIOLE_WAL_RECORDS(X)` at `include/recovery/wal_record.h:42`; the write-side
emitter lives in `src/recovery/wal.c`, the read/parse side in
`src/recovery/wal_reader.c`, and the redo-side dispatcher is
`replay_on_record()` at `src/recovery/recovery.c:3677`.

The record types in payload order:

| Record                          | Struct (`include/recovery/wal.h`)        | Payload                                                                                                             |
|---------------------------------|------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `WAL_REC_XID` (1)               | `WALRecXid` (line 67)                    | `oxid`, `logicalXid`, `heapXid`. Sets recovery's current oxid for subsequent records.                               |
| `WAL_REC_COMMIT` (2)            | `WALRecFinish` (line 159)                | `xmin`, `csn`. Closes current oxid as committed.                                                                    |
| `WAL_REC_ROLLBACK` (3)          | `WALRecFinish` (same)                    | `xmin`, `csn`. Closes current oxid as aborted.                                                                      |
| `WAL_REC_RELATION` (4)          | `WALRecRelation` (line 83)               | `treeType`, `oids` (datoid/reloid/relnode), `xmin`/`csn`/`cid`, `version`, `baseVersion`. Selects the target tree.  |
| `WAL_REC_INSERT/UPDATE/DELETE` (5/6/7) | `WALRecModify1` (line 115)        | `tupleFormatFlags`, `length`, tuple bytes. The PK or SK key/tuple. See payload notes below.                         |
| `WAL_REC_O_TABLES_META_LOCK` (8) | `WALRec` (line 62)                      | None. Acquires the meta lock at the leader.                                                                         |
| `WAL_REC_O_TABLES_META_UNLOCK` (9) | `WALRecOTablesUnlockMeta` (line 105)  | `oids`, `oldRelnode`. Releases meta lock and triggers table-shape changes.                                          |
| `WAL_REC_SAVEPOINT` (10)        | `WALRecSavepoint` (line 135)             | `parentSubid`, `logicalXid`, `parentLogicalXid`.                                                                    |
| `WAL_REC_ROLLBACK_TO_SAVEPOINT` (11) | `WALRecRollbackToSavepoint` (line 143) | `parentSubid`, `xmin`, `csn`.                                                                                  |
| `WAL_REC_JOINT_COMMIT` (12)     | `WALRecJointCommit` (line 151)           | `xid` (PG xid), `xmin`, `csn`. Defers final commit to `o_xact_redo_hook`.                                           |
| `WAL_REC_TRUNCATE` (13)         | `WALRecTruncate` (line 166)              | `oids` to truncate.                                                                                                 |
| `WAL_REC_BRIDGE_ERASE` (14)     | `WALRecBridgeErase` (line 174)           | `iptr` to erase from the bridge index.                                                                              |
| `WAL_REC_REINSERT` (15)         | `WALRecModify2` (line 124)               | Old tuple bytes + new tuple bytes. UPDATEs that change the PK get demoted to DELETE+INSERT on disk.                 |
| `WAL_REC_REPLAY_FEEDBACK` (16)  | `WALRec`                                 | None. Marks the tx as wanting synchronous-replication feedback.                                                     |
| `WAL_REC_SWITCH_LOGICAL_XID` (17) | `WALRecSwitchLogicalXid` (line 76)     | `topXid`, `subXid`. Logical-decoding xid binding. Ignored by redo.                                                  |
| `WAL_REC_RELREPLIDENT` (18)     | `WALRecRelReplident` (line 98)           | `relreplident`, `relreplident_ix_oid`. Currently no-op in redo (`src/recovery/recovery.c:3830`).                    |

#### 3.1 What the modify records carry

A `WAL_REC_INSERT`/`UPDATE`/`DELETE`/`REINSERT` record carries **only the
target tree's tuple bytes**, not a redo image of a page. The target tree was
selected by the most-recent `WAL_REC_RELATION` (the leader caches it in
`ctx->descr` / `ctx->indexDescr`, `src/recovery/recovery.c:3773-3828`).

- For INSERT, the payload is the new leaf tuple.
- For UPDATE, the payload is the new leaf tuple. (Old tuple is reconstructed
  by callbacks at redo time via lookup against the PK.)
- For DELETE, the payload is the **key** (`OKeyLength`), or the **full
  tuple** if `relreplident == REPLICA_IDENTITY_FULL`. See
  `recovery_rec_delete()` at `src/recovery/recovery.c:2314` for the writer's
  view and `replay_on_record` (case `WAL_REC_DELETE`,
  `src/recovery/recovery.c:4046-4072`) for the redo's view.
- For REINSERT (UPDATE that changed the PK), the payload is **both** tuples
  in a `WALRecModify2`; redo splits it into `DELETE` (old) + `INSERT` (new)
  via `apply_modify_record(... RecoveryMsgTypeDelete ...)` then
  `apply_modify_record(... RecoveryMsgTypeInsert ...)`
  (`src/recovery/recovery.c:4034-4035`).

#### 3.2 What modify records do NOT carry

Two omissions are load-bearing for the redo design:

1. **No per-record undo location.** The undo back-pointer in the
   `BTreeLeafTuphdr.undoLocation` field is re-derived during redo by
   *re-inserting* through `o_btree_modify()` — the same modify path used at
   runtime. The undo log is therefore re-populated as a side effect of redo
   applying each record (Section 7). The runtime `undoLocation` value would
   be meaningless to copy verbatim anyway: it is a physical offset into the
   producing process's undo ring, and a fresh recovery process has its own
   ring at different addresses. See §7.0 for the ring buffer / per-tuple
   back-pointer / per-transaction stack model that makes the re-derivation
   work.
2. **No SK deltas.** A WAL record targets one tree; the leader/worker that
   replays an UPDATE against the PK is the same one responsible for
   computing and applying the corresponding SK deltas (Section 5).

### 4. The redo loop in `orioledb_redo`

PostgreSQL's startup process dispatches each `RM_ORIOLEDB_ID` container by
calling `orioledb_redo(XLogReaderState *record)` at
`src/recovery/recovery.c:1106`. The function is small and reads top-down:

```
orioledb_redo:
    [bounds check]    record info == ORIOLEDB_XLOG_CONTAINER
    [PITR check]      record->ReadRecPtr vs orioledb.replay_until_lsn (line 1115)
    [TOAST boundary]  if record->ReadRecPtr >= controlToastConsistentPtr
                          && !toast_consistent:                      (line 1171)
                          workers_synchronize(...)
                          apply_pending_sk_fixups()
                          toast_consistent = true
                          workers_notify_toast_consistent()
    [replay]          if record->ReadRecPtr >= controlReplayStartPtr:    (line 1191)
                          replay_container(msg_start, msg_start+msg_len,
                                            recovery_single, ReadRecPtr, EndRecPtr)
    [crash check]     if unexpected_worker_detach: ERROR
```

`replay_container` (`src/recovery/recovery.c:4103`) wraps the orioledb
container in a `WalReaderState` (declared in `include/recovery/wal_reader.h`)
and walks records via `wal_parse_container`. Three callbacks are wired:

- `replay_check_version`: rejects WAL of newer-than-supported binary version
  (`src/recovery/recovery.c:3631`).
- `replay_on_container`: harvests the optional `WAL_CONTAINER_XACT_INFO`
  (PG xid) into `recoveryHeapTransactionId` for logical decoding seeding.
- `replay_on_record`: the main dispatcher. One huge `switch (rec->type)`
  ladder (`src/recovery/recovery.c:3677-4097`) covering every WAL record.

#### 4.1 Single-process vs parallel

The redo loop is single-threaded when `*recovery_single_process == true`,
parallel otherwise. The branch is taken at most leaves of the dispatcher
ladder:

```c
if (ctx->single) {
    recovery_switch_to_oxid(rec->oxid, -1);
    apply_modify_record(ctx->descr, ctx->indexDescr, type, tuple1.tuple);
} else {
    spread_idx_modify(&ctx->indexDescr->desc, type, tuple1.tuple);
}
```
(`src/recovery/recovery.c:4078-4086`)

`spread_idx_modify` (`src/recovery/recovery.c:4680`) hashes the tuple's
B-tree key and routes the message to one of the
`recovery_pool_size_guc` workers via `worker_send_modify`. Single-process
just runs `apply_modify_record` inline against the leader process. Section 8
expands on the parallel side.

#### 4.2 Ordering invariants the dispatcher enforces

`replay_on_record` keeps a `ReplayWalDescCtx` (`src/recovery/recovery.c:3663`)
across records inside one container. The dispatcher relies on the writer
having emitted records in this canonical order:

1. `WAL_REC_XID` once per oxid before any tuple records for it.
2. `WAL_REC_RELATION` once per (oids, tree-type) before any modify against
   that tree (resets `ctx->descr` / `ctx->indexDescr`).
3. Zero or more `WAL_REC_INSERT/UPDATE/DELETE/REINSERT` for the active
   (oxid, tree).
4. Eventually `WAL_REC_COMMIT` or `WAL_REC_ROLLBACK` (or `JOINT_COMMIT`).

The write side enforces this via the `local_oids`/`local_type` cache in
`src/recovery/wal.c:36-37`: `add_modify_wal_record_extended` checks
`ORelOidsIsEqual(local_oids, oids)` and emits a `WAL_REC_RELATION` whenever
it differs (`src/recovery/wal.c:181-198`). Crash-time, the recovery code
defends against a *truncated* container — every flush (`flush_local_wal`)
emits a self-contained prefix-bounded block.

### 5. From WAL record to btree mutation: `apply_modify_record` and the SK fanout

This is the crux of orioledb redo. A *single* WAL UPDATE record describing one
PK-tree mutation has to result in: one PK row updated, and N SK rows
re-keyed (when any SK-indexed columns changed). The fanout happens
**locally**, on the redo side, in `apply_tbl_*` helpers in
`src/recovery/worker.c`. It is not driven by additional WAL records.

#### 5.1 Top of the call graph

For a non-sys-tree modify, `replay_on_record` ends in either
`apply_modify_record` (single-process) or `spread_idx_modify` (parallel).
`spread_idx_modify` (`src/recovery/recovery.c:4680`) computes
`hash = o_btree_hash(desc, rec, ...)`, picks the worker, and forwards the
tuple via a shm_mq message. The worker, in
`recovery_worker_main`'s message loop (`src/recovery/worker.c:471`), calls
`apply_modify_record` itself.

`apply_modify_record` (`src/recovery/worker.c:670`) is two lines of logic:

```c
if (descr && toast_consistent)
    apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
else
    apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
```

(`src/recovery/worker.c:683`). The branch:

- **Before TOAST consistent**, or **for trees without a parent `OTableDescr`**
  (sys trees, bridge, isolated indexes): write *only* to the tree named in the
  WAL_REC_RELATION header. The reason: SK tuples may depend on TOASTed
  values whose chunks have not been replayed yet, so synthesising a SK row now
  could be wrong. The skipped SK side is captured at checkpoint time as
  `PendingSkFixup` records (Section 9) and re-emitted at the boundary.
- **After TOAST consistent**: walk all indices, computing the SK keys from
  the new tuple, and applying inserts/deletes to each.

#### 5.2 The PK + SK walk for an UPDATE

`apply_tbl_update` (`src/recovery/worker.c:1021`) handles UPDATEs. The key
loop is `for (i = 0; i < descr->nIndices; i++)`. Index 0 is the PK
(`isPrimary = i == PrimaryIndexNumber`). The pseudocode:

```
for each index i in descr->indices:
    if isPrimary:
        o_btree_modify(&PK->desc, BTreeOperationUpdate, new_tuple, ...,
                       modifyCallback = o_update_copy_callback,
                       arg = &tupCopy);
        // o_update_copy_callback writes the *old* tuple into tupCopy
        if modify_result != Updated:
            return;                  // nothing to undo on SK side
        if nIndices == 1:
            return;                  // no SK to update
        tts_orioledb_store_tuple(new_slot, new_tuple, ...);
        tts_orioledb_store_tuple(old_slot, tupCopy.tuple, ...);
    else:
        fill SK keyBound from new_slot -> new_key
        fill SK keyBound from old_slot -> old_key
        if cmp(new_key, old_key) != 0:
            if predicate_satisfied(old_slot):
                o_btree_modify(&SK->desc, Delete, old_key, ...);
            if predicate_satisfied(new_slot):
                build new_stup from new_slot
                o_btree_modify(&SK->desc, Insert, new_stup, ...);
```

Two critical details:

1. **The old PK tuple is captured by callback** during the PK modify.
   `o_update_copy_callback` runs *inside* `o_btree_modify` while it holds
   the PK leaf lock, snapshotting the pre-update tuple into `tupCopy`.
   The redo path then uses `tupCopy.tuple` to recompute the *old* SK key
   for the comparison `cmp(new_key, old_key)`.
2. **The new and old SK keys are compared.** If the SK key is unchanged
   (the UPDATE didn't touch any column the SK indexes), the SK row is
   left alone — no delete-then-insert churn. This is essential for
   replay performance and matches the runtime path.

#### 5.3 INSERT and DELETE fanout

INSERT (`src/recovery/worker.c:818`):

```
tts_orioledb_store_tuple(slot, new_tuple, ...);
o_btree_load_shmem(&PK->desc);
btree_ctid_update_if_needed(...);     // for ctid-PK tables
for each index i:
    if isPrimary:
        callbackInfo = recovery_insert_primary_callback;
        callbackInfo.postUndoRecorded = set_pending_sk_marker_from_descr;
        o_btree_modify(&PK->desc, Insert, new_tuple, ...);
    else:
        stuple = tts_orioledb_make_secondary_tuple(slot, SK, true);
        if !predicate_satisfied(SK, slot): continue;
        if o_btree_len(SK, stuple) > max: continue;   // truncated runtime
        o_btree_modify(&SK->desc, Insert, stuple, ...);
```

DELETE (`src/recovery/worker.c:941`):

```
for each index i:
    if isPrimary:
        callbackInfo.modifyCallback = o_delete_copy_callback;
        callbackInfo.arg = &tupCopy;
        o_btree_modify(&PK->desc, Delete, key, ...);
        if modify_result != Deleted: return;
        tts_orioledb_store_tuple(slot, tupCopy.tuple, ...);
    else:
        if !predicate_satisfied(SK, slot): continue;
        fill SK keyBound -> keyBound
        o_btree_modify(&SK->desc, Delete, ..., keyBound, ...);
```

#### 5.4 Why the "pending SK marker" matters at redo

The PK callback set `callbackInfo.postUndoRecorded` to
`set_pending_sk_marker_from_descr` (INSERT) or
`set_pending_sk_marker_from_tup_copy` (UPDATE/DELETE). On the primary, these
publish a per-backend `pendingSkUndoLoc` (in `ODBProcData`) for the window
between "PK undo record written" and "SK btree modifications complete".

At redo time the workers run the same callbacks, so `pendingSkUndoLoc` is
populated the same way. If the worker is interrupted between the PK insert
and the SK insert (e.g. recovery is shut down, or a parallel worker is
killed), the in-progress oxid is rolled back via the per-worker undo log,
exactly as a runtime crash would.

This mirrors the runtime PK->SK ordering exactly, which is why a *recovery*
SK desynchronization bug shows the same symptoms as a runtime one — and why
the active investigation under `test/t/crash/` instruments both paths
with the same `csn-trace` / `wal-trace` markers.

#### 5.5 The leader-only sys-tree path

System trees are never sharded. The leader applies them inline via
`apply_sys_tree_modify_record` (`src/recovery/recovery.c:4662`), which calls
`apply_btree_modify_record` directly. The leader holds the meta lock for the
duration. See Section 4 above for ordering — system tree records can only
appear when `ctx->xlogRecPtr >= controlSysTreesStartPtr`
(`src/recovery/recovery.c:3941`), because the checkpoint phase ordering
guarantees the on-disk sys-tree image is otherwise complete.

### 6. PK/SK page-LSN checks: how orioledb avoids double-applying records

This section's heading mirrors the original investigation's terminology, but
the answer here is *non-obvious and important to get right* because orioledb's
mechanism does **not** look like PostgreSQL's heap `pd_lsn`-vs-record-LSN
comparison.

#### 6.1 There is no per-page LSN

`OrioleDBPageHeader` (`include/orioledb.h:356`) carries `state`,
`pageChangeCount`, `checkpointNum` — and *no* `lsn` field. The page on disk
(`OrioleDBOndiskPageHeader`, `include/orioledb.h:367`) carries
`checkpointNum`, compression info, and version. Again, no LSN. The closest
the page comes to "version" information is the `checkpointNum`, which is the
generation in which the page was last *written*, not the WAL position it was
last *modified at*.

#### 6.2 The actual skip ladder, in three layers

Redo idempotence is enforced by a layered set of checks, not a single
per-page test:

##### Layer 1 — container-level skip (cheap, common)

In `orioledb_redo` (`src/recovery/recovery.c:1191`):

```c
if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
    replay_container(...);
```

If `ReadRecPtr` is below the last checkpoint's `replayStartPtr`, the entire
container is dropped. This is the dominant case: every record produced
*before* the last checkpoint completed its sys-tree phase is skipped at the
container level, because the on-disk sys-tree/PK pages already absorbed it
via the CoW write. No per-record or per-page logic runs.

##### Layer 2 — boundary skip for SK trees

Between `controlReplayStartPtr` and `controlToastConsistentPtr`, the modify
records *do* run, but `apply_modify_record` short-circuits to PK-only
(`src/recovery/worker.c:683`):

```c
if (descr && toast_consistent)
    apply_tbl_modify_record(descr, type, p, oxid, COMMITSEQNO_INPROGRESS);
else
    apply_btree_modify_record(&id->desc, type, p, oxid, COMMITSEQNO_INPROGRESS);
```

SKs are skipped because their on-disk image already absorbed the records
that came before `controlToastConsistentPtr` (CoW). They will only start
being touched when `toast_consistent` flips to true at the boundary.

**Important: each WAL record is dispatched to `apply_modify_record` exactly
once per redo pass.** The branch above is determined by *which side of the
boundary* the record sits on, not by the record itself. So a given record
hits `apply_btree_modify_record` *or* `apply_tbl_modify_record`, never both.
Across the whole WAL stream the PK is therefore mutated by records from
both regions — pre-boundary records via Layer-2 (PK only) and post-boundary
records via Layer-3 (PK + every SK in `descr->indices`) — but each
individual record contributes one PK apply, not two. The thing that *can*
make the same record hit the PK twice is a **redo restart** (recovery
itself crashing and re-replaying from the last checkpoint), which Layers 1
and 3 below make safe.

The PK records that ran in the Layer-2 short-circuit don't lose their SK
fanout — they get deferred to `apply_pending_sk_fixups()`, which runs in
one shot when `toast_consistent` flips (see §4 dispatcher and §5.4 for the
`pendingSkUndoLoc` mechanism). The active SK-leak investigation is about
whether that deferred fanout is correctly reconciled in every commit /
abort window — see §6.3.

##### Layer 3 — tuple-level idempotence in `o_btree_modify`

For records that *do* execute, idempotence is a property of the modify
machinery itself, not of a "has this page seen this LSN?" check. The recovery
callbacks (`src/recovery/recovery.c:2015-2156`) inspect the existing tuple's
`OTupleXactInfo` and `deleted` flags:

- `recovery_insert_primary_callback`: if a tuple exists for this PK with the
  same `oxid`, the callback returns "OK, already there" — no second insert.
- `recovery_delete_primary_callback`: if no tuple exists, returns "already
  deleted".
- `recovery_*_deleted_*_callback`: handles the case where a tuple was
  inserted-then-deleted in the same oxid.

This is why redo *can* re-execute a record without corruption: the modify
path computes the actual state difference. Inserting a tuple that's already
there is a no-op; deleting a tuple that's already gone is a no-op.

##### Layer 4 — page-write generation (CoW boundary, not redo)

For completeness: `write_page_to_disk` (`src/btree/io.c:1777-1797`) compares
the in-memory page's `o_header.checkpointNum` against the *target* checkpoint
number. If less, the page is "dirty for this checkpoint" and gets written to
a new on-disk location; if equal it's the same generation and either skipped
or overwritten in place (compressed pages have additional rules). This logic
is about **checkpoint write coordination**, not redo, but it's the same
field that recovery later reads to validate the page is from the expected
generation.

#### 6.3 The implication for the active SK-leak investigation

Because there is no per-page LSN, you cannot answer "did this SK page already
absorb this WAL record?" by looking at the page in isolation. The answer
depends on:

1. Where the record sits relative to `controlToastConsistentPtr`.
2. Whether the leader emitted a corresponding `PendingSkFixup` record at
   checkpoint time.
3. Whether the worker that owns the hashed PK key applied the fixup before
   the boundary flipped.

A bug that drops a single SK row therefore cannot be diagnosed by looking at
a page header. It has to be diagnosed by either (a) reconstructing the WAL
stream around `controlToastConsistentPtr` and confirming that a
`PendingSkFixup` should have been emitted and was, or (b) confirming that
the runtime PK->SK ordering (the `pendingSkUndoLoc` mechanism, Section 5.4)
correctly tracked the in-progress oxid through commit. The active
investigation focuses on the second path because the bug reproduces without
any crash near a checkpoint boundary.

#### 6.4 Visual: PK/SK record-vs-state ladder

```
WAL record  R at LSN L  hits orioledb_redo:
        |
        v
   L < controlReplayStartPtr ?
        |yes
        +---> SKIP entire container.  Done.
        |no
        v
   L < controlToastConsistentPtr  &&  !toast_consistent ?
        |yes
        +---> dispatch into replay_on_record:
        |       - sys-tree records: applied only if L >= controlSysTreesStartPtr
        |       - INSERT/UPDATE/DELETE: ONLY against the tree named in
        |         the most-recent WAL_REC_RELATION (no SK fanout).
        |         apply_btree_modify_record(&id->desc, ...).
        |no  (toast became consistent OR L past boundary)
        v
   At boundary: workers_synchronize() drains queues,
                apply_pending_sk_fixups() runs,
                toast_consistent <- true,
                workers_notify_toast_consistent().
        |
        v
   Now apply_modify_record() takes the
   `descr && toast_consistent` branch ->
   apply_tbl_*() which walks descr->indices.
        |
        v
   For each index i in descr->indices:
        if isPrimary: o_btree_modify(PK, ...) with copy-callback.
        else:         build SK key from new/old slot,
                      o_btree_modify(SK, ...) only if key changed.
        |
        v
   Inside o_btree_modify, the recovery_*_callback inspects existing
   tuple's xactInfo/deleted flags to make the operation idempotent.
```

### 7. Recovery transaction state: oxid, CSN, undo log

Redo has to rebuild three pieces of transaction state that runtime carries in
shared memory: the oxid -> CSN map, the per-oxid undo-stack locations, and
the xmin/retain-undo bookkeeping. None of these are stored *on disk* in a
form ready to be loaded; they are *rederived from the WAL stream* and from
the saved per-checkpoint xids file.

#### 7.0 The undo storage model (reference)

Before getting into the per-oxid state, a quick reference for *where* undo
records actually live in shared memory — the design that makes the
re-derivation in §3.2 work.

**Three global ring buffers, one per `UndoLogType`** (`include/orioledb.h:210-223`):

```c
typedef enum {
    UndoLogRegular = 0,            // user data, row-level
    UndoLogRegularPageLevel = 1,   // user data, page-level (merge/split)
    UndoLogSystem = 2,             // system trees (catalog)
    UndoLogsCount = 3
} UndoLogType;
```

`undo_metas` (`src/transam/undo.c:176,330`) is a shared-memory array of three
`UndoMeta` descriptors. There is **no per-tree and no per-tuple ring** —
every user btree (PK, every SK, every TOAST) sets `desc->undoType =
UndoLogRegular` (`src/tableam/tree.c:128`) and shares one ring. Sys-trees go
to `UndoLogSystem`; page-level structural undo (merges/splits) goes to
`UndoLogRegularPageLevel`.

**Data → undo linkage is per-tuple.** `BTreeLeafTuphdr.undoLocation`
(`include/btree/page_contents.h:283`, 62-bit field) is each leaf tuple's
back-pointer into its tree's ring.

**Two linked lists threaded through each ring.** Each
`BTreeModifyUndoStackItem` (`include/btree/undo.h:55-63`) carries an outer
`UndoStackItem header` whose `prev` points to the previous item in the same
transaction, AND an embedded `BTreeLeafTuphdr tuphdr` whose `undoLocation`
points to the previous version of the same row:

```c
typedef struct {
    UndoStackItem      header;     // .prev = prior item in OXID's stack
    BTreeOperationType action;
    ORelOids           oids;
    OInMemoryBlkno     blkno;
    uint32             pageChangeCount;
    BTreeLeafTuphdr    tuphdr;     // .undoLocation = prior version of THIS row
} BTreeModifyUndoStackItem;
```

`src/btree/undo.c:362` sets the per-row link at write time:

```c
if (curTupHdr) {
    item->tuphdr.undoLocation  = curTupHdr->undoLocation;  // copy prev-version pointer
    ...
}
add_new_undo_stack_item(desc->undoType, undoLocation);     // link into OXID stack
```

Practical consequences:

- **Per-tuple version walk** (snapshot visibility,
  `src/btree/undo.c:703-752,1611-1730`): follow `tuphdr.undoLocation`. Hops
  are O(1) physical offset dereferences; entries belonging to other rows
  sitting between two versions are silently skipped.
- **Per-transaction undo walk** (abort / commit / subxact rollback,
  `apply_undo_stack`, `on_commit_undo_stack`, `precommit_undo_stack`): follow
  `header.prev`. This intentionally mixes rows and trees of the same oxid —
  exactly what abort needs.
- **PK and SK of the same row share a ring but not an undo entry.** An
  UPDATE that doesn't change the SK key writes one new
  `BTreeModifyUndoStackItem` for the PK only; the SK leaf tuple keeps its
  old `undoLocation`. The shared ring is what makes
  `apply_undo_stack(UndoLogRegular, oxid, ...)` cover PK + every SK in one
  pass.
- **`UndoLocation` is a physical 62-bit byte offset** into the ring, not a
  logical id. Following a pointer is one dereference; the *target* of each
  pointer is chosen by chain semantics, not by spatial adjacency.

#### 7.1 The per-oxid state at the leader

The leader keeps a `recovery_xid_state_hash` (`src/recovery/recovery.c:1486`)
mapping `OXid -> RecoveryXidState` (defined at
`src/recovery/recovery.c:99-135`). A `RecoveryXidState` carries:

- `xid`: PG xid if this oxid joined a heap commit (joint commit, line 103).
- `retain_locs[UndoLogsCount]`: per-undo-log lowest location that this oxid
  still references (the undo retain pointer; line 107).
- `undo_stacks[UndoLogsCount]`: per-undo-log `UndoStackLocations` tuple
  (branch / subxact / on-commit; line 108). This is the same struct runtime
  backends carry in `cur_undo_locations`.
- `checkpoint_undo_stacks`: dlist of `CheckpointUndoStack` entries loaded
  from the previous-checkpoint xids file for this oxid; replayed at finish
  (line 109, struct at line 142).
- `csn`: COMMITSEQNO_INPROGRESS while in-flight, the real CSN after commit
  (line 110).
- `ptr`: WAL LSN of the commit/abort record (line 111).
- `in_finished_list`, `in_joint_commit_list`, `in_retain_undo_heaps[]`: list
  / heap membership flags so cleanup knows what to detach.
- `systree_modified`, `invalidate_typcache`, `o_tables_meta_locked`,
  `checkpoint_xid`, `wal_xid`: bookkeeping bits read by the dispatcher.
- `used_by[recovery_pool_size_guc + recovery_idx_pool_size_guc]`: which
  workers have seen messages for this oxid; used to coordinate finish.

#### 7.2 The lifecycle of an oxid during redo

```
WAL_REC_XID(oxid)            -> recovery_switch_to_oxid(oxid, -1)
                                   - hash_search HASH_ENTER (new entry if first)
                                   - sets cur_recovery_xid_state
                                   - reset_cur_undo_locations() for the new
                                     oxid (or restores prior undo stacks
                                     if we'd seen this oxid before -- e.g.
                                     workload split across containers).
                                   - if new: adds to xmin_queue, sets csn =
                                     COMMITSEQNO_INPROGRESS.

WAL_REC_RELATION(oids,type)  -> sets ctx->descr / ctx->indexDescr; no state
                                change to the oxid hash.

WAL_REC_INSERT/UPDATE/DEL    -> spread_idx_modify(...) OR apply_modify_record(...).
                                Each successful o_btree_modify writes a NEW
                                UndoLocation into the leaf tuple header and
                                advances cur_undo_locations.{branch,subxact,
                                onCommit}.  The runtime path's
                                add_new_undo_stack_item is shared with redo:
                                no special "recovery" undo emission.

WAL_REC_SAVEPOINT(parent)    -> recovery_savepoint() -> add_subxact_undo_item().
                                Records a savepoint frame in the undo stack
                                so a future ROLLBACK_TO_SAVEPOINT can unwind
                                to it.

WAL_REC_ROLLBACK_TO_SAVEPOINT-> recovery_rollback_to_savepoint() ->
                                rollback_to_savepoint() walks the undo
                                stack tail to the matching subxact frame,
                                reverting in-progress modifications.

WAL_REC_COMMIT / ROLLBACK    -> recovery_finish_current_oxid(csn, ptr, -1, sync).
                                See 7.3.
```

#### 7.3 `recovery_finish_current_oxid` — the per-record close

At `src/recovery/recovery.c:1871`. Three branches by csn/sync:

**Commit + sync** (`!COMMITSEQNO_IS_ABORTED && sync`, line 1883):

```
set_oxid_csn(oxid, COMMITSEQNO_COMMITTING);
if flush_undo_pos: flush_current_undo_stack();
for each undo log type:
    precommit_undo_stack(undoType, oxid, true);
for each undo log type:
    on_commit_undo_stack(undoType, oxid, true);
walk_checkpoint_stacks(state, csn, InvalidSubTransactionId, flush_undo_pos);
csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
set_oxid_csn(oxid, csn);
set_oxid_xlog_ptr(oxid, XLOG_PTR_ALIGN(ptr));
```

The CSN flip pattern (`COMMITTING` -> real CSN) is the same as the runtime
path in `undo_xact_callback`'s XACT_EVENT_COMMIT case (`src/transam/undo.c`
around line 2321-2386). Both call `current_oxid_precommit`/`current_oxid_commit`
on the runtime path; redo opens-codes the equivalents
(`precommit_undo_stack` / `on_commit_undo_stack` are the per-undo-log
helpers that consume the redo-rebuilt undo locations).

**Commit + async** (no sync; line 1899):

Same precommit/on_commit/walk steps, but defers the CSN assignment and the
`set_oxid_csn(...)` call until the leader catches up — the oxid is pushed
into `finished_list` and processed in batches. This is the parallel-recovery
optimisation: workers can report "I'm done" to the leader before the leader
has synthesised the actual CSN.

**Abort** (`COMMITSEQNO_IS_ABORTED`, line 1913):

```
if flush_undo_pos: flush_current_undo_stack();
for each undo log type:
    apply_undo_stack(undoType, oxid, NULL, true);
walk_checkpoint_stacks(state, csn, InvalidSubTransactionId, flush_undo_pos);
if worker_id < 0:
    if sync: set_oxid_csn(oxid, COMMITSEQNO_ABORTED);
    else: push into finished_list (so workers don't see runXmin advance
                                   past us prematurely).
```

`apply_undo_stack` is the *runtime* abort/rollback machinery
(`src/transam/undo.c:1408`): it walks the redo-rebuilt undo log and
inverse-applies each record. Because the undo log was rebuilt purely by
redo's earlier `o_btree_modify` calls, applying it during abort produces
exactly the inverse effect — undoing what redo just did. Net: an aborted
transaction in WAL is replayed by *inserting* the rows (rebuilding undo as
a side effect) and then *undoing* them (consuming that same undo). The
on-disk state ends up as it would have without the transaction.

#### 7.4 The xids file and "checkpoint_xid"

When a checkpoint completes, `o_perform_checkpoint` writes a `chkp_N.xid`
file listing every oxid that was *in-flight* at the moment of checkpoint
(see `flush_current_undo_stack` at `src/recovery/recovery.c:1853` and
`write_to_xids_queue` for the format). Each entry includes the
`UndoStackLocations` for each undo log and a `retainLocation`.

At startup, `read_xids()` (`src/recovery/recovery.c:820`) reads back this
file. For every oxid in it, it pre-populates a `RecoveryXidState` with
`checkpoint_xid = true` and `csn = COMMITSEQNO_INPROGRESS`. This way, when
redo later sees a `WAL_REC_COMMIT` or `WAL_REC_ROLLBACK` for that oxid,
`recovery_switch_to_oxid` finds the existing entry (instead of creating a
fresh one) and the undo stacks the checkpoint saved are already loaded —
ready for `recovery_finish_current_oxid` to walk them.

If a WAL record references an oxid the xids file did not mention, that's a
new oxid that started *after* the checkpoint; redo creates the
`RecoveryXidState` on the fly from the WAL_REC_XID record. Crucially,
*either way* the oxid -> undo stack mapping is whole by the time the
COMMIT/ROLLBACK record arrives.

#### 7.5 `recovery_finish` — final cleanup

At end of recovery (`src/recovery/recovery.c:1620`) the leader walks every
remaining oxid in `recovery_xid_state_hash`. Any oxid still
`COMMITSEQNO_IS_INPROGRESS` is treated as a crashed in-flight transaction:
its undo stacks are applied, and the runtime xid map records it as
`COMMITSEQNO_ABORTED`. Any oxid in the `finished_list` (async-commit case)
has its CSN synthesised here.

Net effect: by the time `recovery_finish` returns, the runtime CSN map is
identical to what would have existed if no crash had happened — all
committed oxids have real CSNs, all aborted (including in-flight) oxids
are marked `COMMITSEQNO_ABORTED`.

### 8. Recovery workers: parallel redo by oxid hash

`recovery_pool_size_guc` (default 3) controls how many background workers
the leader spawns to do parallel redo. Each is an
`orioledb recovery worker N` registered as a `BGWORKER_SHMEM_ACCESS`
worker by `recovery_worker_register` (`src/recovery/worker.c:164`). On
startup they all call into `recovery_worker_main`
(`src/recovery/worker.c:199`).

#### 8.1 Why sharded — and what's *not* sharded

The orioledb engine's PK trees can be modified concurrently because each leaf
modify only locks the *one* PK leaf it's touching. Two transactions that
modify rows with different PK keys never block each other. The same property
makes redo safely parallelisable: as long as two records with the *same* PK
key go to the *same* worker, ordering is preserved.

Hash:
```c
hash = o_btree_hash(desc, rec, BTreeKeyLeafTuple);  // or BTreeKeyNonLeafKey
worker_id = hash % recovery_pool_size_guc;          // GET_WORKER_ID(hash)
```
(`src/recovery/recovery.c:4694, 41`)

The hash is over the tuple's *btree key* — the PK key for PK records, the
SK key for SK records. Net effect:

- All records for a single (tree, PK-key) end up on the same worker.
- All records for a single (tree, SK-key) end up on the same worker.
- Records on the **same row** but in different trees (PK record + SK record
  for the same row) generally land on **different** workers, because the
  hash inputs are different.

**Not sharded** (always leader):

- System-tree modifications (`apply_sys_tree_modify_record`,
  `src/recovery/recovery.c:4662`).
- `WAL_REC_RELATION`: pure ctx update; the leader caches `descr` then sends
  per-modify messages with `RECOVERY_MODIFY_OIDS` flag set so the worker
  re-fetches the same descr (`src/recovery/worker.c:413-446`).
- `WAL_REC_O_TABLES_META_LOCK/UNLOCK`, `WAL_REC_TRUNCATE`,
  `WAL_REC_BRIDGE_ERASE` (single-process path; in parallel, BRIDGE_ERASE
  is sharded by `o_hash_iptr`, line 3916).
- The toast-consistent boundary handshake (`workers_synchronize` +
  `apply_pending_sk_fixups` + `workers_notify_toast_consistent`).
- The final CSN assignment for sync commits.

#### 8.2 Message format and the per-worker queue

Each worker has a fixed-size shm_mq queue
(`RECOVERY_QUEUE_BUF_SIZE = 8KB`, `include/recovery/internal.h:70`),
located by `GET_WORKER_QUEUE(worker_id)` from a global
`recovery_first_queue` base. Messages all start with a
`RecoveryMsgHeader { uint32 type }`. The header type encodes both the
operation (low byte: `RECOVERY_MSG_OPERATION_MASK = 0xFF`) and flags
(`RECOVERY_MODIFY_OXID = 0x100`, `RECOVERY_MODIFY_OIDS = 0x200`).

Message types (`include/recovery/internal.h:48-65`):

| Message                                    | Carries                                                                        |
|--------------------------------------------|--------------------------------------------------------------------------------|
| `RecoveryMsgTypeInsert/Update/Delete`      | header + (optional oxid) + (optional oids+type) + tuple_len + flags + tuple.   |
| `RecoveryMsgTypeBridgeErase`               | header + (optional oxid/oids) + ItemPointerData.                               |
| `RecoveryMsgTypeCommit/Rollback`           | `RecoveryMsgOXidPtr { oxid, ptr, needsFeedback }`.                             |
| `RecoveryMsgTypeSynchronize`               | `RecoveryMsgPtr { ptr }`.                                                      |
| `RecoveryMsgTypeFinished`                  | empty.                                                                         |
| `RecoveryMsgTypeToastConsistent`           | empty.                                                                         |
| `RecoveryMsgTypeSavepoint`                 | `RecoveryMsgSavepoint { oxid, parentSubId }`.                                  |
| `RecoveryMsgTypeRollbackToSavepointt`      | `RecoveryMsgRollbackToSavepoint { oxid, ptr, parentSubId }`.                   |
| `RecoveryMsgTypeLeaderParallelIndexBuild`  | OTable serialisation + ix_num + isrebuild + oxid.                              |
| `RecoveryMsgTypeWorkerParallelIndexBuild`  | DSM seg handle + oxid.                                                         |
| `RecoveryMsgTypeInit`                      | empty (first message per worker).                                              |

Messages are batched by the leader into a per-worker `queue_buf` and flushed
when full (`worker_queue_flush`, `src/recovery/recovery.c:4637`).

#### 8.3 The worker's main loop

`recovery_queue_process` (`src/recovery/worker.c:362`) is a tight
`while (!finished)` over `recovery_queue_read` -> per-message dispatch.
On each modify, the worker:

1. Reads the optional oxid (`RECOVERY_MODIFY_OXID` flag) and switches its
   own `cur_recovery_xid_state` (`recovery_switch_to_oxid(oxid, id)`).
2. Reads the optional (oids, ix_type) (`RECOVERY_MODIFY_OIDS`) and refetches
   the table/index descriptor. If `ix_type == oIndexInvalid`, the message
   targets the PK / table-wide path (with SK fanout) — otherwise it targets
   a specific SK or bridge tree.
3. Reads the tuple bytes from the queue buffer.
4. Calls `apply_modify_record(descr, indexDescr, type, tuple)`.

The worker reports progress via `update_worker_ptr(id, ptr)`
(`src/recovery/worker.c:347`) which writes
`worker_ptrs[id].commitPtr = ptr` and bumps `worker_ptrs_changes`.
The leader can compute the "min commitPtr across workers" via
`get_workers_commit_ptr()` (`src/recovery/recovery.c:1259`).

#### 8.4 Synchronization: `workers_synchronize`

When the leader needs all workers to drain to a given LSN
(`workers_synchronize`, declared at `src/recovery/recovery.c:677`), it
broadcasts a `RecoveryMsgTypeSynchronize { ptr }` message to each worker
and then spins reading `get_workers_commit_ptr() >= ptr`. This is the
single chokepoint between "PK records done up to LSN L" and "SK records
can start" (for the toast-consistent boundary), and between "txn applied
on all workers" and "leader can write the CSN" (for the sys-tree-modifying
commit path, `src/recovery/recovery.c:3723`).

#### 8.5 Tail of recovery: `workers_send_finish` + join

At `o_recovery_finish_hook` (`src/recovery/recovery.c:1209`) the leader
sends `RecoveryMsgTypeFinished` to each worker, then `worker_wait_shutdown`
in a loop until each worker has incremented `worker_finish_count` (and the
optional `idx_worker_finish_count`). A worker that fails to ack the finish
message in time is treated as a recovery failure
(`src/recovery/recovery.c:1247-1250`).

The `index_build_leader` and the `recovery_idx_pool_size_guc` extra
workers handle parallel index builds during recovery (used when
`WAL_REC_RELATION` records describe a new SK whose data must be built
from the PK tree). They're listed at
`src/recovery/worker.c:479-582` (`Recovery{Leader,Worker}ParallelIndexBuild`).

### 9. TOAST and the `toastConsistentPtr` boundary

The TOAST tree is a per-table B-tree storing chunks of out-of-line values.
A row in the PK tree may carry a "toast pointer" pointing into the toast
tree; an SK on that row may also reference the toasted value (e.g. an
SK on a text column that gets toasted). On the primary, writers always
write the toast chunks *before* the PK tuple, and the PK tuple *before*
the SK tuple. The toast tree is therefore at-least-as-current as the PK,
and the PK is at-least-as-current as the SK.

This is the foundation of the per-checkpoint boundary `toastConsistentPtr`.

#### 9.1 Two-phase checkpoint, the writer's view

`o_perform_checkpoint` (`src/checkpoint/checkpoint.c:1305`) walks trees in
a strict order:

1. Sys trees, holding `oTablesMetaLock` + `oSysTreesLock` (line 1389-1409).
2. All trees with `td->type < oIndexUnique` (PK + toast + plain indices),
   no global lock (line 1414).
3. First tree with `td->type >= oIndexUnique` (i.e. a unique/SK index)
   triggers the `toastConsistentPtr` capture and the `PendingSkFixup`
   snapshot:

   ```c
   if (td->type >= oIndexUnique &&
       XLogRecPtrIsInvalid(checkpoint_state->toastConsistentPtr))
   {
       checkpoint_state->toastConsistentPtr = get_checkpoint_xlog_ptr();
       checkpoint_write_pending_sk_fixups();
   }
   ```
   (`src/checkpoint/checkpoint.c:5105-5116`)
4. All remaining SK trees.

The fallback case (no SK trees exist at all) is at lines 1426-1430: the
checkpointer still sets the boundary and writes (an empty)
`PendingSkFixup` snapshot. This keeps the recovery boundary present even
for tables with no SKs.

#### 9.2 The PendingSkFixup snapshot

`checkpoint_write_pending_sk_fixups()` (`src/checkpoint/checkpoint.c:976`)
walks `oProcData[i]` for every backend slot and harvests
`pg_atomic_read_u64(&oProcData[i].pendingSkUndoLoc)`. If that field holds a
valid undo location, the backend is in the **PK-applied / SK-pending
window** (Section 5.4): the PK row has been inserted/updated/deleted but
the SK side hasn't been emitted yet. The checkpointer writes one
`XidFileRec { kind = XidRecPendingSkFixup, oxid, undoLocation, ... }` per
such backend into the active xids file (chkp_N.xid).

A special sentinel value `WaitingSkUndoLoc` means "the backend will
shortly start the PK insert, on a self-created table" — the checkpointer
*spins* outside the proc's flushLock (`src/checkpoint/checkpoint.c:1006`)
until that sentinel clears, then captures the real undo location.

#### 9.3 The recovery boundary handshake

In `orioledb_redo`, just before replaying the first WAL record at LSN
>= `controlToastConsistentPtr`:

```c
if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr &&
    !toast_consistent)
{
    if (!recovery_single)
        workers_synchronize(record->ReadRecPtr, true);   // drain workers
    apply_pending_sk_fixups();                           // emit missing SK
    toast_consistent = true;
    if (!recovery_single)
        workers_notify_toast_consistent();               // tell workers
}
```
(`src/recovery/recovery.c:1171-1189`)

`apply_pending_sk_fixups()` (`src/recovery/recovery.c:600`) walks the
in-memory `pending_sk_fixups_head` list (populated from the xids file by
`record_pending_sk_fixup`, `src/recovery/recovery.c:306`) and, for each
entry:

1. `recovery_switch_to_oxid(entry->oxid, -1)`.
2. `set_oxid_csn(entry->oxid, COMMITSEQNO_INPROGRESS)`.
3. `apply_one_pending_sk_fixup(entry)` — `src/recovery/recovery.c:325`:
   - Reads the PK undo record back via `undo_read(UndoLogRegular,
     itemLoc, ...)`.
   - Resolves the table descr through `o_fetch_index_descr(item.oids,
     oIndexPrimary, ...)`.
   - Looks up the current PK row on the PK tree (the row is still there
     even if `tuphdr->deleted` is set, because no vacuum has run yet).
   - For each SK in `descr->indices[1..]`, computes the old and new SK
     keys and applies DELETE/INSERT pairs through `o_btree_modify` with
     the same `recovery_*_overwrite_callback` as the worker path,
     making the operation idempotent against any later WAL records.

After all fix-ups, the leader flips `toast_consistent = true` and sends
`RecoveryMsgTypeToastConsistent` to each worker
(`src/recovery/recovery.c:4619`).

#### 9.4 The race window

The window the boundary mechanism is designed to close:

1. Primary `T1` does PK INSERT, undo record written, PK leaf modified.
2. **Checkpointer crosses to SKs at this exact moment**, captures
   `toastConsistentPtr` = LSN_N, snapshots `T1.pendingSkUndoLoc`.
3. Primary `T1` does SK INSERT, undo record written, SK leaf modified.
   But the SK leaf write is at LSN_N+1, which is *past* the
   `toastConsistentPtr`, so the on-disk SK page snapshot misses it.
4. Crash. Primary restarts.
5. Recovery replays WAL from `controlReplayStartPtr`. The PK INSERT at
   LSN_N is applied (PK only, because `!toast_consistent`). The SK
   INSERT at LSN_N+1 *is* in WAL, but…
6. At LSN_N+1 the boundary trips: `apply_pending_sk_fixups()` runs first
   (using the snapshot from step 2), then the WAL stream resumes.

The fix-up at step 6 inserts the SK row using the undo-recorded PK key.
*Then* the WAL replay at step 6 inserts the same SK row again from the
WAL stream. Idempotence (`recovery_insert_overwrite_callback`) ensures
the second insert is a no-op.

#### 9.5 The known failure mode (active investigation context)

The SK-leak under investigation does NOT appear to involve the boundary
mechanism (no crash near a checkpoint, no `PendingSkFixup` records
visible in failing trials). It instead reproduces with stress workload
that crashes via `orioledb-before-pre-commit-wal-finish`
(`src/recovery/wal.c:338`) — i.e. between PK undo write and
`WAL_REC_COMMIT` flush. The recovery path under suspicion is the one in
Section 7.3 (the rollback case for an in-flight oxid). The boundary
machinery here is documented for completeness; see Topic 4 for the WAL
sequence edge cases relevant to the active bug.

### 10. Recovery finish: cleanup, dropped tables, file cleanup

#### 10.1 `o_recovery_start_hook` and `o_recovery_finish_hook`

The hooks are wired in `src/orioledb.c` near `_PG_init` via
`o_recovery_start_hook` / `o_recovery_finish_hook` pointers (declared at
`include/recovery/recovery.h:21,24`). They straddle the redo loop:

- `o_recovery_start_hook` (`src/recovery/recovery.c:1025`): runs once,
  *before* the first `orioledb_redo` call. Initialises
  `recovery_xid_state_hash`, `idxbuild_oids_hash`, spawns the worker pool
  if `recovery_pool_size_guc > 0`, reads the previous-checkpoint xids file
  via `read_xids()` (line 1042 calls `recovery_cleanup_old_files` first to
  drop unfinished checkpoint sidecars), and finally calls
  `apply_xids_branches()` to register the saved in-flight oxids.

- `o_recovery_finish_hook` (`src/recovery/recovery.c:1209`): runs once,
  after the *last* `orioledb_redo`. Sends `RecoveryMsgTypeFinished` to
  each worker, waits for them, calls `recovery_finish(-1)` (Section 7.5),
  and, when `cleanup && remove_old_checkpoint_files`, runs a second
  `recovery_cleanup_old_files(startup_chkp_num, false)` to drop the
  *previous* generation of per-tree files now that no recovery process
  can possibly reference them.

#### 10.2 `recovery_cleanup_old_files`

At `src/recovery/recovery.c:3154`. Walks `orioledb_data/` (and every
tablespace's mirror) deleting files for checkpoint numbers earlier than
the parameter. Two modes:

- `before_recovery=true` (called at start): drop checkpoint sidecars (like
  `<chkpNum+1>.xid.tmp`) left half-written by a crash mid-checkpoint, and
  any per-tree file segments newer than `lastCheckpointNumber + 1` that
  could not possibly be referenced by the control file we're about to
  recover from.
- `before_recovery=false` (called at end): drop the *previous*
  checkpoint's per-tree files, now that the control file is durable.

#### 10.3 Table drops, truncates, and replica index rebuilds

`WAL_REC_O_TABLES_META_UNLOCK` (line 3842) ends a meta-locked section.
Its replay path goes through `handle_o_tables_meta_unlock` which checks
whether the table's OIndexChunk was inserted or deleted in the meta-locked
section. If deleted, `add_undo_drop_relnode` (line 3968) defers the file
deletion until the undo log catches up; if inserted,
`add_undo_create_relnode` (line 3994) does the inverse. Either way the
*file deletion* doesn't happen synchronously inside redo; it's scheduled
through the same undo path that the runtime uses, so a roll-back during
or just after redo correctly undoes it.

`WAL_REC_TRUNCATE` (line 3864) handles `TRUNCATE TABLE` by calling
`o_truncate_table(oids, true)` after a `workers_synchronize` chokepoint
to drain pending modifications. The `true` second argument indicates
"during recovery" — `o_truncate_table` uses the standard runtime path
to drop and recreate per-tree state but skips WAL emission.

Index drops/creations seen mid-stream (e.g. `WAL_REC_O_TABLES_META_LOCK`
followed by `WAL_REC_O_TABLES_META_UNLOCK` and bracket modifications to
`SYS_TREES_O_INDICES`) trigger `rebuild_indices` /
`build_secondary_index` if the table's primary structure changed. The
leader may delegate the actual rebuild to the index-build pool via
`recovery_send_leader_oids` (`src/recovery/recovery.c:3551, 3590`,
declaration at `include/recovery/recovery.h:33`).

#### 10.4 Promotion / "ready for new connections"

After `o_recovery_finish_hook` returns, the cluster is ready to accept
non-recovery connections. The flag mechanism for this is PG-native:
recovery completes, `recovery_target_*` or end-of-WAL is detected by
startup, startup signals postmaster, and orioledb's `iam_recovery` flag
goes false on the relevant processes. Backends that connect from this
point on enter the runtime path (`is_recovery_in_progress()` returns
false; see `include/recovery/recovery.h:41`).

### 11. ASCII diagrams

#### 11.1 Redo call graph (parallel mode)

```
PG startup process
   |
   +-- xlog dispatch: rmgr==RM_ORIOLEDB_ID
       |
       v
   orioledb_redo(record)                 src/recovery/recovery.c:1106
       |
       +-- PITR check (orioledb.replay_until_lsn)
       |
       +-- if reach controlToastConsistentPtr:
       |       workers_synchronize(L, send_synchronize=true)
       |       |   for w in workers:
       |       |       send RecoveryMsgTypeSynchronize{ptr=L}
       |       |   spin until min(worker_ptrs[*].commitPtr) >= L
       |       apply_pending_sk_fixups()  src/recovery/recovery.c:600
       |       toast_consistent = true
       |       workers_notify_toast_consistent()
       |
       +-- if past controlReplayStartPtr:
               |
               v
           replay_container(start,end,single,readPtr,endPtr)
               |
               +-- wal_parse_container(WalReaderState{...})
                       |
                       v
                   replay_on_record(WalReaderState*, WalRecord*)  recovery.c:3677
                       |
                       +-- WAL_REC_XID:
                       |     advance_oxids() + recovery_switch_to_oxid(oxid,-1)
                       |
                       +-- WAL_REC_RELATION:
                       |     update ctx->descr / ctx->indexDescr
                       |
                       +-- WAL_REC_INSERT/UPDATE/DELETE:
                       |     if sys_tree_num > 0:
                       |         apply_sys_tree_modify_record(...) [leader]
                       |     if ctx->indexDescr != NULL:
                       |         if single:
                       |             apply_modify_record(descr, indexDescr, type, tuple)
                       |         else:
                       |             spread_idx_modify(&indexDescr->desc, type, tuple)
                       |                 hash = o_btree_hash(...)
                       |                 worker_send_modify(hash%pool, ...)
                       |
                       +-- WAL_REC_COMMIT / WAL_REC_ROLLBACK:
                       |     workers_send_oxid_finish(...)
                       |     if systree_modified: workers_synchronize(...)
                       |     recovery_finish_current_oxid(csn, ptr, -1, sync)
                       |
                       +-- WAL_REC_SAVEPOINT / WAL_REC_RB_TO_SP:
                       |     recovery_savepoint / recovery_rollback_to_savepoint
                       |     + workers_send_savepoint / workers_send_rollback_to_savepoint
                       |
                       +-- WAL_REC_O_TABLES_META_LOCK / UNLOCK:
                       |     leader-only; modifies system trees + invalidations
                       |
                       +-- WAL_REC_TRUNCATE / WAL_REC_BRIDGE_ERASE / etc.
```

Worker side:

```
recovery_worker_main(N)                  src/recovery/worker.c:199
    +-- shm_mq_attach(GET_WORKER_QUEUE(N))
    +-- recovery_queue_process(queue, N)
            loop:
                data = recovery_queue_read(queue, &size, N)
                for each message in data:
                    Insert/Update/Delete -> apply_modify_record(descr,indexDescr,type,tuple)
                                            -> if toast_consistent: apply_tbl_modify_record
                                                                      |
                                                                      +-- apply_tbl_insert/delete/update
                                                                          for i in descr->indices:
                                                                              o_btree_modify(&indices[i]->desc, ...)
                                               else: apply_btree_modify_record(&id->desc, ...)
                    Commit/Rollback -> recovery_finish_current_oxid(csn,ptr,N,false)
                                       update_worker_ptr(N, ptr)
                    Synchronize     -> update_worker_ptr(N, ptr)
                    ToastConsistent -> toast_consistent = true
                    Savepoint/RB    -> recovery_savepoint / recovery_rollback_to_savepoint
                    Finished        -> break
```

#### 11.2 Page-LSN comparison ladder (the actual mechanism)

```
WAL record R at LSN L hits redo:

   |    +-- L < controlReplayStartPtr ---------------+
   |    |                                            |
   |    |   on-disk PK/SK pages already absorbed     |
   |    |   this via CoW write. Skip container.      |
   |    +--------------------------------------------+
   |
   |    +-- controlReplayStartPtr <= L                +
   |    |   < controlToastConsistentPtr,              |
   |    |   !toast_consistent                         |
   |    |                                             |
   |    |   Dispatch into replay_on_record:           |
   |    |   sys-tree:  applied if L >= sysTreesStartPtr
   |    |   PK record: apply_btree_modify_record(PK)  |
   |    |   SK record: apply_btree_modify_record(SK)  |
   |    |              one tree only (no fanout)      |
   |    +---------------------------------------------+
   |
   |    +-- At LSN_N == controlToastConsistentPtr      +
   |    |                                              |
   |    |   workers_synchronize(LSN_N, true)            |
   |    |   apply_pending_sk_fixups()                   |
   |    |      walk pending_sk_fixups_head:             |
   |    |          for each entry { oxid, undoLoc }:    |
   |    |              undo_read(...) -> recover undo   |
   |    |              fetch PK page, get current tuple |
   |    |              for each SK in descr:            |
   |    |                  o_btree_modify(SK, DELETE+INSERT)
   |    |   toast_consistent = true                     |
   |    |   workers_notify_toast_consistent()           |
   |    +-----------------------------------------------+
   |
   v    +-- L > controlToastConsistentPtr               +
        |   toast_consistent == true                    |
        |                                               |
        |   Dispatch into replay_on_record:             |
        |   PK record (or unified): apply_modify_record |
        |       -> apply_tbl_*: walk descr->indices,    |
        |          modify PK AND every SK with changed  |
        |          key.                                 |
        |                                               |
        |   Within o_btree_modify, recovery_*_callback  |
        |   inspects existing tuple's xactInfo/deleted: |
        |       if same oxid + same op: no-op           |
        |       else: apply                             |
        +-----------------------------------------------+
```

#### 11.3 Consistent-point timeline

```
WAL time-axis: ----+-------+-------+-------+-------+----+-->
                   ^       ^       ^       ^
                   |       |       |       |
                   replayStartPtr  |       |
                   |               |       |
                   |        sysTreesStartPtr (= same in current code)
                   |               |
                   |        toastConsistentPtr
                   |               |
                                   |
                                   end-of-checkpoint (record durable)
                                   |
                                   redo_pos in PG control file


  Checkpointer phase:
     [   sys-tree image    ][  PK-tree image   ][  SK-tree image   ]
          |                       |                    |
          oTablesMetaLock         |                    |
          oSysTreesLock           |                    |
          held                   relased              relased
                                                       |
                                                  toastConsistentPtr
                                                  + PendingSkFixup snapshot

  Recovery dispatch from this control file:
     skip --------------------> replay PK only ---------> replay PK+SK
     (L < controlReplayStartPtr) (controlReplayStartPtr   (>= controlToastConsistentPtr)
                                  <= L
                                  < controlToastConsistentPtr)

     For sys-tree records:
     skip --------------------> applied
     (L < controlSysTreesStartPtr)  (>= controlSysTreesStartPtr)
```

Note that in practice `replayStartPtr == sysTreesStartPtr` whenever sys-tree
checkpointing finishes promptly: both are captured under the same
oTablesMetaLock/oSysTreesLock acquisition. The two fields exist for the
case where checkpoint sys-tree work is non-trivial and a record arrives
between them.

### 12. Open questions and pointers into the active investigation

This section anchors the SK-leak hypotheses (from
`feature/add_stress_bank_account_test`) onto the code surveyed above. None
of these are conclusions; they are the surface area to investigate.

#### 12.1 The race window the harness is actually exercising

The crash injection point that reproduces the bug is
`orioledb-before-pre-commit-wal-finish` (`src/recovery/wal.c:336-340`).
This is *inside* `wal_commit`, *between* the `add_xid_wal_record_if_needed`
call and `add_finish_wal_record(WAL_REC_COMMIT, ...)` call. Concretely:

```
PRIMARY backend B1:
   ... earlier in transaction ...
   o_btree_modify(PK, INSERT, tuple) -> undo record, leaf tuple, PK row visible
                                        only as xactInfo.oxid=B1's.
   o_btree_modify(SK1, INSERT, ...)   -> similarly
   ... many more rows ...
   CommitTransaction:
       wal_commit:
           flush local WAL buffer  [PK INSERT, SK1 INSERT, ...]
                                   [...flushed to PG WAL stream...]
           add_xid_wal_record(B1.oxid, B1.logicalXid)  if needed
           --> START_CRIT_SECTION; INJECTION_POINT("orioledb-before-pre-commit-wal-finish"); ...
               PANIC here (error -> escalates to PANIC inside crit section).
           [WAL_REC_COMMIT is NEVER appended; never flushed.]
```

So the WAL stream contains the row records (PK and SK), an optional XID
record, but *no* COMMIT or ROLLBACK record for that oxid.

#### 12.2 What recovery does with such an oxid

`recovery_switch_to_oxid` creates the `RecoveryXidState` entry on first
sighting (Section 7.1). Subsequent INSERT/UPDATE/DELETE records run
through `apply_modify_record`. The PK row is inserted; if past
`controlToastConsistentPtr`, the SK is also inserted via `apply_tbl_insert`
(Section 5.3).

When the WAL stream ends (or moves to a different oxid) without a
COMMIT or ROLLBACK, `recovery_finish` (`src/recovery/recovery.c:1620`) is
the catch-all. Its first inner loop (line 1641) iterates every entry in
the hash and, for any whose `csn == COMMITSEQNO_INPROGRESS`, runs:

```c
for each undo log type:
    set_cur_undo_locations(undoType, cur_state->undo_stacks[undoType]);
if flush_undo_pos:
    flush_current_undo_stack();
for each undo log type:
    apply_undo_stack(undoType, recovery_oxid, NULL, true);
walk_checkpoint_stacks(cur_state, COMMITSEQNO_ABORTED, ..., flush_undo_pos);
```

In other words: a never-committed in-flight oxid is *aborted* by walking
its undo log and inverse-applying every record. The SK INSERT we just
applied during redo should be inverted by an SK DELETE produced by the
undo walk — assuming the undo log is well-formed and the SK undo entry
is reachable from one of the four locations in `cur_state->undo_stacks`
(branch/subxact/onCommit/...).

The bug hypothesis is that, in some path through this code, the SK
undo entry **is not consumed** by `apply_undo_stack`, leaving an SK
row alive without a corresponding PK row.

#### 12.3 Code surfaces that match the symptom

The harness's `sk_extra` set (SK rows whose PK key is absent in PK tree)
points at undo entries that didn't apply. Candidate sites where this can
happen:

1. **`apply_undo_stack` stops short.** If `cur_state->undo_stacks[i]` is
   incomplete — e.g. only the branch location is set, the subxact tail is
   not — `apply_undo_stack` will not reach the SK undo item. Check
   `set_cur_undo_locations` paths in `src/transam/undo.c:2023` for any
   case where a stack is restored with `InvalidUndoLocation` even though
   the records actually were emitted.

2. **`recovery_finish` vs `recovery_finish_current_oxid` divergence.**
   The "leaf cleanup" loop in `recovery_finish` (line 1641-1672) does
   `set_cur_undo_locations` from `cur_state->undo_stacks` then
   `apply_undo_stack`. The normal commit/abort path
   (`recovery_finish_current_oxid`) takes the same steps. If
   `recovery_switch_to_oxid` was called for this oxid but
   `recovery_finish_current_oxid` was *not* (because the WAL stream ran
   out), the cleanup loop is the only safety net. Cross-check whether
   `cur_state->undo_stacks` was kept in sync as records were applied —
   it's only stored on `recovery_switch_to_oxid`'s save path (line
   1730-1741), which runs **only when switching away** from the oxid,
   not after each modify.

3. **Inter-oxid races in parallel recovery.** When records are sharded
   to workers, the worker stores its *own* `cur_recovery_xid_state`
   (per-worker hash). The leader's hash entry for the same oxid is
   distinct and is updated only when the leader itself touches the
   record. The undo stack the leader sees might lag behind the
   worker's. `workers_synchronize` aligns workers' progress against an
   LSN, but does NOT export the workers' undo-stack tail to the leader.
   On end-of-WAL with no COMMIT, the leader's `recovery_finish`
   cleanup uses the leader's hash entry — which may have a partial
   undo stack — to call `apply_undo_stack`. The workers' hash entries
   are torn down separately in `recovery_finish` per-worker.

4. **`apply_undo_stack` with `toLocation=NULL`** (line 1659 of
   `recovery.c`) walks the whole stack tail. If, however, the stack is
   anchored at `cur_undo_locations` which itself was reset by an
   earlier `reset_cur_undo_locations` (line 2031 of undo.c, or any of
   the per-record `reset_cur_undo_locations` calls at 2147, 2386), the
   walk silently terminates immediately. The current branch's
   instrumentation (the `csn-trace` markers) is designed to expose
   exactly this scenario.

#### 12.4 Relevant `csn-trace` / `wal-trace` markers

The active investigation has surrounded several call sites with
elog(LOG,…) markers (intentionally — these are timing scaffolding,
not just diagnostics). The most relevant for this topic:

- `src/btree/page_state.c` per-op `lock_page request/got/wait/release`
  — broadens the SK race window so the bug repro rate climbs from ~33%
  to ~75%. Removing these silently changes the timing.
- `src/transam/undo.c` `commit-assert-trace pre/post` around line 2400
  — captures the COMMIT path's pre-flip / post-flip state.
- `src/recovery/wal.c` `wal-trace modify` (currently `#if 0` gated, line
  140-148) — when enabled, drops bug rate to ~0%. Useful to confirm a
  candidate fix actually narrows the window vs. masks it.

#### 12.5 What this topic does NOT cover

- The exact undo-log layout, which is in Topic 4's territory (WAL +
  undo sequence edge cases).
- The runtime PK->SK ordering on the primary; this is in Topic 2's
  PG <-> orioledb interface coverage (the `pendingSkUndoLoc` mechanism).
- The actual fix candidates; those follow from instrumenting the
  hypotheses above.

---

## 4. WAL-stream edge cases during recovery

*(Source: `recovery_report_topic4_wal_edge_cases.md`; original title: "Topic 4: Edge Cases in the WAL Stream That Recovery Must Handle Correctly")*
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

### 1. Unfinished-Transaction WAL on Crash

#### Scenario

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

#### WAL byte sequence

```
... [XID 17] [INS k1] [INS k2] [UPD k3]  <<TAIL>>
                                          ^
                                          xlogreader stops here: no COMMIT
                                          or ROLLBACK record for oxid 17
```

#### Code path

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

#### CSN state machine and persistence

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

#### Invariant preserved

> *No partial transaction's effects are ever visible after crash recovery
> completes.*

This holds because the visibility check is CSN-based, the CSN is never
`COMMITSEQNO_NORMAL` for an oxid lacking a `WAL_REC_COMMIT` in the WAL
stream, and the final cleanup pass converts dangling INPROGRESS to
ABORTED before the cluster opens for connections.

### 2. Mid-COMMIT Crash (WAL_REC_COMMIT Flushed, Bookkeeping Lost)

#### Scenario

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

#### WAL byte sequence

```
... [XID 17] [INS k1] [UPD k2] [COMMIT 17]   ## flushed to disk
                                          ^
                                          backend died here, before
                                          current_oxid_commit() ran
```

#### Code path

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

#### The `XLogFlush`-vs-bookkeeping window

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

#### Invariant preserved

> *If `WAL_REC_COMMIT` for oxid X is durable, all of oxid X's effects
> become visible after recovery; if it is not, none of them do.*

The boundary is the WAL flush. There is no "half-committed" outcome
because the in-memory CSN/undo bookkeeping is reconstructed during
replay from the durable record, exactly as it would have been on the
primary path — only that the original backend is no longer alive to do
it.

### 3. Torn / Partial WAL Records at the Tail

#### Scenario

A crash interrupts the kernel's writeback of a WAL page. The on-disk
state holds the first few sectors of a logically larger record but not
the rest, or holds the previous segment's recycled bytes spuriously
matching an old prev-link. Recovery must detect that the tail of the
stream is corrupt-by-truncation and stop replaying *before* the
incomplete record — without ever passing partial bytes to OrioleDB's
`replay_on_record()`.

This is entirely a PostgreSQL-layer concern. OrioleDB does not see the
record at all until `xlogreader` has validated it.

#### WAL byte sequence

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

#### Code path

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

#### Implications for OrioleDB

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

#### Invariant preserved

> *No bytes from a torn or truncated WAL record reach OrioleDB's redo
> dispatcher; the recovery cursor stops cleanly at the last fully
> validated record.*

The boundary is the CRC32C check inside `ValidXLogRecord`. The recovery
process resumes new-WAL-generation at exactly that boundary, and an
`OVERWRITE_CONTRECORD` marker is written so future readers know to
ignore the abandoned partial.

### 4. WAL Records Past Last Checkpoint That Touch Already-Flushed Pages

#### Scenario

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

#### WAL byte sequence

```
... [CHECKPOINT redo=L_chkp] [..] [INS p k1] [..] [UPD p k2] [..]   <<TAIL>>
                                  ^^^^^^^^^^         ^^^^^^^^^^
                                  R_1                R_i
                                  may or may not be reflected on disk
                                  depending on whether the writer
                                  flushed P after applying these
```

#### Code path

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

#### What if the on-disk page is newer than the WAL record

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

#### Invariant preserved

> *No modification is applied twice. Recovery replays exactly the set
> of WAL records strictly after `controlReplayStartPtr`, and the
> on-disk page state at that LSN reflects everything strictly before
> it.*

The `controlReplayStartPtr` comparison at `src/recovery/recovery.c:1191`
is the load-bearing site.

### 5. WAL Touching a Tree Whose Checkpoint Metadata Wasn't Fully Written

#### Scenario

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

#### WAL byte sequence

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

#### Code path — three boundary checks

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

#### The `toastConsistentPtr` race and pending-SK-fix-ups

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

#### Correct behaviour without re-litigating the bug

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

#### Invariant preserved

> *At every moment after `toast_consistent = true`, every PK row that
> exists has exactly the corresponding SK tokens; recovery does not
> publish a half-synchronised cross-tree state to backends.*

Because the worker queues are drained before SK replay begins, no
backend can ever observe an SK-without-PK or PK-without-SK except
during the controlled pending-SK-fix-up pass which itself runs while
the recovery process holds the worker synchronisation barrier.

### 6. Out-of-Order Interleaved WAL from Multiple Backends

#### Scenario

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

#### Per-oxid state and the switch protocol

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

#### How records advertise their oxid

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

#### Undo location pointer per record

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

#### Interaction with COMMIT and checkpoint stacks

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

#### Invariant preserved

> *Recovery applies the modifications of each oxid in the order they
> were appended to its local buffer on the primary, even though the PG
> WAL stream interleaves containers from many backends.*

Because the per-oxid undo cursor is saved on every switch and restored
on every re-entry, the redo dispatcher behaves as if it were processing
N independent serial streams whose union happens to be the actual
totally-ordered PG WAL stream.

### 7. `WAL_REC_ROLLBACK` Without Prior Data Records

#### Scenario

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

#### WAL byte sequence

```
... [XID 42] [ROLLBACK 42]   ## flushed
```

#### Code path

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

#### Why this is a no-op and not an error

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

#### Invariant preserved

> *A WAL_REC_ROLLBACK record for an oxid with no prior modifications is
> equivalent to no record at all; recovery's only durable effect is
> setting the oxid's CSN slot to `COMMITSEQNO_ABORTED`.*

This also keeps the dispatcher trivial — there is no need for the redo
to know whether modifies preceded the terminator.

### 8. `WAL_REC_COMMIT` Without Prior Data Records

#### Scenario

The previous checkpoint flushed *all* of oxid 42's modifications into
the on-disk page state (the modifies happened well before
`replayStartPtr`), and oxid 42 then performed its COMMIT *after* the
checkpoint redo pointer. Recovery starts from the checkpoint and sees
only `WAL_REC_XID 42` followed by `WAL_REC_COMMIT 42` — the modifies
are not in the replayed WAL range because they are already on disk.

This is the dual of Case 7 but for COMMIT, and it is the common case
for any long-running transaction that committed shortly after a
checkpoint.

#### WAL byte sequence

```
... [CHECKPOINT redo=L_chkp] ... [XID 42] [COMMIT 42]   ## flushed
                                         ^^^^^^^^^^
                                         oxid 42's modifies happened
                                         before L_chkp and are already
                                         on disk
```

#### Code path

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

#### What about the modifies that are already on disk

They are visible because (a) they are physically present in the
on-disk B-tree pages (the checkpoint serialised them), and (b) their
per-tuple undo headers were written with `oxid = 42`. The visibility
check in `oxid_get_csn()` (`src/transam/oxid.c:1611`) reads the
xidBuffer slot for oxid 42 and finds the CSN written by
`recovery_finish_current_oxid` — so the tuples become visible at
exactly the right moment.

#### Invariant preserved

> *Recovery does not need the modify records to be in the replayed WAL
> range in order to honour a COMMIT. The CSN assigned via
> `set_oxid_csn(oxid, csn)` is the sole gate that makes the (already
> on-disk) modifications visible.*

### 9. WAL Records the On-Disk Page Already Reflects (Idempotence)

#### Scenario

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

#### WAL byte sequence

```
... [INS k=5 v=42] ...
                       ^ may or may not be reflected in the on-disk
                         leaf page when recovery applies this record
                         for the first time, second time, or N-th time
```

#### Code path — idempotence of modify records

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

#### Code path — the CSN/page-checkpointNum gate

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

#### Invariant preserved

> *Replaying the same WAL record N times produces the same on-disk
> state as replaying it once.*

This is a precondition for crash-during-recovery to be safe; see
Case 10 below.

### 10. SIGQUIT Mid-Undo-Replay

#### Scenario

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

#### WAL byte sequence (after the SIGQUIT)

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

#### Code path — what restart does

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

#### What makes the undo stack restartable

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

#### Quickdie-vs-cleanup hazard

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

#### Invariant preserved

> *Recovery is restartable: a crash mid-recovery loses no durable
> state. The next recovery cycle starts from the same checkpoint redo
> pointer, re-applies the same WAL records (idempotently), and reaches
> the same terminal state.*

This is the property that makes the postmaster's "restart on crash"
loop safe — without it, repeated crashes would compound into
corruption.

---

## 5. Streaming replication recovery

A streaming standby runs the *same* redo machinery as single-node crash recovery — the same `recovery.c` WAL applier, the same parallel leader/worker split, the same in-memory undo stacks — but in a **perpetual, stream-fed regime** rather than a bounded one that ends at end-of-redo. Almost every streaming-replication bug on this branch lives in the handful of places where "recovery never ends" changes the behavior of code that single-node crash recovery only ever runs to completion.

### 5.0 Summary

A standby is distinguished from a crash-recovering node by one fact: it **never calls `recovery_finish()`**. On the primary, crash recovery ends when PostgreSQL fires the resource-manager cleanup hook (`o_recovery_finish_hook` → `recovery_finish(-1)`, `src/recovery/recovery.c:1241-1260`), which sets `iam_recovery=false` (`src/recovery/recovery.c:1786`), sweeps in-flight `INPROGRESS` oxids by aborting them in memory (`src/recovery/recovery.c:1683-1709`), and later re-emits those aborts as WAL so downstream standbys can converge. A standby is never restarted when the primary crashes — it merely reconnects to the restarted primary and keeps replaying — so it stays `RecoveryInProgress()==true` forever, `is_recovery_process()` stays true forever, and every recovery-only branch stays latched (§5.1).

The standby is seeded physically by `pg_basebackup` (the whole `orioledb_data` dir, control file, and `.xidmap` files), then begins WAL replay at the control file's `replayStartPtr` seed horizon and reaches a consistent state at `controlReplayStartPtr` (§5.2). From there the parallel-recovery LEADER (worker_id < 0) reads WAL and dispatches modifies to WORKERS (worker_id ≥ 0) by key-hash modulo; **only the leader assigns final CSNs** from `nextCommitSeqNo`, transitioning each oxid `INPROGRESS → COMMITTING → final-CSN` (§5.3). Non-sync commits are deferred onto a `finished_list` and finalized lazily by the leader's drain in `update_proc_retain_undo_location()`, but only up to `listPtr = MIN(worker commitPtr)` — which a spinning worker can pin, producing a deferred-commit-finalization deadlock unique to the standby (§5.4).

The standby computes its own xmin horizon: oxid numbers are identical to the primary's, **CSN is independently re-assigned** (WAL-carried csn is ignored), and **xmin is carried but `globalXmin` diverges** — computed from the local `xmin_queue`, and (unlike the primary's monotonic `advance_global_xmin()`) allowed to move *backward* via `update_run_xmin()`/`free_run_xmin()` (§5.5). The two open livelocks both stem from this: an in-flight oxid the primary aborted **in memory with no WAL** stays `INPROGRESS` on the standby forever, and a deferred-rollback oxid can regress `globalXmin` below `writtenXmin`, re-exposing FROZEN slots that `oxid_get_csn` then mis-reads as `INPROGRESS`. Both manifest as a worker spinning at 100% CPU in `o_btree_modify_handle_conflicts()`. The fix is an eager-WAL + deferred-ROLLBACK schema (§5.6) and a two-part InvalidOXid sentinel + deferred-rollback xmin-queue filter (§5.7). For deeper background on the deferred-commit drain see `deferred_commit_mechanism.md`; for the xid/oxid/csn model see `xid_oxid_csn_in_orioledb.md`; for the second livelock's blow-by-blow see `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md`.

### 5.1 Perpetual recovery: why the standby never leaves the recovery branches

OrioleDB tracks recovery state with a process-local flag. `is_recovery_process()` returns `iam_recovery` (`include/recovery/recovery.h:29`), which is initialized `false` (`src/recovery/recovery.c:257`), set `true` in `recovery_init()` (`src/recovery/recovery.c:1522`), and cleared in `recovery_finish()` (`src/recovery/recovery.c:1786`). The broader predicate `is_recovery_in_progress()` is `is_recovery_process() || RecoveryInProgress()` (`include/recovery/recovery.h:42-46`), where `RecoveryInProgress()` is PostgreSQL's own "startup process has not transitioned to production" check.

On the primary, crash recovery ends through PostgreSQL's resource-manager cleanup path: `o_recovery_cleanup` installs `rm_cleanup` via `RmgrData` (`src/orioledb.c:386-396`), and at end-of-redo PostgreSQL invokes `o_recovery_finish_hook`, which calls `recovery_finish(-1)` (`src/recovery/recovery.c:1241-1260`). That clears `iam_recovery` (`src/recovery/recovery.c:1786`) and transitions to production.

**Where the STANDBY differs:** a streaming standby is never restarted when the primary crashes; it reconnects to the restarted primary and keeps replaying, so `recovery_finish()` is never called and `RecoveryInProgress()` stays true indefinitely (`test/t/crash/ISSUE_streaming_standby_recovery_livelock.md:67-71`). Consequently every recovery-gated branch stays latched permanently:

- `XLogInsertAllowed()` stays false, so the normal abort-path WAL emitter `wal_rollback()` — which asserts `!is_recovery_process()` (`src/recovery/wal.c:433`) — can never run; likewise `wal_emit_recovery_finish_rollback()` asserts `!is_recovery_process()` (`src/recovery/wal.c:490`) and runs only after `LocalSetXLogInsertAllowed()`.
- `is_recovery_process()` keeps the CSN-clobber traces and the `recovery_map_oxid_csn` redirect live in `oxid.c` (`src/transam/oxid.c:635-636`, `745-749`, `1295-1299`, `1929-1935`).
- `recovery_rec_insert/update/delete()` always skip undo-location recording and return the tuple as-is (`src/recovery/recovery.c:2444-2484`; cf. the `!is_recovery_process()` guard at `src/btree/modify.c:789-790`).
- Index builds take the recovery branch: `CreateParallelRecoveryContext` instead of `EnterParallelMode`, and shutdown skips `WaitForParallelWorkersToFinish`/`ExitParallelMode` (`src/catalog/indices.c:758-777`, `1030-1051`).

Critically, `recovery_finish()` is also where the primary aborts leftover `INPROGRESS` oxids — by applying their undo stacks **in memory with no `wal_*` call** (`src/recovery/recovery.c:1683-1709`). A standby that already eagerly applied those transactions' modifies (marked `COMMITSEQNO_INPROGRESS`) never receives a corresponding verdict, so the oxid stays `INPROGRESS` forever, and a later conflicting modify spins in `o_btree_modify_handle_conflicts()` where `oxid_get_csn()` returns `INPROGRESS` on every retry (`test/t/crash/ISSUE_streaming_standby_recovery_livelock.md:74-81`).

### 5.2 Seeding and the streaming pipeline: pg_basebackup → control file → walreceiver

The standby is brought up in three phases.

1. **Physical seed via `pg_basebackup`** copies the entire `orioledb_data` directory, including the control file and the `.xidmap` files tagged `OXID_BUFFERS_TAG` (value 0) (`src/transam/oxid.c:38`, `163-168`). The `.xidmap` file carries the frozen CSN/xlog-ptr mapping for oxids; it is produced on the primary by `write_xidsmap()`, which copies `xidBuffer` to disk via `o_buffers_write` and scans until `COMMITSEQNO_FROZEN` to pick a safe xmax (`src/transam/oxid.c:820-891`). The standby inherits `writtenXmin` from this checkpoint (`src/transam/oxid.c:1189-1216`).

2. **Load checkpoint metadata from the control file.** `CheckpointControl` defines the seed horizon and recovery boundaries: `replayStartPtr` (`include/checkpoint/control.h:51`) and `toastConsistentPtr` (`include/checkpoint/control.h:50`), plus undo/xid watermarks (`lastUndoLocation`, `undoInfo`, `checkpointRetainXmin/Xmax`). At startup these are loaded into the `xid_meta` atomics (`src/checkpoint/checkpoint.c:335-368`), and `controlReplayStartPtr`/`controlToastConsistentPtr` are taken from `control` (`src/checkpoint/checkpoint.c:330-331`).

3. **WAL streaming replay** by the startup process consuming the `walsender → walreceiver → startup` pipeline (`src/recovery/recovery.c:32`, `259-264`). Replay begins at the seed horizon: WAL is applied once `record->ReadRecPtr >= controlReplayStartPtr` (`src/recovery/recovery.c:1224`); the primary stamps `replayStartPtr` from `get_checkpoint_xlog_ptr()` at checkpoint time (`src/checkpoint/checkpoint.c:1392`, `1498`). The pre-toast/post-toast boundary (`controlToastConsistentPtr`) separates PK-only from whole-table recovery (`src/recovery/recovery.c:1204-1222`).

At startup the in-flight transactions captured in the checkpoint's xid file are reloaded by `read_xids()` into `recovery_xid_state_hash`, each initialized `csn = COMMITSEQNO_INPROGRESS`, `checkpoint_xid=true`, `wal_xid=false` (`src/recovery/recovery.c:849-984`), with their undo stacks restored when `lastCheckpointNumber > 0` (`src/recovery/recovery.c:1075`, `1134-1135`).

**Where the STANDBY differs:** the standby resolves in-flight transactions purely from streamed `WAL_REC_COMMIT/ROLLBACK` records — it *cannot* resolve them on its own (`src/recovery/recovery.c:259-264`) — whereas crash recovery can abort them in memory and later flush `recovery_finish_aborted_oxids` as `WAL_REC_ROLLBACK` (`src/recovery/recovery.c:266-274`, `1796`).

### 5.3 LEADER vs WORKERS and CSN assignment

Parallel recovery splits work by `worker_id`: the LEADER is `worker_id < 0` (the startup process), pool WORKERS are `worker_id >= 0`. The leader reads WAL and dispatches each modify to a worker by key hash, `GET_WORKER_ID(hash) = hash % recovery_pool_size_guc` (`src/recovery/internal.h:41`), via `worker_send_modify()` (`src/recovery/recovery.c:4942`, `4948`).

Per-oxid state is created in `recovery_xid_state_hash` by `recovery_switch_to_oxid()` with `csn = COMMITSEQNO_INPROGRESS` (`src/recovery/recovery.c:1827-1943`, csn at `1905`); only the leader adds the oxid to `xmin_queue` (guard `worker_id < 0` at `1924-1927`) and allocates the `used_by` array (`1933-1937`). New oxids' `xidBuffer` slots are filled with `COMMITSEQNO_INPROGRESS` by `advance_oxids()` (`src/transam/oxid.c:1260-1309`, write at `1302-1303`).

**Only the LEADER assigns final CSNs.** On `WAL_REC_COMMIT/ROLLBACK` (`src/recovery/recovery.c:3898-3992`) the leader forwards the verdict to the workers that touched the oxid via `workers_send_oxid_finish()` (selecting on `used_by[i] || checkpoint_xid`, `src/recovery/recovery.c:4759-4802`), then calls `recovery_finish_current_oxid(..., worker_id=-1, sync)`. CSN is fetched from `nextCommitSeqNo` only inside the `worker_id < 0 && sync` branch (`src/recovery/recovery.c:2027`). The state transitions are `INPROGRESS → COMMITTING → final-CSN`: `COMMITSEQNO_STATUS_IN_PROGRESS=0x0`, `COMMITSEQNO_STATUS_CSN_COMMITTING=0x1` (`src/transam/oxid.c:42-43`); the leader does `set_oxid_csn(..., COMMITSEQNO_COMMITTING)` then `set_oxid_csn(..., final_csn)` (`src/recovery/recovery.c:2028`, `2038`). Readers spin while the COMMITTING bit is set (`src/transam/oxid.c:1733-1735`).

**Workers never assign CSNs.** The worker `RecoveryMsgTypeCommit` branch calls `recovery_finish_current_oxid(COMMITSEQNO_MAX_NORMAL-1, ptr, id>=0, false)` with `sync=false` (`src/recovery/worker.c:584-596`); the `worker_id >= 0` path skips `set_oxid_csn()` entirely (`src/recovery/recovery.c:2041-2060`).

Before touching system trees the leader waits for workers to advance their `commitPtr` via `workers_synchronize()` (`src/recovery/recovery.c:4811-4854`, called at `3962-3964`). The leader-only `xmin_queue` and `runXmin` tracking (`update_run_xmin()`, `src/recovery/recovery.c:2525-2542`) prevents the horizon from regressing below in-flight oxids.

**Where the STANDBY differs:** `waitCallback` is `NULL` during recovery (`nullCallbackInfo`, `src/btree/modify.c:74-81`, used at `src/recovery/recovery.c:2317`), so a worker conflicting on an `INPROGRESS` oxid cannot *block* — it **busy-spins** in `o_btree_modify_handle_conflicts()` (`src/btree/modify.c:566-659`, Assert at `575`, `wait_for_tuple()` at `638-643`) calling `oxid_get_csn()` (`src/transam/oxid.c:1719-1738`). Because the standby never runs `recovery_finish()`, the oxid the primary aborted in memory stays `INPROGRESS` and the spin never ends (`src/recovery/recovery.c:1689-1694`).

### 5.4 Deferred-commit finalization drain and its standby liveness hazard

Non-sync commits during parallel recovery are *deferred*: `recovery_finish_current_oxid()` runs precommit/on-commit undo and `walk_checkpoint_stacks()`, sets `in_finished_list=true`, and `dlist_push_tail()`s the entry with a placeholder CSN (`COMMITSEQNO_MAX_NORMAL-1` for commit, `COMMITSEQNO_ABORTED` for abort) — **no `set_oxid_csn()` yet** (`src/recovery/recovery.c:2041-2059`).

These are finalized lazily, **leader-only**, in the drain loop of `update_proc_retain_undo_location()` (`src/recovery/recovery.c:2636`): `dlist_foreach_modify` over `finished_list` (`2671`), and only `worker_id < 0` runs `set_oxid_csn()` (`2685-2697`); workers merely `dlist_delete()` (`2702`). The drain advances only up to `listPtr = MIN(worker commitPtr)`: the leader reads `listPtr` via `recovery_get_current_ptr()` → `get_workers_commit_ptr()` (`src/recovery/recovery.c:2666-2669`, `1345`), which computes `MIN(worker_ptrs[i].commitPtr)` (`1313-1315`) and breaks the drain when `state->ptr > listPtr` (`2682-2683`). A worker advances its `commitPtr` only while processing leader messages, via `update_worker_ptr()` (`src/recovery/worker.c:348-356`, called on COMMIT/ROLLBACK/SYNCHRONIZE/FINISHED at `592/605/611/614`).

**The standby-specific deadlock:** if a worker spins on a conflicting `INPROGRESS` oxid (§5.3), it stops reading messages, so its `commitPtr` freezes — and that frozen value becomes the `MIN`, i.e. `listPtr` (`src/recovery/recovery.c:1313-1315`). `get_workers_commit_ptr()` even caches the result via a `worker_ptrs_changes` change-counter, so once no `commitPtr` moves, the pinned MIN is returned without recomputation (`src/recovery/recovery.c:1293-1331`). The leader's drain then stalls on the first entry beyond the frozen `listPtr` (`2680-2683`) — including the very oxid the spinning worker needs a CSN for. This is a circular wait: worker-must-advance-commitPtr → leader-can-advance-listPtr → oxid-can-be-finalized → worker-can-unblock (`deferred_commit_mechanism.md:207-225`).

**Where the STANDBY differs:** single-node crash recovery has a backstop — `recovery_finish()` finalizes any leftover `in_finished_list` entries (leader-only, `src/recovery/recovery.c:1743-1748`). A streaming standby never reaches it (`src/recovery/recovery.c:1689-1690`), and no other path finalizes deferred commits, so the deadlock is permanent.

### 5.5 The xmin horizon: identical oxids, divergent globalXmin

Three invariants describe how horizons relate across primary and standby:

1. **Oxid numbers identical.** Both allocate from the same `nextXid` sequence; the standby advances via `advance_oxids()` at `WAL_REC_XID` (`src/recovery/recovery.c:3890`; allocation at `src/transam/oxid.c:1323`).
2. **CSN independently re-assigned.** The replica ignores the WAL-carried csn (calls `recovery_finish_current_oxid` with the `COMMITSEQNO_MAX_NORMAL-1` placeholder, `src/recovery/recovery.c:3980`) and re-assigns its own from `nextCommitSeqNo` during deferred finalization (`src/recovery/recovery.c:2636-2642`; `xid_oxid_csn_in_orioledb.md:199-201`).
3. **xmin carried, `globalXmin` diverges.** WAL xmin feeds `recovery_xmin` (`recovery_xmin = Max(recovery_xmin, rec->u.finish.xmin)`, `src/recovery/recovery.c:3923-3924`; joint-commit at `4001`; initialized from `runXmin` at `1559`), but `globalXmin` is computed from the **local** `xmin_queue` + `recovery_xmin`, never streamed (`src/recovery/recovery.c:2529-2541`; `xid_oxid_csn_in_orioledb.md:220-223`).

**Where the STANDBY differs — backward `globalXmin`.** On the primary, `advance_global_xmin()` is strictly monotonic: it writes only `if (globalXmin > prevGlobalXmin)` (`src/transam/oxid.c:1183`) and asserts `globalXmin >= writeInProgressXmin` and `globalXmin >= writtenXmin` (`src/transam/oxid.c:1197-1198`). On the standby, `update_run_xmin()` (`src/recovery/recovery.c:2525-2569`) and `free_run_xmin()` (`2571-2595`) use the *reversed* guard `if (xmin < prevGlobalXmin)` (`2546`, `2581`) and deliberately write `globalXmin` **backward**, with **no** `writtenXmin` assert. The dangerous case is `xmin < writtenXmin`: it re-exposes the already-frozen `[globalXmin, writtenXmin)` region, where a slot holding `COMMITSEQNO_FROZEN` is mis-resolved as `IN_PROGRESS` by `oxid_get_csn`'s fast-path (which returns FROZEN only when `oxid < globalXmin`, `src/transam/oxid.c:1700-1729`) — the recovery-livelock root-cause candidate (csn-trace at `src/recovery/recovery.c:2550-2566`; `xid_oxid_csn_in_orioledb.md` Appendix A:374, 409). A deferred-rollback guard already exists to avoid one source of this regression: low-oxid deferred rollbacks are dropped from `xmin_queue` *without* calling `update_run_xmin()` (`src/recovery/recovery.c:3916-3947`). `free_run_xmin()` itself is leader-only and only runs at `recovery_finish()` (guard `worker_id < 0`, `src/recovery/recovery.c:1767`), which the standby never reaches.

### 5.6 Eager WAL and the deferred recovery-finish ROLLBACK

OrioleDB's eager-WAL design lets a standby apply each modify marked `COMMITSEQNO_INPROGRESS` *before* the verdict arrives, relying on a later `WAL_REC_COMMIT/ROLLBACK` to resolve it (`src/recovery/wal.c:471-479`). When the primary crashes mid-transaction, the standby has the modifies but no terminator. The restarted primary's `recovery_finish()` aborts these in-flight oxids in memory (iterating `recovery_xid_state_hash`, `COMMITSEQNO_IS_INPROGRESS` check at `src/recovery/recovery.c:1683`, `apply_undo_stack()`/`walk_checkpoint_stacks()` at `1706-1707`) and — only on the main process (`worker_id < 0`, `src/recovery/recovery.c:1717-1740`; "workers don't write WAL" at `1711-1715`) — collects them into `recovery_finish_aborted_oxids`.

Those collected aborts are flushed *after* end-of-redo, once `XLogInsertAllowed()` is true: `o_emit_recovery_finish_rollbacks()` runs from the `after_checkpoint_cleanup_hook` with `flags == 0` (`src/checkpoint/checkpoint.c:1872-1873`) — explicitly **not** from inside `rm_cleanup`, which still has `XLogInsertAllowed()==false` (`src/recovery/wal.c:481-483`). For each oxid it calls `wal_emit_recovery_finish_rollback()` (`src/recovery/wal.c:485-512`), which emits a **two-record** sequence:

- `WAL_REC_XID` carrying `oxid`/`logicalXid`/`heapXid` (`src/recovery/wal.c:494`; `add_xid_wal_record` memcpys at `635-637`),
- `WAL_REC_ROLLBACK` carrying the verdict with **`xmin = InvalidOXid`** (`src/recovery/wal.c:504`; `add_finish_wal_record` memcpys xmin at `571`).

The finish record carries **no oxid field** (`WALRecFinish` is `recType`/`xmin`/`csn` only, `wal.h:159-164`); the parser reuses a single `WalRecord` struct, so `wal_parse_rec_xid` sets `rec->oxid` (`src/recovery/wal_reader.c:71`) and the subsequent `wal_parse_rec_finish` inherits it without re-reading (parses only xmin/csn, `src/recovery/wal_reader.c:87-95`; struct layout `wal_reader.h:25-104`). The `InvalidOXid` xmin (vs. the `runXmin`-at-emit used by the normal `wal_rollback` path, `src/recovery/wal.c:452-453`) signals "this record carries no horizon information," so the standby excludes the oxid from its xmin horizon (`src/recovery/wal.c:496-502`). This bypasses the normal-abort `local_wal.has_material_changes` gate (`src/recovery/wal.c:425-430`) by emitting stand-alone records after end-of-redo.

**Where the STANDBY differs:** without this explicit terminator on the wire, the standby holds the oxid `INPROGRESS` forever and livelocks on the next conflicting modify in `o_btree_modify_handle_conflicts()` — issue #876 (`src/recovery/wal.c:477-479`; `src/recovery/recovery.c:1796-1799`). The `InvalidOXid` xmin is what prevents a *second-order* livelock in the standby's horizon (the §5.5 backward-`globalXmin` problem).

### 5.7 Primary↔standby invariants, the livelock, and the fix

The full livelock (detailed in `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md`) chains §5.5 and §5.6: a deferred-rollback oxid inserted into the `xmin_queue` regresses `globalXmin` below `writtenXmin` (observed `352 → 248 < writtenXmin=352`, `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md §1-§6`; `src/recovery/recovery.c:2546-2566`). That re-exposes a slot still holding `COMMITSEQNO_FROZEN=0x3`, which `oxid_get_csn` — having skipped its `oxid < globalXmin` fast-path — returns as `COMMITSEQNO_INPROGRESS` (`src/transam/oxid.c:1723-1748`; `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md §5`). Workers then spin forever and `listPtr` stays pinned (§5.4).

The fix is two coordinated parts:

1. **`InvalidOXid` sentinel.** `wal_emit_recovery_finish_rollback()` stamps the finish record with `InvalidOXid` instead of `runXmin` (`src/recovery/wal.c:504`, comment `497-502`; `InvalidOXid = 0x7FFFFFFFFFFFFFFF`, `include/orioledb.h:166`), marking it "no horizon information." A guard asserts that commits never carry the sentinel (`src/recovery/recovery.c:3919-3921`).
2. **Deferred-rollback xmin-queue filter.** On replay, `deferred_rollback = (rec->type == WAL_REC_ROLLBACK && !OXidIsValid(rec->u.finish.xmin))` (`src/recovery/recovery.c:3916-3917`). For such records the xmin is **not** folded into `recovery_xmin` (`3923-3924`), and if the oxid is already in the queue it is removed and its `in_xmin_queue` flag cleared (`3933-3947`) — preventing a later `update_run_xmin()` from regressing onto another not-yet-removed deferred-rollback oxid. The `in_xmin_queue` field (`src/recovery/recovery.c:116`; set true at `1926-1927`, checked in `check_delete_xid_state` at `1968-1972`, cleared at `3947`) prevents double-removal heap corruption. The undo path is untouched — only `xmin_queue` membership is suppressed; undo is still applied via `recovery_finish_current_oxid(..., COMMITSEQNO_ABORTED, ...)` → `apply_undo_stack` (`src/recovery/recovery.c:3935-3943`, `3980`, `2066`; `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md §11`).

**Where the STANDBY differs:** these deferred-rollback markers exist *only* in the streaming case — they are emitted by the restarted primary's post-recovery `after_checkpoint_cleanup_hook` (`src/recovery/wal.c:467-483`) specifically because standbys eagerly applied the in-flight modifies; single-node crash recovery resolves the same oxids in memory and never puts a deferred-rollback marker on the wire. Supporting machinery: when the on-disk xidmap is unavailable during recovery, `map_oxid` redirects to `recovery_map_oxid_csn`, which looks the oxid up in the recovery-local `recovery_xid_state_hash` and returns its csn or `COMMITSEQNO_ABORTED` (`src/transam/oxid.c:745-756`; `src/recovery/recovery.c:1490-1502`).

### Caveats / unverified

- **Fix-applied status not confirmed in production code.** The csn-trace logging at `src/recovery/recovery.c:2550-2566` / `2557` / `2585` indicates the backward-`globalXmin` livelock is under *active investigation*; whether the flooring/sentinel fix is fully landed (vs. instrumented) in this tree is not independently verified. (Low confidence.)
- **`[repro-branch trace]` instrumentation.** The comment block documenting the in-memory abort with no WAL (`src/recovery/recovery.c:1686-1695`) is `IS_DEV`-only instrumentation on the `add_stress_bank_account_test` branch, not stable source.
- **External cross-references.** `ISSUE_streaming_standby_recovery_livelock.md` cites external GitHub permalinks/commit hashes that may point to a different source state than the working tree.
- **`pg_basebackup` seed mechanism.** How `pg_basebackup` actually transfers seed values lives in patched PostgreSQL (external to this repo); the orioledb code merely assumes basebackup has completed. `.xidmap` binary layout beyond `OXidMapItem`/the `o_buffers_write` write-side is not shown.
- **`rm_cleanup` invocation timing** is established by code structure (`orioledb.c` `RmgrData` registration) but not traced through PostgreSQL's own source.
- **Standby-side `InvalidOXid` interpretation** (how the standby uses the sentinel to exclude an oxid from its horizon) is inferred from the `src/recovery/wal.c:499-502` comment; the consuming standby code was not directly examined. (Medium confidence — `wal.c` "second-order livelock" reference.)
- **`update_run_xmin()` leader-only execution** is inferred from surrounding `worker_id < 0` guards; the function itself carries no visible `worker_id` guard.
- **Deferred-rollback discriminator shift.** The design doc's `oxid < carried_xmin` discriminator vs. the code's `!OXidIsValid(xmin)` sentinel reflects a narrowing; end-to-end crash-consistency between primary emission and standby undo application is not verified from source alone. The `UNDO_REC_EXISTS` assertion noted in the issue (§10) may indicate a separate retention bug not covered here.
- **`finished_list` `recoveryPtr` threshold** (`src/recovery/recovery.c:2682`) gates CSN assignment by WAL-LSN ordering; its subtler standby implications are not fully explored.

---
