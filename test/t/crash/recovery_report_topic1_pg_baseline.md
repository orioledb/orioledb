# Topic 1: Vanilla PostgreSQL Crash Recovery — Baseline Reference

> Scope: the stock PostgreSQL 17 (patched, branch `patches17_6`) crash-recovery
> machinery, with **no** OrioleDB layer in the picture. Topic 2 covers the
> PG <-> custom-AM interface during recovery; Topic 3 is the OrioleDB-specific
> deep-dive; Topic 4 is WAL-sequence edge cases. This document is the floor
> the others build on.
>
> All source references point at `/home/user/work/postgres`, which is the
> patched PG 17 tree at branch `patches17_6`. File paths are written relative
> to that root.

## 1. The Big Picture

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

## 2. The Control File (`pg_control`)

The control file lives at `$PGDATA/global/pg_control`. The path is hardcoded
as `XLOG_CONTROL_FILE` (`src/include/access/xlog_internal.h:150`). It is the
single authoritative on-disk record of "where the cluster left off." Every
backend that runs `pg_controldata`, every replica that wants to know
`system_identifier`, and every startup process that needs to decide where to
begin redo reads this file.

### 2.1 What it stores

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

### 2.2 The `DBState` enum

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

### 2.3 Atomic update discipline

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

### 2.4 The Redo pointer

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

## 3. Checkpoints

A checkpoint is a synchronization point: by the time it completes, every
dirty buffer that existed when the checkpoint started has been flushed to
disk, and `pg_control` has been updated to point at it.

### 3.1 Full checkpoints vs restart points

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

### 3.2 The `CheckPoint` record payload

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

### 3.3 The two-phase checkpoint protocol

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

## 4. WAL Record Structure

### 4.1 `XLogRecord` header

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

### 4.2 LSN as a position

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

### 4.3 Written / flushed / inserted distinctions

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

### 4.4 Per-page header and segment files

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

### 4.5 Resource managers and the `RmgrTable`

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

## 5. The Startup Process and `StartupXLOG`

### 5.1 Entry point and high-level flow

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

### 5.2 Picking the recovery starting LSN

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

### 5.3 `PerformWalRecovery` — the redo loop

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

### 5.4 `XLogReadRecord` — how a record is decoded

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

### 5.5 Per-rmgr dispatch

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

## 6. Page-LSN Idempotence

The redo loop is **idempotent** at the page level. The same WAL records
can be replayed multiple times (e.g. crash during recovery -> replay from
scratch again) without corrupting data. The mechanism is dead simple: every
heap/index page on disk carries the LSN of the last WAL record that
modified it, and redo skips a record whose `EndRecPtr` is `<=` the on-disk
LSN.

### 6.1 The LSN-on-page rule

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

### 6.2 How `XLogReadBufferForRedo` enforces it

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

### 6.3 Why this lets redo be re-run

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

## 7. `minRecoveryPoint`, Full-Page Writes, Backup Blocks

### 7.1 `minRecoveryPoint`

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

### 7.2 Full-page writes (`XLOG_FPI`)

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

### 7.3 Backup blocks attached to ordinary records

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

## 8. Transition from Recovery to Production

### 8.1 End-of-recovery checkpoint

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

### 8.2 `pg_control` state flip

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

### 8.3 Opening for connections

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

## 9. Sequence Diagrams

### 9.1 Crash + recovery cycle (clean view)

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

### 9.2 Per-record redo dispatch

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

### 9.3 pg_control state diagram

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

## 10. Cross-References for the Other Topics

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

