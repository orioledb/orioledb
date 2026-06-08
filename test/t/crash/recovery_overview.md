# OrioleDB recovery — high-level overview

A condensed reference for the recovery machinery. Whenever you need the
full picture, jump to the matching section in
[`recovery_investigation.md`](./recovery_investigation.md) — section
numbers below match its TOC.

## The 30-second picture

```
   crash / SIGKILL / clean shutdown
            │
            ▼
   ┌────────────────────────────────┐
   │  PG postmaster starts up       │
   │  (vanilla path)                │
   │  reads pg_control → sees that  │
   │  recovery is needed            │
   └─────────────┬──────────────────┘
                 │
                 ▼
   ┌────────────────────────────────┐
   │  StartupXLOG (PG)              │
   │  • picks redo start LSN from   │
   │    last checkpoint             │
   │  • loops over WAL records      │
   │  • per-record dispatch:        │
   │    RmgrTable[rmid].rm_redo(r)  │
   └─────────────┬──────────────────┘
                 │ rmgr=129
                 ▼
   ┌────────────────────────────────┐
   │  orioledb_redo                 │
   │  (registered custom RM)        │
   │  switches on WAL_REC_*:        │
   │   - INSERT/UPDATE/DELETE       │
   │   - XID / COMMIT / ROLLBACK    │
   │   - RELATION / SYS_TREE / ...  │
   └─────────────┬──────────────────┘
                 │
                 ▼
   ┌────────────────────────────────┐
   │  apply_modify_record           │
   │  • mutates PK btree page       │
   │  • cascades into every SK      │
   │    (delta reconstructed from   │
   │    the same record)            │
   │  • updates per-oxid CSN / undo │
   └─────────────┬──────────────────┘
                 │
                 ▼
   ┌────────────────────────────────┐
   │  Per-tree consistency point    │
   │  reached → opens to clients    │
   │  toastConsistentPtr / sysTrees │
   │  StartPtr / replayStartPtr     │
   └────────────────────────────────┘
```

## Three layers, three places to read

Recovery is fundamentally three layers stacked on top of each other:

| Layer | Owned by | Read first | Then deep dive |
|---|---|---|---|
| 1. WAL framing, control file, redo loop dispatch | vanilla PG | §1 *PG baseline* | §1.2 Control File, §1.5 StartupXLOG |
| 2. Hooking PG's redo loop into orioledb | patches17_6 + orioledb's `_PG_init` | §2 *PG ↔ OrioleDB interface boundary* | §2 H2 "Custom Resource Manager", §2 H2 "WAL replay dispatch" |
| 3. What orioledb does to its own btrees once dispatched | orioledb | §3 *OrioleDB recovery internals* | §3.4 redo loop, §3.5 PK/SK fanout, §3.7 oxid/CSN/undo |

## How a WAL record reaches orioledb

PG's redo loop in `xlogrecovery.c` is one big switch on the resource
manager id (`RmgrTable[rmid].rm_redo`). OrioleDB registers itself as a
**custom resource manager** at id **129** during `_PG_init` (one line,
`RegisterCustomRmgr`), so the moment PG sees a record tagged "129" it
calls `orioledb_redo`. Full registration chain and the patches17_6
hooks that enable it are in §2 H2 "Custom Resource Manager" and §2 H2
"patches17_6 hooks".

The TableAM interface (`orioledb_am_methods` in
`src/tableam/handler.c:2480`, returned from `orioledb_tableam_handler`
at line 2553) is **producer-only** — its callbacks are how regular
SQL statements reach orioledb during normal operation, but they are
never invoked during WAL replay. Recovery only goes through the rmgr
path. The orioledb-side planner integration (`o_scan_methods` custom-
scan path in `src/tableam/scan.c:98`) is similarly inactive during
redo. This is a common source of confusion; §2 H2 "TableAM" makes
it explicit.

## Row-level WAL, not page images

PostgreSQL's heap WAL records mostly include page-image (full-page-write)
data for torn-page safety. OrioleDB's WAL is **row-level** — each
record carries the PK key bytes + new tuple + undo location + oxid +
CSN. Redo reconstructs the per-page mutation by **looking up the PK
key, walking to the leaf, and applying the change in place** — and
the same record drives **every secondary index** the row participates
in (one WAL record → 1 PK update + N SK updates). This asymmetry is
detailed in §3.3 "WAL record taxonomy" and §3.5 "the SK fanout".

Practical consequence: if an UPDATE crosses the kill window, the SK
update may have produced different in-memory state on the primary vs
the rebuilt state in recovery — even though both follow the same
WAL stream. This is the surface the active SK-leak / replication-
divergence investigations sit on.

## CoW checkpoint + per-tree consistency points

Each btree has its own checkpoint metadata file (`.map`) that pins
the data file's reachable extent set. Recovery resumes from
`replayStartPtr` (per-tree), then applies WAL records up to PG's
current LSN. The trick is that **the consistent point is per-tree**:
the PK and each SK have their own checkpoint progress, and redo can
selectively re-apply records to a tree whose checkpoint lagged. This
is the toastConsistentPtr / sysTreesStartPtr / PendingSkFixup
machinery covered in §3.2 "CoW checkpoint architecture" and §3.9
"TOAST and the toastConsistentPtr boundary".

## Idempotence: how redo avoids double-application

PG protects against re-applying records via per-page LSN checks. OrioleDB
has a four-layer protection ladder that does the equivalent without
storing a PG-style LSN on every page:

1. Checkpoint metadata pins the file extents that survived
2. Per-tree replayStartPtr advances past records older than the
   tree's last checkpoint
3. Per-page change counter / page version
4. The redo handler itself is no-op-safe for repeated WAL_REC_INSERT
   if the key already exists

Full walk-through, with citations into `src/recovery/recovery.c`
and `src/btree/io.c`, in §3.6 "PK/SK page-LSN checks".

## Recovery state: oxid, CSN, undo

OrioleDB's MVCC runs on its own xid space (`oxid`) and Commit
Sequence Numbers (CSN) rather than the PG xid + clog model. Recovery
rebuilds:

- the `oxid → CSN` map (so visibility can be reconstructed for
  in-flight tx that survived a crash)
- the per-oxid undo log positions (so an aborted tx's row mutations
  can be unwound)

This is the most failure-rich surface in orioledb recovery (see §3.7
"Recovery transaction state" — covers `recovery_switch_to_oxid`,
`recovery_finish_current_oxid`, and the cleanup paths flagged in the
SK-leak investigation).

## End-of-recovery handshake

After WAL replay reaches PG's stop point, two things must happen
before the cluster opens to user connections:

1. PG runs its end-of-recovery checkpoint
2. OrioleDB runs its `PendingSkFixup` pass (apply pending SK fix-ups
   collected during redo where the SK's consistent point lagged
   the PK's) — see §3.9 and §3.10

Only then does `database system is ready to accept connections` go
to the log.

## What's *not* in this overview

This document is intentionally shallow. Anything below the surface —
specific file/line citations, the call graph between functions,
worker-shard sharding details, the per-record WAL field layouts,
edge cases like mid-commit crash or torn-tail records — is in
`recovery_investigation.md`. The 10 edge-case scenarios in §4
("WAL-stream edge cases") are particularly useful when chasing a
specific reproduction.

## Companion docs

- [`recovery_investigation.md`](./recovery_investigation.md) — the
  4-topic deep-dive this overview summarises.
- [`tx_flow.md`](./tx_flow.md) — annotated control-flow narrative of
  COMMIT / ABORT / UPDATE on the *primary* side. Useful to read
  alongside §3.4 (`orioledb_redo`) to compare the runtime path with
  the redo path.
- [`extent_leak_issue.md`](./extent_leak_issue.md) — active
  investigation that sits on top of §3.9 (TOAST / consistent-point
  boundary) and the recovery cleanup paths in §3.10.
- [`checkpoint_investigation.md`](./checkpoint_investigation.md) —
  the checkpoint side of the same flow.
