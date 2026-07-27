---
slug: non-modify-wal-record-replay-safety
attention_focus: Idempotency and Replay
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# Structural WAL record types (TRUNCATE, BRIDGE_ERASE, DATABASE_COPY, ...) don't share the row-level idempotency machinery

## What led to this

The assigned focus asked whether the fuzzy-checkpoint idempotency claim
holds "for ALL WAL record types replayed by the custom rmgr, or only some."
`include/recovery/wal_record.h:42-61` (`ORIOLE_WAL_RECORDS` X-macro) lists
19 record types. Only three go through the version/oxid-checked dedup
callbacks examined in `sk-overwrite-callback-identity-dedup.md`:
`WAL_REC_INSERT`, `WAL_REC_UPDATE`, `WAL_REC_DELETE` (`WAL_REC_REINSERT`
decomposes into an insert+delete pair per the header comment at
`wal_record.h:23-25` and also routes through `apply_btree_modify_record`).

The other sixteen — `WAL_REC_XID`, `WAL_REC_COMMIT`, `WAL_REC_ROLLBACK`,
`WAL_REC_RELATION`, `WAL_REC_O_TABLES_META_LOCK`/`_UNLOCK`,
`WAL_REC_SAVEPOINT`, `WAL_REC_ROLLBACK_TO_SAVEPOINT`,
`WAL_REC_JOINT_COMMIT`, `WAL_REC_TRUNCATE`, `WAL_REC_BRIDGE_ERASE`,
`WAL_REC_REPLAY_FEEDBACK`, `WAL_REC_SWITCH_LOGICAL_XID`,
`WAL_REC_RELREPLIDENT`, `WAL_REC_DATABASE_COPY` — each have their own bespoke
replay handling in the large switch in `src/recovery/recovery.c` (dispatch
starting around line 4200+), none of which was checked in this pass for an
equivalent "replay this twice, get the same result" property.

## Why this matters for the "replay from a fixed checkpoint boundary,
   however many times recovery restarts" model

`recovery-worker-redispatch-consistency.md` establishes that a recovery
worker crash triggers a full-instance crash-restart, and replay always
resumes from the *same* checkpoint's `replayStartPtr` on every attempt —
meaning **every** record type between that boundary and the current end of
WAL gets reprocessed on every restart, not just the row-modify records.
The row-modify path has purpose-built dedup callbacks precisely because
this is expected. Two of the structural record types stood out as worth a
closer look than this pass had time for:

- `WAL_REC_TRUNCATE` → `o_truncate_table(rec->u.truncate.oids, true)`
  (`recovery.c:4304-4318`). Truncating an already-truncated table/tree is
  plausibly a natural no-op, but this was not verified against
  `o_truncate_table`'s implementation.
- `WAL_REC_BRIDGE_ERASE` → `replay_erase_bridge_item()` (single-process) or
  dispatched via `worker_send_modify(..., RecoveryMsgTypeBridgeErase, ...)`
  (parallel) (`recovery.c:4339-4363`). Erasing an already-erased bridge
  item could plausibly hit a "not found" assumption somewhere downstream
  if the erase path expects the item to exist; not traced.
- `WAL_REC_DATABASE_COPY` → `handle_movedb(...)` (`recovery.c:4278-4280`).
  `sut-analysis.md` §11 already separately flags an author-acknowledged
  race in `MOVE DATABASE`-style tablespace moves under hot standby
  (`recovery.c:4019`, "XXX there is a race condition here") — replaying
  this record type twice on a crash-during-recovery is a plausible
  amplifier of that already-acknowledged issue, though the two were not
  connected by the original author's comment and this connection is this
  pass's own inference, not a confirmed link.

## Why this is a lead, not a finding

Nothing in this pass demonstrated an actual double-apply failure for any
of these record types — this is a scope gap in the analysis (there wasn't
time to trace all sixteen handlers to the same depth as the row-modify
path), recorded explicitly so it isn't silently dropped. The row-modify
path's dedup logic exists precisely *because* naive re-application isn't
safe for B-tree inserts/deletes; the prior for structural/DDL-shaped
operations (truncate, erase, movedb) is that they are more often naturally
idempotent (delete-if-exists semantics), but that's a plausibility
argument, not a verified one.

## Antithesis angle

A workload that forces recovery to restart multiple times (repeated
targeted crashes of a recovery worker, or a scripted SIGKILL of the startup
process) while DDL operations that emit these record types are in flight —
`TRUNCATE`, index rebuilds that erase bridge items, `ALTER TABLE ... SET
TABLESPACE`/database moves — combined with a structural check
(`orioledb_tbl_check()`/`verify_orioledb()`) afterward, would be a natural
way to probe this without needing to hand-derive the exact failure
condition for each record type first.

## Open Questions

- Is `o_truncate_table()` idempotent when called on a tree it has already
  truncated (e.g. does it check current state before acting, or
  unconditionally assume non-empty state)? `(needs further code reading —
  out of scope for this pass)`
- Does `replay_erase_bridge_item()` (or the parallel `RecoveryMsgTypeBridgeErase`
  path) tolerate being asked to erase an item that is already gone? Same
  scope caveat.
- Is the already-known `MOVE DATABASE` race (`recovery.c:4019`) made worse,
  unaffected, or actually mitigated by repeated replay of the same
  `WAL_REC_DATABASE_COPY` record across recovery restarts? This pass only
  noticed the two comments are near each other conceptually, not that
  they're mechanistically connected — needs a dedicated read of
  `handle_movedb()` to confirm either way. `(needs human input / further
  investigation)`

## Suggested instrumentation

No stopevents currently exist for `WAL_REC_TRUNCATE`, `WAL_REC_BRIDGE_ERASE`,
or `WAL_REC_DATABASE_COPY` replay. If this property is prioritized, the
minimum useful addition is a stopevent per handler (mirroring
`recovery_start`/`checkpoint_*` conventions already in `stopevents.txt`) so
a deterministic test can pin recovery mid-way through one of these records,
force a restart, and assert (`always`) the post-second-replay state matches
the post-first-replay state.

### Investigation Log

#### Is `o_truncate_table()` idempotent when called on a tree it has already truncated?

- Examined: the `WAL_REC_TRUNCATE` dispatch site (`recovery.c:4304-4318`, calling `o_truncate_table(rec->u.truncate.oids, true)`).
- Found: `WAL_REC_TRUNCATE` replay calls `o_truncate_table()` directly; DDL-shaped operations are plausibly more often naturally idempotent (delete-if-exists semantics) than the row-modify path, by general prior.
- Not found: `o_truncate_table()`'s own implementation was not read — whether it checks current state before acting or unconditionally assumes non-empty state is unverified.
- Conclusion: tagged `(needs further code reading — out of scope for this pass)`.

#### Is the already-known `MOVE DATABASE` race (`recovery.c:4019`) made worse, unaffected, or mitigated by repeated replay of the same `WAL_REC_DATABASE_COPY` record across recovery restarts?

- Examined: the `WAL_REC_DATABASE_COPY` dispatch site (`recovery.c:4278-4280`, calling `handle_movedb(...)`), and `sut-analysis.md` §11's separately-flagged author comment at `recovery.c:4019` ("XXX there is a race condition here") for `MOVE DATABASE`-style tablespace moves under hot standby.
- Found: the two issues are conceptually adjacent (both concern `handle_movedb`/database-copy replay), and repeated replay-on-restart is a plausible amplifier of the acknowledged race.
- Not found: `handle_movedb()`'s implementation was not read, so whether repeated replay actually worsens, is unaffected by, or mitigates the race is unconfirmed — the connection is this pass's own inference, not one drawn by the original author's comment.
- Conclusion: tagged `(needs human input / further investigation)` — needs a dedicated read of `handle_movedb()`.
