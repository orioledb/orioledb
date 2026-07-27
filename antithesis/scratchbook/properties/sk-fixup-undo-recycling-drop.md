# sk-fixup-undo-recycling-drop

## Merge note

This evidence file merges three independently-written files that converged on
the same finding: `pk-sk-fixup-undo-recycled.md`, `sk-fixup-recycled-undo-drop.md`,
and this file (`sk-fixup-undo-recycling-drop.md`, kept as the canonical slug).
All three focus passes (Failure Recovery, Idempotency and Replay) independently
read `checkpoint_write_pending_sk_fixups()` / `apply_one_pending_sk_fixup()`
and reached the same conclusion; content below is the union of their evidence.

## Focus

Failure Recovery / Idempotency and Replay. Directly targets `sut-analysis.md`
§2's headline finding: the orioledb#855 fix exists but has an acknowledged gap
— the fixup record it writes does not pin the undo location it references, so
replay can find the record already recycled and silently drop it.

## What led to this

`src/checkpoint/checkpoint.c:1004-1074`, `checkpoint_write_pending_sk_fixups()`. Read directly:

```c
xidRec.kind = XidRecPendingSkFixup;
xidRec.retainLocation = InvalidUndoLocation;   /* checkpoint.c:1012 */
...
oxid = oProcData[i].vxids[level].oxid;
if (OXidIsValid(oxid))
{
    xidRec.oxid = oxid;
    xidRec.undoLocation.location = pendingLoc;
    write_to_xids_queue(&xidRec);
}
```

The fixup record captures `pendingLoc` (the PK undo location) but explicitly sets `retainLocation = InvalidUndoLocation` — it makes **no attempt to hold that undo location against reclamation**. Compare with `finish_write_xids()` (`checkpoint.c:902-948`), which for ordinary in-flight-oxid bookkeeping *does* read and persist `oProcData[i].undoRetainLocations[...]` (`checkpoint.c:933`) — i.e., the codebase has a working retain-location mechanism and simply doesn't invoke it here.

On the replay side, `apply_one_pending_sk_fixup()` (`src/recovery/recovery.c:377-419`):

```c
itemLoc = tuphdrLoc - offsetof(BTreeModifyUndoStackItem, tuphdr);

if (!UNDO_REC_EXISTS(UndoLogRegular, itemLoc))
{
    /* recycled in the meantime; nothing we can do */
    elog(DEBUG2,
         "pending-SK fix-up: undo record at %X/%X recycled, skipping",
         (uint32) (itemLoc >> 32), (uint32) itemLoc);
    return;
}
```

This is not a defensive-programming leftover; it is the code explicitly handling the case where the referenced undo record is gone by the time replay tries to use it — and the handling is to silently skip the fixup entirely (`return;`, no error, no retry, only a `DEBUG2` log that is invisible at default log levels).

## Mechanism / what has to happen for this to bite

1. A backend is caught in the PK-applied/SK-pending window (`oProcData[i].pendingSkUndoLoc` set) exactly when `checkpoint_write_pending_sk_fixups()` walks all backends — this is the same window orioledb#855's original bug lived in, and the same window `sk-recovery-race[-chaos]`'s `sk_modify_pending` stopevent pins deterministically.
2. The checkpoint writes a `XidRecPendingSkFixup` record pointing at `pendingLoc`, with no retain-location protection.
3. Before crash recovery reaches `apply_one_pending_sk_fixup()` for that record, the undo region containing `pendingLoc` gets recycled — i.e., undo churn (from ordinary commits/vacuums of other transactions) advances far enough to reclaim that location before replay consumes it.
4. Replay silently drops the SK-side fixup. The PK row exists; its secondary-index row is never inserted (or the corresponding delete never applied) — a genuine PK/SK divergence, i.e. a **new variant of #855's failure mode surviving the #855 fix itself**.

Step 3 is the crux: it requires *delay between checkpoint-write and replay* (a lagging replica doing repeated restartpoints against a busy WAL stream is the natural real-world shape) combined with *enough undo churn* in that delay window to reclaim the specific location. Neither condition is exotic; both are things a chaos workload with checkpoint pressure and a slow/paused recovery process can induce directly.

This is the same *symptom* the existing `sk-recovery-race[-chaos]` harness already checks (`orioledb_tbl_check()` + PK-count == distinct-SK-token-count), but a different *trigger*: the existing harness pins the race with a stopevent and checkpoints immediately, giving replay no time to lag behind undo recycling. This property specifically wants delayed/lagging replay relative to undo churn.

## The property

| | |
|---|---|
| **Type** | Safety |
| **Property** | A PK/SK-fixup record written at checkpoint time for a backend caught in the PK-applied/SK-pending window is always eventually applied during crash recovery — the referenced undo location is never recycled out from under a still-pending fixup, so no PK row is ever left without its corresponding secondary-index entry (or vice versa) after a crash. |
| **Invariant** | `Always`: reuse the existing `sk-recovery-race` oracle — PK-row-count must equal distinct-SK-token-count for `o_sk_pending`, plus `orioledb_tbl_check()` structural consistency (same check as `sk-recovery-race/driver.py:89-95`). The novel angle versus the existing harness is deliberately widening the checkpoint-write-to-replay gap (delayed/paused recovery, or a lagging standby doing several restartpoints) and injecting undo churn from concurrent unrelated DML in that gap, specifically to try to reach the `UNDO_REC_EXISTS(...) == false` branch in `apply_one_pending_sk_fixup()`. |
| **Antithesis Angle** | The existing `sk-recovery-race[-chaos]` workload constructs the checkpoint-time race window but immediately checkpoints+recovers with minimal delay — it does not stretch the window between the fixup being *written* and *replayed*, nor inject heavy undo churn in that window. A variant workload that (a) pins the `sk_modify_pending` stopevent as the existing driver does, (b) checkpoints, (c) then — before letting recovery run — drives a burst of unrelated commits/rollbacks designed to advance the undo horizon past the captured `pendingLoc`, (d) *then* triggers/allows recovery, directly targets this gap. On a standby topology, Antithesis's process-pause fault on the recovery/startup process combined with sustained undo-churning DML on the primary is the natural way to widen the window without hand-crafted timing. |
| **Why It Matters** | This is a silent-corruption class failure — a PK row visible in scans but missing/wrong in a secondary-index lookup, or vice versa — which per `sut-analysis.md` §10 is explicitly the worst-case failure category for a database engine (wrong query results, not a crash). It is also specifically a **surviving variant of a bug (#855) the team already fixed once and built dedicated Antithesis coverage for**, which makes it a high-value regression target per `references/property-catalog.md`'s "Cross-Reference Closed Issues" guidance — the existing harness's own construction may not be reaching this specific edge of the fix. |

**Open Questions:**

- Is there a numeric/practical floor on how much undo churn is needed to recycle a `pendingLoc` written moments earlier at checkpoint time — i.e., is this reachable within a realistic Antithesis run duration, or does it need an unrealistically large burst of unrelated commits? Not measured — would need to read the undo-log recycling/retain-horizon advancement logic (`src/transam/undo.c`) to bound this. `(needs further investigation)`
- Does the existing `sk-recovery-race-chaos` driver's reliance on "chance overlap + Antithesis's own fault injection" (per `existing-assertions.md`) ever incidentally stretch the checkpoint-to-replay gap far enough to exercise this path today, or does its current design (checkpoint immediately after parking backends) structurally avoid it? Not traced against the current driver.py logic line-by-line.
- Does anything else (e.g. `retainLocation` on some *other*, unrelated structure) incidentally protect this specific undo location in the common case, making the gap narrower than the code alone suggests? Not ruled out — only the `checkpoint_write_pending_sk_fixups()` call site's own `retainLocation = InvalidUndoLocation` was examined.
- Is the recycled-skip branch actually reachable under any workload achievable in a reasonable Antithesis run duration, or does normal undo retention policy make the window too narrow in practice? `(needs further investigation)`

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing, but the SUT already logs the exact failure signal at `DEBUG2`.** `existing-assertions.md` confirms the only assertions anywhere are the two `always()`/one `reachable()`/one `sometimes()` in `sk-recovery-race[-chaos]/driver.py`, all checking the *outcome* (PK-count vs SK-distinct-count) after the race, not this specific mechanism. The highest-value SUT-side addition here is turning the existing `elog(DEBUG2, "pending-SK fix-up: undo record at %X/%X recycled, skipping", ...)` at `recovery.c:412-414` into (or pairing it with) an Antithesis `unreachable()`/`reachable()` call: this is precisely the internal state a workload-only check cannot directly observe (the workload only sees the downstream row-count mismatch, not *why* it happened). Turning today's silent DEBUG2 log line into a `reachable()` marker lets a deterministic test assert the recycled-and-dropped branch was actually hit, and lets Antithesis's search prioritize toward widening the checkpoint-to-replay gap.

## Scope note (added by evaluation pass, R15)

This property, like the other undo-retention properties in this catalog
(`sk-fixup-undo-recycling-drop`, `replica-undo-reclaimed-too-early`,
`undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant`),
implicitly exercises only the `enable_rewind=false` branch of shared
undo-retention logic — `orioledb.enable_rewind` is never set to `true`
anywhere in `test/antithesis/`. Flagged here explicitly since it was never
stated before; assessed as low-risk because `enable_rewind` is
`PGC_POSTMASTER` (fixed at server start, not a runtime-mutable/session-level
GUC that could flip mid-test), and rewind is out of this catalog's scope
regardless per the top-of-file scope restriction.
