---
slug: sk-overwrite-callback-identity-dedup
attention_focus: Idempotency and Replay
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# Secondary-index redo dedup is identity-based (oxid match), not content/version-based

## What led to this

The assigned focus centers on the claim at `doc/architecture/overview.mdx:147`
("Since OrioleDB implements fuzzy checkpointing, we require idempotency
property here... if some changes are applied to secondary keys twice or
more, it does not affect the final state") and asks to dig into the actual
overwrite-callback code to assess how strong the claim really is. This
property is the result of that dig: it identifies exactly *what* mechanism
implements the claim, and the specific way it is weaker than the
equivalent primary-index mechanism.

## The mechanism

`apply_btree_modify_record()` (`src/recovery/recovery.c:2333-2407`) picks a
callback pair based on tree type:

```c
else if (tree->type == oIndexPrimary || tree->type == oIndexToast || tree->type == oIndexBridge)
{
    /* ... */
    callbackInfo.modifyCallback = recovery_insert_primary_callback;
    callbackInfo.modifyDeletedCallback = recovery_insert_deleted_primary_callback;
    /* delete branch: recovery_delete_primary_callback / _deleted_primary_callback */
}
else
{
    /* everything else -- i.e. secondary indexes */
    callbackInfo.modifyCallback = recovery_insert_overwrite_callback;
    callbackInfo.modifyDeletedCallback = recovery_insert_deleted_overwrite_callback;
    /* delete branch: recovery_delete_overwrite_callback / _deleted_overwrite_callback */
}
```

The **primary**-index callback (`recovery.c:2190-2217`) compares content,
not just identity:

```c
recovery_insert_primary_callback(...)
{
    if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
        o_tuple_get_version(tup) >= o_tuple_get_version(*newtup))
        return OBTreeCallbackActionUndo;   /* skip: existing is same-or-newer */
    return OBTreeCallbackActionUpdate;     /* apply: existing is older */
}
```

The **secondary-index / "overwrite"** callback
(`recovery.c:2219-2231,2233-2245,2289-2317`) drops the version comparison
entirely:

```c
recovery_insert_overwrite_callback(...)
{
    if (XACT_INFO_OXID_EQ(xactInfo, oxid))
        return OBTreeCallbackActionUndo;   /* skip: same oxid, ANY version */
    return OBTreeCallbackActionUpdate;
}
```

i.e. for secondary indexes, "an entry already exists for this key, written
by this same oxid" is treated as sufficient proof the write is a duplicate
of the current one and can be silently dropped — with no check that the
existing entry's *content* actually matches what the current record would
have written.

This callback is used both for the ordinary post-toast-boundary WAL replay
path and for the synthesized fix-up records from
`apply_one_pending_sk_fixup()` (`recovery.c:600-627`, whose own comment at
line 556-558 makes the intent explicit: "Same shape as apply_tbl_update()
so workers' overwrite callbacks make these idempotent against any later
WAL records" — i.e. the design deliberately relies on this callback to
deduplicate the fixup-synthesized write against a possible later *real*
WAL record for the same logical operation).

## Where this is weaker than it looks

The version-checked primary-index path defends against a specific failure
mode: two different writes competing for the same key, where only the
*newer* one (by version) should win, regardless of arrival order. The
secondary-index path has no equivalent defense: it assumes that whenever it
sees "same oxid, already present," the existing content *is* the correct,
final content for that key — it never verifies this.

This is fine as long as a single (oxid, secondary key) pair can only ever
be the target of one, single, unambiguous logical write during a recovery
pass (in which case identity alone is a correct enough dedup signal). It
stops being provably fine if the *same* secondary key can legitimately
receive two different intended tuple images from the same oxid during one
recovery pass, e.g.:

- The pending-SK fix-up path (`apply_one_pending_sk_fixup`) synthesizes an
  insert using whatever is on the primary-key page *at the moment the
  toast-consistency boundary is crossed* (`recovery.c:490-531`), not
  necessarily the exact tuple image the original (crashed or
  not-yet-replayed) transaction would have written for that specific
  operation. If a subsequent *real* WAL_REC_UPDATE for the same oxid and
  key arrives later in the stream with genuinely different content, the
  overwrite callback will see "same oxid, entry present" and skip it —
  silently keeping the fix-up's reconstruction instead of the real record,
  with no way to tell afterward that the two disagreed.
- More generally, whenever the pending-SK-fixup mechanism and ordinary WAL
  redo can both apply to the same (oxid, key), the code's only overlap
  contract is oxid equality, which is a strictly weaker check than the
  content/version-aware one used for the primary index.

## Why it matters

If the two paths ever *do* diverge in content for the same key, the
divergence is unobservable from a mismatch report — `orioledb_tbl_check()`
(the harness's structural oracle) checks tree structure, not "does this SK
entry's payload match what a from-scratch redo of the transaction would
have produced." A silent, wrong-but-structurally-valid secondary index
entry is exactly the failure mode `doc/architecture/overview.mdx:147`
claims cannot happen.

## Why this is a lead, not a confirmed bug

This pass did not find or construct a concrete input sequence that makes
the fix-up-synthesized tuple diverge from what the real subsequent record
would carry — `apply_one_pending_sk_fixup` reads the PK page *after*
`workers_synchronize()` has drained every WAL record up to the toast
boundary (`recovery.c:1223-1241`), which is designed precisely to keep the
on-page state consistent with "everything applied so far." Whether a
same-oxid, multi-write-to-the-same-row sequence can still produce a
fix-up/real-record mismatch under this synchronization was not traced to a
definitive answer — this is exactly the kind of narrow-window question
Antithesis's search is well-suited to explore, since a human trace through
every interleaving is expensive and error-prone.

## Antithesis angle

A workload that repeatedly updates the same row (changing its secondary-key
value back and forth) inside one transaction, combined with a checkpoint
landing in the PK-applied/SK-pending window on *every* one of those
sub-updates (not just once, unlike the existing `sk-recovery-race` driver
which targets a single race instance), would maximize the chance of
exercising this path multiple times per transaction and increase the odds
of a divergence surfacing.

## Open Questions

- Can a single oxid legitimately produce two different secondary-index
  target values for the exact same PK row inside the pending-SK window
  that `checkpoint_write_pending_sk_fixups()` samples (recall: it samples
  at most one `pendingSkUndoLoc` per backend per checkpoint, per
  `checkpoint.c:1017-1073`)? If not, this property may be unreachable in
  practice and should be downgraded; if so, it is a plausible correctness
  gap in the idempotency claim.
- Does `orioledb_tbl_check()` (or amcheck's `verify_orioledb()`) have any
  check that would catch a structurally-valid-but-semantically-wrong
  secondary index entry (SK value doesn't match what the PK/TOAST content
  implies), or would this class of bug only surface later as a wrong query
  result on an index scan? `(needs human input — requires reading
  amcheck/verify_orioledb's check coverage, out of scope for this pass)`

## Suggested instrumentation

No stopevent currently distinguishes "overwrite callback fired and chose
Undo (skip)" from "chose Update (apply)." Adding a stopevent or lightweight
counter inside `recovery_insert_overwrite_callback`/
`recovery_delete_overwrite_callback` that fires when the skip branch is
taken — with the oxid, tree oids, and (if cheap) a hash of both the
existing and incoming tuple bytes — would let a workload assert
`always(existing_hash == incoming_hash, ...)` whenever a skip happens,
turning "we assume dedup by identity is safe" into a directly checked
invariant instead of an assumption.

### Investigation Log

#### Does `orioledb_tbl_check()` (or amcheck's `verify_orioledb()`) have any check that would catch a structurally-valid-but-semantically-wrong secondary index entry?

- Examined: `orioledb_tbl_check()`'s described role as the harness's structural oracle (per "Why it matters" above); did not read `amcheck`/`verify_orioledb()` source directly.
- Found: `orioledb_tbl_check()` checks tree structure, not whether an SK entry's payload matches what a from-scratch redo of the transaction would have produced.
- Not found: whether amcheck's `verify_orioledb()` has any content-correctness check for this; whether this bug class would only surface later as a wrong query result on an index scan.
- Conclusion: tagged `(needs human input — requires reading amcheck/verify_orioledb's check coverage, out of scope for this pass)`.
