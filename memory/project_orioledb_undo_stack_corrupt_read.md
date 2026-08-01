---
name: project_orioledb_undo_stack_corrupt_read
description: ci_fixes churn bug — corrupted read from the transam undo stack (bad type / bad itemSize) during abort/rollback; instrumentation live
metadata:
  type: project
---

Distinct from [[project_orioledb_recovery_undo_read_assert]] (that one is btree/undo.c `get_prev_leaf_header_and_tuple_from_undo`). This is the **transam** undo-stack walk in `src/transam/undo.c`.

**Run:** 30669172451 job 91283085237 (amd64/18/clang, pg_tests 15), branch ci_fixes @ 88825095.

**Signature (oxid=9800, abort/rollback path, two asserts):**
- `item_type_get_descr(type=1051323404)` — Assert `(int)type>=1 && <=N` — via `walk_undo_range` (undo.c:1308) ← `walk_undo_range_with_buf` ← `walk_undo_stack` ← `rollback_to_savepoint(UndoStackFull, parentSubid=13)` ← `undo_subxact_callback(SUBXACT_EVENT_ABORT_SUB, mySubid=15)`.
- `Assert(itemSize >= sizeof(UndoStackItem))` at `undo_item_buf_read_item` (undo.c:1251) ← `apply_undo_branches(oxid=9800)`, `location=864691128455146000` (=0x0C00000002D2CE10).

**Mechanism (working theory):** a link field (`item->prev` on abort, or `onCommitLocation`, or the branch chain `branchLocation`/`prevBranchLocation`/`longPathLocation`) points to a location that PASSES both `UndoLocationIsValid` (bit 61 = `0x2000000000000000` clear) and `UNDO_REC_EXISTS` (>= minProcRetain), i.e. **inside the retained undo range**, but the bytes there are garbage/stale. So it's not an out-of-range pointer — it's a wrong-but-plausible location, or content overwritten under the retain window. `UndoStackItem` = {UndoLocation prev; uint16 itemSize; uint8 type; uint8 indexType;} (16 bytes).

**Instrumentation live (ci_fixes c0d5c7c4, TEMP):**
- `undo_item_buf_read_item`: on `itemSize < sizeof(UndoStackItem)` → `elog(PANIC, "UNDOCORRUPT undo_item_buf_read_item bad itemSize: ... location= itemSize= type= indexType= prev= minProcRetain= chkpRetainStart= chkpRetainEnd= pid=")` before the bare Assert.
- `walk_undo_range`: 16-deep ring of last items (`loc/type/size/idx/prev`); on out-of-range `item->type` → `elog(PANIC, "UNDOCORRUPT walk_undo_range bad type: ... chain(oldest..newest): [k]loc=,type=,size=,idx=,prev= ...")`. The chain reveals the last GOOD item and its corrupt link → source of the bad location.

**Next:** grep next churn hit for `UNDOCORRUPT`. If the chain's newest-good item's `prev` == the bad location, the link write is the bug (who wrote it); if the location is sane but content garbage, it's a retain-window / overwrite bug (undo space reused too early). Cross-check the bad location against the retain bounds in the dump.
