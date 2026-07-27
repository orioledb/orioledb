# checkpoint-corrupted-tree-silent-skip

## Status

**Confirmed open/unfixed at the analyzed commit `a975c702156cd449e9c0a8db6f8d9bf5bca4537d`.**
This is Task B, item 2, of a follow-up gap-filling pass: a checkpoint that can
silently skip a corrupted tree from sys-tree bookkeeping and still report
success, found via commits `af851ce4`/`d482623e`. Both commits were located
directly in this local repository's object database (no fetch needed):

- `af851ce4c00b9e693cb8e4c76dbfdfe7a751f929` — on branch
  `origin/checkpoint-io-error-fatal`
- `d482623e4f32d30e6b636d7301a331c62b7df553` — on branch
  `origin/checkpoint_avoid_error_loops`

Both carry an **identical commit message and diff shape**, same author
(Pavel Borisov), same timestamp, on two different feature branches whose
underlying blob context differs slightly (`af851ce4`'s base blob is
`c4ed4e7b`, `d482623e`'s is `109341a9`) — apparently the same fix
cherry-picked/rebased onto two different feature-branch bases, neither of
which has been merged. Ancestry confirmed directly, not assumed:

```
git merge-base --is-ancestor af851ce4 a975c702156cd449e9c0a8db6f8d9bf5bca4537d ; echo $?   →  1  (not an ancestor)
git merge-base --is-ancestor d482623e a975c702156cd449e9c0a8db6f8d9bf5bca4537d ; echo $?   →  1  (not an ancestor)
```

Neither fix is present at the analyzed commit — this is a genuinely
**open, unfixed** bug at `a975c702`, independently re-verified against the
current worktree's actual code (not merely trusted from the fix commits'
own framing), per `validating-claims.md`.

## The commit message (verbatim, both commits) — the team's own root-cause account

> FATAL on corrupted page file during checkpoint tree load
>
> When read_page_from_disk fails in evictable_tree_init_meta, the ERROR came
> before the sharedRootInfo was inserted into SYS_TREES_SHARED_ROOT_INFO. On
> the next checkpoint cycle o_find_shared_root_info returns NULL,
> tree_is_under_checkpoint returned true (the checkpointer matches its own
> state), and o_btree_load_shmem_internal gave up, so the corrupted tree was
> silently skipped and the checkpoint succeeded.

## The mechanism, independently re-traced against current `a975c702` code (not just the commit message)

1. **`evictable_tree_init_meta()`** (`src/checkpoint/checkpoint.c`, around
   lines 5624-5641) calls `read_page_from_disk()` to load a tree's root page
   during checkpoint-time tree initialization. On failure it raises
   `ereport(ERROR, ...)` — **still `ERROR`, not `FATAL`**, confirmed present
   today at both failure sub-branches: a checksum failure
   (`errmsg("invalid rootPageBlkno page in %s", ...)`, line ~5632) and a
   generic I/O failure
   (`errmsg("could not read rootPageBlkno page from %s: %m", ...)`, line
   ~5638). An `ERROR` unwinds only the current subtransaction/statement — it
   does **not** stop the checkpoint process, and crucially, it fires *before*
   this tree's entry gets (re-)inserted into `SYS_TREES_SHARED_ROOT_INFO`.
2. **`o_btree_load_shmem_internal()`** (`src/tableam/descr.c`, function
   starts around line 363) is the function that would normally (re-)insert
   that sys-tree entry. Near the top (around line 393-404):
   ```c
   sharedRootInfo = o_find_shared_root_info(&key);
   if (sharedRootInfo == NULL)
   {
       /*
        * Deletion from SYS_TREES_SHARED_ROOT_INFO comes before applying undo
        * records to SYS_TREES_O_INDICES.  So, this situation is possible in
        * checkpointer due to concurrent deletion.  Just give up then.
        */
       if (checkpoint && tree_is_under_checkpoint(desc))
           return false;
       ...
   ```
   On the checkpoint cycle *after* the failed load above, `sharedRootInfo` is
   `NULL` (it was never inserted, because the `ERROR` fired first) and
   `tree_is_under_checkpoint(desc)` is `true` (the checkpointer's own
   bookkeeping believes it owns this tree's checkpoint), so this function
   returns `false` **silently** — the comment's own rationale ("possible ...
   due to concurrent deletion") is explicitly for a *different*, benign
   scenario (a tree legitimately dropped mid-checkpoint), but the code cannot
   distinguish that benign case from "this tree failed to load due to
   corruption and never got its sys-tree entry (re-)inserted."
3. **The `false` return is swallowed identically to the benign case, all the
   way up:**
   - `o_btree_load_shmem_checkpoint()` (`descr.c:548-551`) is a thin
     pass-through: `return o_btree_load_shmem_internal(desc, true);`
   - `perform_writeback_and_relock()` (`checkpoint.c`, around lines 566-614)
     — two call sites (one around line 601, inline in the function shown
     above; the general shape recurs elsewhere in the same function) — on a
     `false` return, releases the `AccessShareLock`/`oTablesMetaLock` it
     holds and returns `NULL`, with no error, warning, or bookkeeping update
     of any kind.
   - `checkpoint_btree()` (`checkpoint.c`, around lines 1759-1792 and again
     around 3056+) treats a `NULL` descriptor from
     `perform_writeback_and_relock()` exactly like "lock already released by
     ordinary concurrent deletion" — the checkpoint loop simply continues to
     the next tree. **No error counter is bumped, no entry is withheld from
     any structure that would flag the gap, and no non-zero/failure status
     propagates upward to whatever ultimately decides checkpoint success.**

**Net effect:** a single corrupted root page, once it triggers this sequence
once, causes the affected tree to be **silently and permanently excluded**
from that and every subsequent checkpoint's sys-tree bookkeeping — the
checkpoint as a whole completes and is reported as successful (no distinct
error, no log line beyond the original, one-time `ERROR` from the *first*
failed load) while this specific tree's on-disk state is never actually
checkpointed again. The corruption itself doesn't become invisible forever —
a query against the affected table would still hit `read_page_from_disk()`
and surface an error at query time — but the *checkpoint's own* success
reporting gives no indication that one of its trees was silently dropped
from its bookkeeping, which is exactly the "still report success" framing in
the commit message.

## The fix's own regression test (confirms the mechanism end-to-end; not present at `a975c702`)

Both fix commits add `test_checkpoint_fatal_on_corrupted_tree` to
`test/t/file_operations_test.py` (~58 new lines): create a table, checkpoint,
force eviction (`orioledb_evict_pages(...)`), then truncate every non-`.map`
file for that table's `datoid` directory to zero bytes on disk (directly
simulating on-disk corruption/truncation), then issue `CHECKPOINT`. The
test's own comment: *"Without the fix the checkpointer gets ERROR and either
hits an Assert (debug builds) or silently skips the corrupted tree on
subsequent checkpoints (release builds). With the fix it FATALs immediately
and shuts down the cluster cleanly."* The test asserts the node reaches
`NodeStatus.Stopped` within 10 seconds and that the log contains "could not
read rootPageBlkno page from" **without** a `TRAP` string (i.e., a clean
`FATAL` shutdown, not an `Assert` crash) — confirming the fix's actual
remedy is to escalate `ERROR` → `FATAL` at the point of failure (stopping the
checkpoint/cluster loudly and immediately) rather than trying to make the
sys-tree bookkeeping itself more robust to the missing-entry case.

## Property

| | |
|---|---|
| **Type** | Safety |
| **Property** | A checkpoint never completes and reports success while having silently excluded a tree from `SYS_TREES_SHARED_ROOT_INFO` bookkeeping due to an on-disk read failure (checksum mismatch or I/O error) during that tree's root-page load — either the checkpoint fails loudly and attributably (the fix's chosen remedy: escalate to `FATAL`), or the affected tree's exclusion is itself recorded somewhere a monitoring/verification pass could detect it, but the exclusion is never indistinguishable from "this tree was legitimately, benignly dropped mid-checkpoint by a concurrent `DROP`/`TRUNCATE`." |
| **Invariant** | `Unreachable("checkpoint completes with a tree silently missing from SYS_TREES_SHARED_ROOT_INFO due to a load failure, indistinguishable from benign concurrent deletion")` — as implemented today this state **is** reachable (confirmed above), so the practical near-term assertion is `Always(checkpoint_failure_surfaces_loudly_or_is_recorded)`: after deliberately corrupting an on-disk root page (truncate/bit-flip a data file, as the fix's own test does) and forcing a checkpoint, assert that either the process terminates with a clear `FATAL`/corruption-attributed message (today's absent, soon-to-land remedy) or that a subsequent structural check (`orioledb_tbl_check()`/`verify_orioledb()`) flags the affected tree as inconsistent — today, neither holds: the process continues (`ERROR` only, not `FATAL`) and the exclusion is not distinguishable from benign concurrent deletion by any existing check. |
| **Antithesis Angle** | Direct disk-level fault injection (bit-flip or zero-fill a B-tree data file's root page while it's on disk but not yet buffer-resident) timed to land just before a `CHECKPOINT` needs to load that tree's root, repeated across multiple checkpoint cycles — this doesn't require a replication topology, unlike most of this pass's other findings; it's directly reachable on the existing single-node harness with `orioledb_checksums_enabled` at its default (`true`, confirmed never overridden in `test/antithesis/`, per the same reachability note already established for `disk-leaf-header-read-before-validation`). A workload that periodically forces `CHECKPOINT` and, separately, periodically checks whether every table it created is still present in and consistent with `orioledb_tbl_check()`/`verify_orioledb()`'s view, would surface the gap: the table would still exist and be queryable-until-hit, but a checkpoint-cycle-scoped structural check might never flag "this tree stopped being checkpointed N cycles ago." |
| **Why It Matters** | This is a **silent, permanent loss of checkpoint coverage** for a corrupted tree, masked as checkpoint success — precisely the "wrong query results or lost writes" failure class `sut-analysis.md` §10 calls the worst case for a database engine, here specialized to "a whole tree's crash-consistency guarantee silently degrades and nothing says so." Compounding this: `perform_writeback_and_relock()`'s `false`-return handling conflates two semantically opposite situations (harmless concurrent `DROP`, vs. genuine on-disk corruption) under one code path — exactly the kind of "defensive code that accidentally swallows a real error" pattern the codebase's own bug history shows recurring around `orioledb_tbl_check()`'s own historical instability (`sut-analysis.md` §8, `tbl-check-oracle-transient-false-negative.md`). |

**Open Questions:**

- Does `orioledb_tbl_check()`/`verify_orioledb()`, run independently of a
  checkpoint, actually detect that a tree's `SYS_TREES_SHARED_ROOT_INFO` entry
  is stale/missing relative to its last-known-good checkpoint number — i.e.,
  is there *any* existing oracle that would catch this today, even without
  the fix, given enough elapsed checkpoint cycles? Not traced in this pass;
  determines whether this property needs new instrumentation or can reuse
  the existing structural-check oracle already relied on elsewhere in this
  catalog. `(needs further investigation)`
- Is `af851ce4`/`d482623e`'s chosen remedy (escalate to `FATAL`, i.e., "fail
  loudly by crashing the instance") actually the intended long-term fix, or
  a stopgap while a more surgical fix (making the sys-tree bookkeeping itself
  correctly distinguish "corrupted, never inserted" from "benignly dropped
  concurrently") is still pending? The two branch names
  (`checkpoint-io-error-fatal`, `checkpoint_avoid_error_loops`) suggest the
  team was actively iterating on this exact tradeoff across (at least) two
  parallel attempts — worth checking whether either branch has since been
  superseded by a third, more complete approach not found in this pass's
  commit-hash-directed search. `(needs human input — this pass located only
  the two named commits, not a broader survey of either branch's full commit
  history or whether a newer iteration exists)`
- Since the fix's own chosen remedy is `FATAL` (crash the whole instance),
  does that reintroduce the same "crash-loop if the underlying corruption is
  persistent and re-checkpointed on every restart" risk `sut-analysis.md` §6
  already documents for the S3 worker/checkpoint-hang paths — i.e., would a
  genuinely corrupted, unrecoverable root page on disk cause the instance to
  `FATAL`-and-restart-and-immediately-`FATAL`-again on every subsequent
  checkpoint attempt, rather than settling into a stable (if degraded)
  state? Not traced — the fix's own test only checks the *first* FATAL/clean
  shutdown, not repeated-restart behavior. `(needs further investigation)`

## SUT-side instrumentation

`existing-assertions.md` confirms zero assertions in `src/checkpoint/`. The
dangerous state here — a tree silently excluded from
`SYS_TREES_SHARED_ROOT_INFO` bookkeeping due to a load failure rather than a
benign concurrent drop — is not distinguishable from the outside today.
Suggested: a `Reachable`/counter marker at the `if (checkpoint &&
tree_is_under_checkpoint(desc)) return false;` branch in
`o_btree_load_shmem_internal()` (`descr.c`, ~line 403), tagged with whether a
prior `ERROR` was actually raised for this same tree/checkpoint cycle
(requiring a small addition: recording "did `evictable_tree_init_meta()`
just fail for this tree" somewhere the `descr.c` code can check) — this
would let the two currently-conflated cases (real corruption vs. benign
concurrent deletion) be told apart by instrumentation even before either
fix branch's structural remedy lands.

## Investigation Log

#### Is the bug mechanism actually present and reachable in the current worktree, or only asserted by the fix commits' own messages?

- Examined: current worktree (checked out at `a975c702`) —
  `src/checkpoint/checkpoint.c` (`evictable_tree_init_meta`,
  `perform_writeback_and_relock`, `checkpoint_btree`),
  `src/tableam/descr.c` (`o_btree_load_shmem_internal`,
  `o_btree_load_shmem_checkpoint`).
- Found: every step of the commit message's own root-cause account is
  independently confirmed by reading the current code directly — the
  `ERROR` (not `FATAL`) at both `read_page_from_disk()` failure branches in
  `evictable_tree_init_meta()`, the exact `if (checkpoint &&
  tree_is_under_checkpoint(desc)) return false;` early-give-up in
  `o_btree_load_shmem_internal()`, and the silent-`NULL`-propagation chain
  through `perform_writeback_and_relock()`/`checkpoint_btree()` with no
  error counter or bookkeeping signal anywhere in that chain.
- Not found: any existing test, assertion, or structural-check path in the
  current (`a975c702`) codebase that would catch this gap absent the fix —
  confirmed by reading `test/t/file_operations_test.py`'s current (pre-fix)
  content, which lacks the new `test_checkpoint_fatal_on_corrupted_tree`
  method entirely at this commit.
- Conclusion: **confirmed open, not a stale/already-fixed claim** — this is
  a real, currently-reachable defect in the analyzed commit, independently
  re-derived from the code, not merely accepted from the fix branches'
  framing.
