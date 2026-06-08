# Issue — orioledb btree extent leak after unclean shutdown

## Summary

`kill -9` of the postmaster (unclean shutdown), followed by automatic crash recovery and continued write workload, leaks a single `ORIOLEDB_BLCKSZ` block from the **secondary unique index**'s data file (`o_bank_account_token_uniq`, *not* the PK). The leaked block exists on disk, the in-memory tree does not reference it (so it is not "busy"), and the `.map` free-extent list does not list it either (so it is not "free"). The SK page allocator has lost track of a block.

`orioledb_tbl_check('o_bank_account')` reports the leak as the pair

```
NOTICE:  Extent X 1 is neither free or busy
NOTICE:  Corrupted index name = o_bank_account_token_uniq
```

emitted from `check_extents()` in `src/btree/check.c:403` and from the loop in `orioledb_tbl_check()` at `src/tableam/func.c:1281`. `X` is the offset (in blocks) where the orphaned extent starts; in every observed run `X ∈ {2, 3, 4}` and the length is always `1` — a single 8 KB block low in the SK data file. The PK btree (`o_bank_account_pkey`) is **always** clean in the same run, even when the SK leaks.

## Observable state

In every bug trial the user-visible state checks come back clean:

- `sum(balance) == 100 000`
- `count(*) == count(DISTINCT id) == count(DISTINCT token) == 100`

So no row has been lost or duplicated, the tree walk succeeds. The corruption is exclusively in the page-allocator's bookkeeping (busy ∪ free ≠ entire data file).


## Query plan violation

In the same trials, the hunt's plan-check (`run_hunt.sh::verify_explain_plans`) reports a violation on the diagnostic query `SELECT count(*) FROM o_bank_account` when run with `enable_seqscan=off, enable_bitmapscan=off, enable_indexscan=off` (intended to force an SK index-only scan):

```
[explain sk-forced count(*)] Aggregate
[explain sk-forced count(*)]   ->  Custom Scan (o_scan) on o_bank_account
[explain sk-forced count(*)]         Forward index only scan of: o_bank_account_pkey
[plan-check trial=N FAIL] [explain sk-forced count(*)] did NOT use o_bank_account_token_uniq
```

The planner picks an `o_scan` custom path over the PK heap (which is index-organized, so it can serve `count(*)` directly) instead of the expected SK Index-Only-Scan on `o_bank_account_token_uniq`.

**This is correlated with the kill+recovery cycle but *not* with the
FSM leak itself.** 

The same plan-FAIL fingerprint appears in both clean trials and bug trials at the kill-enabled configs — i.e. plan-FAIL fires on trials that do NOT have an FSM leak. The two phenomena are therefore **independent symptoms** of the same upstream event (SIGKILL + recovery), not cause-and-effect.


## Reproduction

Branch: `add_stress_bank_account_test`. Patched PG 17 must be built with `IS_DEV=1`. From `/home/user/work/orioledb`:

```
source /home/user/work/venv/bin/activate
RR_STORAGE_ENGINE=orioledb \
RR_WRITERS=20 RR_READERS_PK=0 RR_READERS_SK=0 RR_READERS_MIXED=0 \
RR_DURATION=120 RR_INJECTION_POINTS=NONE RR_ASSERT_FIRINGS=0 \
RR_KILL_POSTMASTER=1 RR_KILL_POSTMASTER_INTERVAL=8 RR_PANIC_FATAL=0 \
TIMEOUT=180 ./test/t/crash/run_hunt.sh 30 deep_kill
```

Workload: 20 writers running a pocket-free 2-row token swap under `REPEATABLE READ` against the `o_bank_account(id PK, balance, token UNIQUE DEFERRABLE INITIALLY DEFERRED) USING orioledb` table. The `postmaster_kill_loop` worker `kill -9`s the postmaster every 8 s (3 kills per 30 s trial) and restarts the cluster.

## Fix attempts on `check_walk_btree` (and why none of them moved the bug rate)

Three independently-shaped patches were applied to `src/btree/check.c::check_walk_btree`, all targeting the working hypothesis that the orphan extent is a **phase-1 split right page that the top-down downlink walk can't see** (the right half is allocated and rightlink'd by the splitter, but its parent downlink hasn't been installed yet, so a concurrent `orioledb_tbl_check()` misses it in the busy set and `check_extents()` reports it as leaked).

### Background — split protocol

For a left page `L` splitting to a new right `R`, with parent `P`:

1. `perform_page_split` allocates `R`, fills it, sets `L.rightLink → R`. `btree_register_inprogress_split(R)` records it on the splitter backend's per-backend list (page_state.c:1189) — **no flag is set on R itself**.
2. Splitter walks up, installs `R`'s downlink in `P`.
3. `btree_split_mark_finished(R, success=true)` clears `L.rightLink` (page_state.c:1296-1301).

`O_BTREE_FLAG_BROKEN_SPLIT` is **only** set on the cleanup path (`btree_mark_incomplete_splits` → `btree_split_mark_finished(R, success=false)` at page_state.c:1304-1305), i.e. when the splitting xact aborts before step 2 completes. In the normal in-flight window between step 1 and step 2, `R` has **no flag** — it's reachable from the tree only via `L.rightLink`, and no marker distinguishes "live phase-1 in progress" from "regular tree page".

### v0 — upstream cherry-pick `d2a2723e` ("Don't misreport phase-1 splits as leaked extents")

After the downlink loop, if `header->rightLink` is set and the target page has `BROKEN_SPLIT` → recurse into it as if it were a downlink child so its extent joins `status->busy`. Covers only the abandoned-cleanup state (R already marked broken). Does **not** cover the live phase-1 window, because in that window `R` has no flag at all.

### v1 — `4da80ba1` ("Resolve child's rightlink under parent lock")

Restructure `check_walk_btree` to return the current page's rightlink target (or Invalid) to its caller. In the downlink-iteration loop, carry the previous child's returned rightlink in `childRightlinkBlkno`; before recursing into the next downlink, if it isn't equal to the next downlink's target, walk it via rightlink. Drops the `BROKEN_SPLIT` gate.

Idea: under `P`'s lock (held when control returns from a child), the question *"is R reachable from above me?"* is race-free. If `R == P.downlink[i+1]`, the loop will walk it next; otherwise `R` is orphaned from the downlink topology and we walk it now.

Known holes when committed: last in-memory downlink's rightlink never matched against a "next iteration"; root's rightlink discarded by the top-level caller; cascading splits only single-stepped; add_extent double-count on concurrent downlink install.

Smoke (10 trials of `deep_kill`): trial 2/10 reproduced.

### v2 — `acc0c70d` ("peek-ahead rightlink resolution")

Same return-value plumbing as v1, but the inner resolution uses a *peek-ahead* on a copy of the locator rather than a carry-forward across iterations:

```c
OInMemoryBlkno pending = check_walk_btree(status, childBlkno, blkno);

while (OInMemoryBlknoIsValid(pending))
{
    BTreePageItemLocator peek = loc;        /* fresh copy each iter */
    BTREE_PAGE_LOCATOR_NEXT(p, &peek);

    if (BTREE_PAGE_LOCATOR_IS_VALID(p, &peek))
    {
        BTreeNonLeafTuphdr *nextHdr =
            (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &peek);

        if (DOWNLINK_IS_IN_MEMORY(nextHdr->downlink) &&
            DOWNLINK_GET_IN_MEMORY_BLKNO(nextHdr->downlink) == pending)
            break;        /* outer loop's natural advance will walk it */
    }
    pending = check_walk_btree(status, pending, blkno);   /* walk via rightlink */
}
```

Why this is structurally better than v1:

- **Last in-memory downlink uniformly handled.** If `loc` is the last item, `BTREE_PAGE_LOCATOR_NEXT(p, &peek)` makes `peek` invalid → falls into the rightlink walk. No need for a separate post-loop fixup.
- **Cascading splits handled.** The `while` runs on `pending`'s return value, so if `R` itself returns a non-Invalid rightlink (`R'`), we resolve that one too — same peek, same compare against position `i+1`.
- **No carry-state across outer iterations.** `pending` is scoped to the current child; less surface area to reason about.
- **`peek` doesn't drift.** Declared inside the `while` body, so `peek = loc` resets each iteration. The outer `loc` only advances at line 607 (after the inner `while` finishes).

`SplitTest.test_phase1_split_not_reported_as_leak` (the focused regression added by `d2a2723e`, which deterministically returns False without any rightlink walk) **passes** under v2 — confirms the patch correctly handles phase-1 split reachability in the controlled setup.

Smoke (10 trials of `deep_kill`): **trial 2/10 reproduced** with the identical `Extent 2 1` / `o_bank_account_token_uniq` fingerprint. Same outcome as v1.

### Conclusion — the leak isn't a phase-1 split reachability case

Three independently-designed `check_walk_btree` patches all leave the deep_kill reproduction rate effectively unchanged. The focused phase-1 regression test passes; the stress repro doesn't. Strong inference: the orphan extent under `deep_kill` is not reachable from the tree via *any* combination of downlinks and rightlinks — it's been disconnected from the topology entirely.

Most likely source: a recovery-side bookkeeping leak after `kill -9`. WAL replay reconstructs the index but doesn't re-mark some allocated extent as either busy (in-tree) or free (returned to the freelist), so `check_extents` finds it in neither set.

**Next step (when resumed):** confirm by parsing a saved BUG log (`test/t/crash/results/*_deep_kill_*_invariant.log` or `/tmp/leak_hit_smoke.log`) — find the orphan blkno, then verify it is unreachable from the SK root by any walk. If confirmed, the investigation moves from `check.c` to:
- `src/recovery/recovery.c` (the WAL replay path)
- `src/checkpoint/checkpoint.c` (free-extent persistence in `.map`)
- `src/catalog/free_extents.c` (in-memory FSM bookkeeping)

The current `acc0c70d` branch state has v1 + v2 committed in sequence on top of the cherry-picked `d2a2723e`. Both improve the *structural* correctness of `check_walk_btree`'s rightlink handling and should stay even though they don't resolve the stress-repro leak.
