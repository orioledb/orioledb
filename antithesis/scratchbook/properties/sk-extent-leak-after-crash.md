# sk-extent-leak-after-crash

## Merge note

Merges two independently-written files (Resource Boundaries and
Data-Integrity-adjacent focus passes) that both read the exact same
unmerged-branch document (`extent_leak_issue.md`) and reached the same
conclusion: `compressed-extent-leak-after-crash.md` and this file
(`sk-extent-leak-after-crash.md`, kept as the canonical slug — has the
clean catalog-format property table).

## Focus

Resource Boundaries / Data Integrity. This property is built from a document found on an **unmerged remote branch, `origin/add_stress_bank_account_test`** (`test/t/crash/extent_leak_issue.md`, read via `git show origin/add_stress_bank_account_test:test/t/crash/extent_leak_issue.md`) — it does **not** describe code or a fix present on `main`. Flagged explicitly per `validating-claims.md`'s standing rule that external material is a lead, not a fact.

## Summary

A single `ORIOLEDB_BLCKSZ` (8 KB) block leaks from a secondary unique index's data file after repeated `kill -9` + crash-recovery cycles under concurrent write load: the block exists on disk, but is referenced by neither the in-memory tree (not "busy") nor the free-extent `.map` list (not "free"). Three independently-designed patches to the structural checker (`check_walk_btree`) all fail to change the reproduction rate, which the investigating doc treats as strong evidence the bug is a genuine recovery-side free-space-management bookkeeping defect, not a checker false-positive on a racy split. This directly corroborates `sut-analysis.md` §10's flagged "plausible corruption vector" for compressed-tree free space management, with much stronger (instrumented, hypothesis-eliminating) evidence than the original static-analysis lead.

## Validation performed (per `validating-claims.md`)

This is a bug-investigation document, not a single-reporter issue — the competing explanation to rule out is "the checker (`orioledb_tbl_check`) is buggy, not the storage engine" (the doc itself raises and investigates this exact alternative). Key facts, quoted/paraphrased directly from the doc:

- Symptom: `orioledb_tbl_check()` reports `NOTICE: Extent X 1 is neither free or busy` / `Corrupted index name = o_bank_account_token_uniq` — **always** on the secondary index, **never** on the PK, even in the same run where the SK leaks. The PK btree is index-organized and is always clean.
- The doc separately confirmed no data is actually lost: `sum(balance)`, row/distinct-id/distinct-token counts are all correct in every trial — "the corruption is exclusively in the page-allocator's bookkeeping (busy ∪ free != entire data file)," not row loss. This distinguishes it clearly from `recovery-sk-rebuild-desync.md` (a different bug with a different fingerprint, explicitly cross-referenced and disambiguated by a later commit in the same doc lineage, `8703c229`: "tbl_check=false divergence is the KNOWN extent leak, not this issue").
- Three independently-designed patches to `check_walk_btree` (`src/btree/check.c`) targeting the working hypothesis "the orphan extent is an in-flight phase-1 split page the top-down checker can't see yet" were implemented and smoke-tested against the repro: the upstream cherry-pick `d2a2723e`, and two structurally deeper rightlink-carrying rewrites `4da80ba1`/`acc0c70d`. All three left the `deep_kill` stress reproduction rate **unchanged** (trial 2/10 reproduced in both v1 and v2, identical fingerprint), while the *focused* regression test added by `d2a2723e` (`SplitTest.test_phase1_split_not_reported_as_leak`) passes cleanly. The doc's own conclusion, quoted directly: "Three independently-designed `check_walk_btree` patches all leave the deep_kill reproduction rate effectively unchanged. The focused phase-1 regression test passes; the stress repro doesn't. Strong inference: the orphan extent under deep_kill is not reachable from the tree via *any* combination of downlinks and rightlinks — it's been disconnected from the topology entirely," pointing the remaining investigation at `src/recovery/recovery.c` (WAL replay), `src/checkpoint/checkpoint.c` (free-extent `.map` persistence), and `src/catalog/free_extents.c` (in-memory FSM bookkeeping) instead. This is exactly the kind of "ruled out the ordinary alternative with a specific detail" evidence `validating-claims.md` calls for.
- Repro mechanism: `kill -9` of the postmaster (unclean shutdown) followed by automatic crash recovery, under sustained concurrent write load against a table with a secondary `UNIQUE` index (`test/t/crash/run_hunt.sh`, `RR_KILL_POSTMASTER=1` every 8s). This is squarely within Antithesis's fault-injection repertoire (process kill + concurrent load), not a contrived synthetic setup.
- A curious, separately-noted symptom in the same trials: a planner query-plan check expecting an SK Index-Only-Scan instead got a PK-based custom scan — but the doc explicitly rules out a causal link ("plan-FAIL fires on trials that do NOT have an FSM leak... independent symptoms of the same upstream event [SIGKILL+recovery], not cause-and-effect"). Worth noting so a future investigator doesn't waste time chasing a red herring the original investigator already ruled out.

Independently confirmed the compressed/secondary free-space-management mechanism this bug lives in is real and matches `doc/architecture/fsm.mdx`'s own description (read in full): two system trees, `SYS_TREES_EXTENTS_OFF_LEN` and `SYS_TREES_EXTENTS_LEN_OFF`, with an explicitly multi-step, lock-free-ish concurrent extent-merge protocol ("insert first (step 2), only then delete (step 3)... no intermediate state with no extent to merge") — i.e. the mechanism the doc's remaining hypothesis points at is genuinely complex, multi-tree, and concurrency-sensitive, consistent with a recovery-replay bookkeeping gap being plausible.

**Conclusion of validation**: this reads as a real, reproducible defect in recovery-side free-extent bookkeeping (candidates named in the doc: WAL replay path `src/recovery/recovery.c`, checkpoint's `.map` persistence `src/checkpoint/checkpoint.c`, in-memory FSM bookkeeping `src/catalog/free_extents.c`), not a checker false-positive and not reporter error. It remains **unfixed and unmerged** as of this pass — the document's own "Next step (when resumed)" section confirms the investigation was left open, not concluded.

## Fix status — not found on `main`

None of the three `check_walk_btree` patch attempts (`d2a2723e`, `4da80ba1`, `acc0c70d`) nor their reverts (`b97d2545`, `524a0d4f`, `da78ee75`) are ancestors of the analyzed commit `a975c702` (checked via `git merge-base --is-ancestor`, all "NO"). This entire investigation lineage appears to sit on a divergent branch not merged into the analyzed commit's history at all — i.e. **this is a genuinely unmerged lead**, not a fixed-then-regressed one. One adjacent commit, `a0c7b3ab` ("check: include the next-checkpoint .tmp + in-memory seq_buf tail"), *is* an ancestor of `a975c702`, suggesting the lineages share a common ancestor before diverging, but exactly where the `check.c` code at `a975c702` stands relative to the three patch attempts (whether it has none of them, or an earlier/different variant) was not fully traced.

## Why this belongs to Resource Boundaries specifically

A "neither free nor busy" extent is a permanent, small (8 KB per occurrence) disk-space leak: the block can never be reused (the free-list doesn't know about it) and will never be visited again by tree walks (nothing references it). Under Antithesis's style of sustained, repeated crash-injection over a long run (exactly what the `deep_kill` reproducer does — repeated `kill -9` cycles), these leaks are cumulative: each crash-recovery cycle that hits the race can leak another block, and nothing in the system reclaims them. This is a slow, invisible-to-a-single-check resource leak — precisely the shape this attention focus targets ("file descriptor leaks, memory growth... capacity limits") applied to on-disk space instead of memory/fds.

## What goes wrong

A leaked extent is not itself data loss (rows are all present and correct per the doc), but it is a real accounting corruption: disk space is silently wasted (permanently, since nothing will ever free or reuse that block), and `orioledb_tbl_check()` — the tool users/`pg_amcheck` integration would run to verify integrity — correctly flags it as "neither free or busy," so this is at least detectable, just not self-healing.

## Property

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every physical extent (block) in a B-tree data file is, at all times after crash recovery completes, accounted for as exactly one of "free" (in the `.map` free-extent list) or "busy" (reachable from the tree via downlinks/rightlinks) — never neither. |
| **Invariant** | `Always(check_extents_reports_no_orphans)`: run `orioledb_tbl_check()` (or the lower-level `check_extents()` check it wraps) after crash recovery and assert it reports zero "neither free or busy" extents. This is exactly the existing oracle the `sk-recovery-race[-chaos]` harness already uses for a different invariant (PK/SK consistency) — this property asks a second, distinct question of the same oracle output (extent accounting, not tuple-level consistency), and per `sut-analysis.md` §8, this checker's own historical instability (two reverts of a related "misreport phase-1 splits as leaked extents" fix — see also `tbl-check-oracle-transient-false-negative.md`) means this exact check path is worth exercising directly rather than assumed solid. |
| **Antithesis Angle** | Repeated, sustained `SIGKILL` of the postmaster under concurrent DML load against a table with a secondary index — the existing `sk-recovery-race-chaos` topology and workload shape is close to what's needed; the missing ingredient is running `orioledb_tbl_check()`'s extent-accounting output (not just its boolean pass/fail) after *repeated* crash cycles in the same run, since the doc's own reproduction needed a `deep_kill` loop (multiple kills over a duration) rather than a single crash. `orioledb_tbl_check()`'s NOTICE output is a directly assertable signal (`Always(no "Extent X Y is neither free or busy" notice)` or `Unreachable()` on the specific corrupted-index-name NOTICE pattern). |
| **Why It Matters** | Individually tiny (8 KB), but the mechanism producing it is a recovery-side bookkeeping gap that could plausibly also affect the *other* direction (a busy extent wrongly marked free, which — unlike this leak — would be actual corruption/data loss, not just wasted space) if the same root cause has a symmetric failure mode not yet observed. Establishing the property gives Antithesis a standing check that would also catch that more severe symmetric case if it exists. |

**Open Questions:**

- Is this reachable on `main` at all, or was it introduced/only observable on the unmerged branch's own modifications (the branch adds a bank-account stress harness and fault-injection points, `test/t/crash/rr_stress_test.py`, not present on `main`)? The doc's own "Fix attempts" section patches `src/btree/check.c::check_walk_btree`, which **is** present on `main` in some form — but whether the underlying leak mechanism (recovery-side FSM bookkeeping) is a defect in `main`'s recovery/checkpoint code specifically, or somehow entangled with the branch's own injection scaffolding, was not independently re-derived; the doc's own analysis places the bug in `src/recovery/recovery.c` / `src/checkpoint/checkpoint.c` / `src/catalog/free_extents.c`, all of which exist on `main`, making the "reachable on `main`" reading the more likely one, but this should be confirmed empirically (running the `main`-branch checker against a `main`-branch `deep_kill` repro) before treating it as certain. `(needs human input / empirical confirmation on main)`
- Where does `a975c702`'s `check_walk_btree` currently stand relative to the three patch attempts? Not determined — would need to read the function at the analyzed commit and compare against the doc's described `d2a2723e`/`4da80ba1`/`acc0c70d` diffs directly. `(partial: confirmed none of the named commits are ancestors; did not read the current function body to characterize its actual behavior)`.
- Is the true root cause (recovery-side FSM bookkeeping, per the doc's final hypothesis) confirmed anywhere, or does the investigation lineage end at "not yet resumed" (per the doc's own "Next step (when resumed)" section)? Per the doc, this is explicitly an unfinished investigation — treat the root cause as unconfirmed.
- Does `orioledb.remove_old_checkpoint_files` (default `true`, `src/orioledb.c:672-681`, cleans up `.tmp`/`.map` files post-checkpoint) interact with this leak — e.g., could an interrupted cleanup during the same crash window itself contribute to the orphaned-extent bookkeeping gap, or is it unrelated? Not investigated.

## SUT-side instrumentation

`existing-assertions.md` confirms the only existing consumer of `orioledb_tbl_check()` as an Antithesis oracle is `sk-recovery-race/driver.py` (line 89-95) and `sk-recovery-race-chaos/driver.py` (line 87-93), both checking PK-row-count vs. distinct-SK-token-count plus the tbl_check boolean — **neither currently inspects `check_extents()`'s specific NOTICE output for orphaned extents**, only the overall pass/fail. This is a **partially existing** instrumentation point: the harness already calls the right function on the right fault pattern, but doesn't check the specific signal this property needs. Suggested addition: parse/assert on the "neither free or busy" NOTICE text (or add a dedicated boolean-returning variant of `check_extents()` if one doesn't already exist) rather than only the top-level boolean.

### Investigation Log

#### Is this reachable on `main` at all, or only on the unmerged branch's own modifications?

- Examined: the doc's "Fix attempts" patches to `src/btree/check.c::check_walk_btree`; the doc's named root-cause candidate files `src/recovery/recovery.c`, `src/checkpoint/checkpoint.c`, `src/catalog/free_extents.c`.
- Found: all of `check.c` and the three root-cause-candidate files exist on `main` in some form, making "reachable on main" the more likely reading.
- Not found: no independent empirical confirmation — did not run the `main`-branch checker against a `main`-branch `deep_kill` repro; whether the leak mechanism is entangled with the branch's own bank-account stress/injection scaffolding (not present on `main`) was not re-derived.
- Conclusion: tagged `(needs human input / empirical confirmation on main)` — plausible but unverified without a live repro on `main`.

#### Where does `a975c702`'s `check_walk_btree` currently stand relative to the three patch attempts?

- Examined: `git merge-base --is-ancestor` against `a975c702` for commits `d2a2723e`, `4da80ba1`, `acc0c70d` (and reverts `b97d2545`, `524a0d4f`, `da78ee75`), and adjacent commit `a0c7b3ab`.
- Found: none of the three patch commits or their reverts are ancestors of `a975c702` (all checks returned "NO"); `a0c7b3ab` ("check: include the next-checkpoint .tmp + in-memory seq_buf tail") is an ancestor, suggesting a shared lineage before the branches diverged.
- Not found: did not read `check_walk_btree`'s actual body at `a975c702` to characterize whether it independently contains equivalent, partial, or none of the patched logic.
- Conclusion: tagged `(partial: confirmed none of the named commits are ancestors; did not read the current function body to characterize its actual behavior)`.
