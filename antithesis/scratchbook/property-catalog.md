---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: In-repo documentation site (doc/architecture/*.mdx, doc/usage/*.mdx, doc/contributing/*.mdx) is the primary source of claimed guarantees and product framing; treated as leads to validate, not facts.
---

# Property Catalog: OrioleDB

## Scope restriction (read this first)

This catalog deliberately excludes **rewind** (`src/rewind/`, `orioledb_rewind_*`)
and **S3-backed decoupled storage** (`src/s3/`, `orioledb.s3_mode`) functionality.
The user ruled both out of scope for this research pass, and all evidence
files about them (rewind container-topology interactions, S3 lock-file
deletion, S3 checkpoint hangs, S3 crash-restart escalation, etc.) have been
removed from `properties/`. `sut-analysis.md` still describes this surface
(§2, §6, §7, §8, §11, §12) as background/context, since that file predates the
scope narrowing and was not edited — but no property below targets rewind or
S3, and none should be added later without the user re-opening that scope.

Relatedly, `/Users/artur/supabase/orioledb_postgres` (the patched PostgreSQL
source) is **no longer an active external reference** for this catalog or any
future work on it, per explicit user instruction, even though it was consulted
during the earlier (now-superseded) phase of this research pass that produced
`sut-analysis.md`'s architecture and concurrency sections. Do not re-open or
cite that path when extending this catalog.

Despite these exclusions, initial SUT discovery found significant untested
surface in the excluded areas — rewind especially (164 commits, zero
Antithesis exposure, a plausible container-topology interaction with the
harness's process model). That surface remains real and undiscovered; it is
simply out of scope for this catalog by user decision, not because it was
judged low-value.

## Methodology and provenance

This catalog was assembled by deduplicating 47 independently-written property
evidence files (produced by 11 parallel discovery agents, one per attention
focus: Data Integrity, Concurrency, Failure Recovery, Protocol Contracts,
Resource Boundaries, Security Boundaries, Distributed Coordination, Lifecycle
Transitions, Idempotency and Replay, Version Compatibility, Wildcard) into 38
canonical properties. See `property-relationships.md` for how these properties
cluster by shared code paths and failure mechanisms, and the merge log
recorded in each merged evidence file's own "Merge note" section for exactly
which files were combined.

A subsequent evaluation pass (`evaluation/synthesis.md`) ran four independent
lenses (antithesis-fit, coverage-balance, implementability, wildcard) against
those 38 properties. The lenses named seven coverage gaps (G1-G7) and fifteen
refinements (R1-R15); G1-G6 were filled by targeted discovery agents (G7 was
handled as a `deployment-topology.md` refinement instead, since its property
already existed), adding **15 new properties**: 4 covering resource-boundary/
infrastructure-fault gaps (G1), 6 covering backup/restore and cross-major
`pg_upgrade` gaps (G2, G6), and 5 covering three dropped `sut-analysis.md`
risks plus two bugs found by a fresh branch sweep (G3, G5). This brings the
catalog to **53 canonical properties** total (see "Sub-counts" immediately
below). The fifteen refinements (R1-R15) were applied directly to existing
entries in place; see `evaluation/synthesis.md` for the full rationale behind
each gap and refinement, which is not repeated here.

Every property below has a corresponding evidence file at
`antithesis/scratchbook/properties/{slug}.md` with fuller code traces,
Investigation Logs, and Open Questions detail than fits in this summary.

**Sub-counts (SUT-behavior vs. harness-integrity meta-properties).**
Of the 53 properties in this catalog, **47 are SUT-behavior properties**
(claims about OrioleDB's own runtime correctness or liveness) and **6 are
harness-integrity meta-properties** (claims about whether the *existing*
Antithesis harness/test suite's own verification pipeline, or its own driver
code, can be trusted), and these two buckets exhaustively account for all 53:
the four-member "Test Oracle and Harness Integrity" category
(`tbl-check-oracle-transient-false-negative`, `jepsen-verdict-not-sdk-visible`,
`chaos-driver-skips-check-on-fault-landing`, `core-postgres-hook-coverage-
blind-spot`) plus `backup-restore-lacks-structural-oracle` (added by the
gap-fill pass — a property about the existing pgbackrest/wal-g integration
tests' own oracle, not about OrioleDB's data, matching the same shape as the
other four) plus `checkpoint-stats-view-pg-major-branch`. That sixth member is
a related but distinct case — a CI-hygiene regression guard on the Antithesis
*driver's own* Python code (see its entry below) rather than a claim about the
harness's verification pipeline's trustworthiness — but it is still a claim
about test-harness code rather than about OrioleDB's own runtime behavior, so
it belongs in the harness-integrity bucket, not the SUT-behavior one, bringing
that bucket to 6 and SUT-behavior to 47 (47 + 6 = 53, with no property left
uncounted).

---

## Category: Checkpoint / Recovery Boundary Consistency

The single most concretely-tracked bug class in this codebase: the
checkpoint-boundary race between primary-key and secondary-key state
(orioledb#855) and its structurally-related siblings. All properties in this
category concern whether a checkpoint's captured LSN/undo/oxid boundaries stay
consistent with what crash recovery later replays — the same failure shape
recurring at different points in the checkpoint/recovery machinery. This is
also where the existing Antithesis harness (`sk-recovery-race[-chaos]`)
already has coverage; every property here either extends that coverage into a
timing window the existing harness doesn't reach, or targets a structurally
adjacent bug found by the same investigation.

### sk-fixup-undo-recycling-drop — Pending SK fix-up dropped if its undo record is recycled before replay

| | |
|---|---|
| **Type** | Safety |
| **Priority** | High — surviving variant of #855, the catalog's top-tracked bug class; silent PK/SK divergence risk |
| **Property** | A PK/SK-fixup record written at checkpoint time for a backend caught in the PK-applied/SK-pending window is always eventually applied during crash recovery — the referenced undo location is never recycled out from under a still-pending fixup, so no PK row is ever left without its corresponding secondary-index entry (or vice versa) after a crash. |
| **Invariant** | `Always`: reuse the existing `sk-recovery-race` oracle — PK-row-count == distinct-SK-token-count for `o_sk_pending`, plus `orioledb_tbl_check()` structural consistency (same check as `sk-recovery-race/driver.py:89-95`). The novel angle is deliberately widening the checkpoint-write-to-replay gap and injecting undo churn in that gap, to try to reach `apply_one_pending_sk_fixup()`'s `UNDO_REC_EXISTS(...) == false` branch. |
| **Antithesis Angle** | Pin the `sk_modify_pending` stopevent as the existing driver does, checkpoint, then — before letting recovery run — drive a burst of unrelated commits/rollbacks designed to advance the undo horizon past the captured `pendingLoc`, then trigger/allow recovery. On a standby topology, a process-pause fault on the recovery/startup process combined with sustained undo-churning DML on the primary is the organic way to widen this window. |
| **Why It Matters** | Silent-corruption failure class (a PK row visible in scans but missing/wrong in a secondary-index lookup) — the worst-case failure category for a database engine per `sut-analysis.md` §10. It is specifically a **surviving variant of #855** that the team already fixed once and built dedicated Antithesis coverage for. |
| **Scope note** | Like the other undo-retention properties in this catalog (see `undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant`, `replica-undo-reclaimed-too-early`), this implicitly tests only the `enable_rewind=false` branch of shared undo-retention logic — low-risk since `enable_rewind` is `PGC_POSTMASTER` (fixed at server start, not runtime-fragile), and rewind is out of this catalog's scope regardless. |

**Open Questions:**

- Is the recycled-skip branch reachable within a realistic Antithesis run duration, or does normal undo retention make the window too narrow in practice? `(needs further investigation)`
- Does the existing `sk-recovery-race-chaos` driver's reliance on chance overlap ever incidentally stretch the checkpoint-to-replay gap far enough to hit this today?
- Does anything else incidentally protect this specific undo location, narrowing the gap beyond what the code alone suggests?

### sk-fixup-sentinel-spin-livelock — Checkpoint's self-created-table sentinel wait has no timeout or interrupt check

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | Medium — real unbounded busy-wait, but availability-only impact and needs blocking new instrumentation to check precisely |
| **Property** | The checkpoint process's wait for a `WaitingSkUndoLoc` sentinel to clear (self-created-table fast path in `checkpoint_write_pending_sk_fixups()`) always resolves within a bounded time proportional to normal commit/abort latency — it does not stall the entire checkpoint indefinitely if the backend that set the sentinel is delayed, descheduled, or fails to reach commit/abort promptly. |
| **Invariant** | `Sometimes(checkpoint_entered_sentinel_spin_wait)` to confirm the interesting path is reached, paired with `Always(checkpoint_completes_within_bounded_time_after_sentinel_clears)` under fault injection that delays the specific backend holding the sentinel. |
| **Antithesis Angle** | Create-and-modify-in-one-transaction workload on a table with a secondary index, force a `CHECKPOINT` to land exactly mid-window, combine with scheduling-delay fault injection targeting that specific backend between setting the sentinel and reaching commit. |
| **Why It Matters** | This spin has no `CHECK_FOR_INTERRUPTS()` — the checkpoint process cannot even be cleanly cancelled out of this wait. Worth checking this isn't a livelock relocated (not closed) by the #855 fix itself. |
| **Instrumentation** | **Blocking, not optional** (evaluation, Implementability lens): an exhaustive read of `stopevents.txt` confirmed no existing stopevent pins the moment the checkpoint enters this sentinel-spin wait or the moment `pendingSkUndoLoc` clears — without adding one, `Sometimes(checkpoint_entered_sentinel_spin_wait)` cannot be implemented with precision, only inferred from timing. |
| **Antithesis Angle (cheaper alternative)** | Per the cross-cutting "unbounded busy-wait, no `CHECK_FOR_INTERRUPTS()`" pattern (see `property-relationships.md` Cluster 9): rather than only relying on container/process-level fault injection (CPU starvation, process pause) to test whether this wait is interruptible, target the specific backend holding the sentinel directly with `pg_cancel_backend()`/`pg_terminate_backend()` or a `statement_timeout` — a cheaper, more direct SQL-level test of the same "is `CHECK_FOR_INTERRUPTS()` ever consulted here" question than inferring it from process-freeze timing. |

**Open Questions:**

- Can a backend that set the sentinel be killed/crash without another mechanism resetting `pendingSkUndoLoc`, leaving the checkpoint's spin waiting forever? `(needs further investigation — the single most important unresolved question for this property)`
- Is there any outer bound (overall checkpoint timeout) that would eventually abort a checkpoint stuck here?
- What is the realistic worst-case duration of the window absent fault injection?

### checkpoint-abort-snapshot-standby-panic — Checkpoint's in-flight-oxid snapshot can resurrect an already-aborted oxid and PANIC a standby

| | |
|---|---|
| **Type** | Safety |
| **Priority** | High — confirmed producer-side race still open after a partial fix; standby PANIC is a hard availability failure |
| **Property** | A checkpoint's snapshot of in-flight oxids (`finish_write_xids()`) never causes crash recovery to resurrect an oxid whose rollback has already been durably applied below the checkpoint's own replay-start boundary — no such resurrection ever reaches a point where a streaming standby's undo-stack walk sees a zeroed/invalid location and PANICs. |
| **Invariant** | `Always`: a standby involved in a checkpoint/crash-recovery cycle must never PANIC, complemented by `Sometimes(recovery_leader_undo_drain_lagged_behind_deferred_rollback)` to confirm the interesting timing window is actually reached under fault injection, not just under the deterministic test's hand-crafted stopevent pinning. |
| **Antithesis Angle** | Needs a primary+streaming-standby topology. Abort-heavy DML + periodic checkpoints on the primary, with the standby's recovery leader lagging behind its own workers' commit-pointer drain (via scheduling-delay/CPU-throttling fault injection) — a more organic way to reach the same window the deterministic test manufactures with two stopevents. |
| **Why It Matters** | A standby PANIC is a hard availability failure triggered purely by primary-side checkpoint/abort timing. The landed fix (`93db964d`) closes only the consumer-side (standby-leader) misinterpretation of a resurrected oxid; the producer-side race in `finish_write_xids()` is confirmed, by direct code reading, still present. |

**Open Questions:**

- Is there a fault sequence — not relying on the deterministic test's `replay_on_record` stopevent — under which the consumer-side fix itself still fails to prevent the PANIC? `(needs further investigation)`
- Does any other consumer of the checkpoint's xids dump read a resurrected-oxid's undo location without a validity-sentinel check?
- Is the "second failure leg" (primary-side snapshot/`replayStartPtr` synchronization) tracked anywhere beyond the unmerged-branch doc it was sourced from?

### checkpoint-recovery-lsn-sync-gap — Checkpoints taken during recovery use the leader's replay pointer, not a worker-synchronized one

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — real FIXME-documented gap in the #855 bug class, but needs blocking new instrumentation before the reachability half is checkable |
| **Property** | A checkpoint or restartpoint taken while `RecoveryInProgress()` is true captures `replayStartPtr`/`sysTreesStartPtr`/`toastConsistentPtr` values that correctly reflect a point no later than what every parallel recovery worker has actually applied — not merely the leader's own replay position — so the PK/SK fixup mechanism and the "no partial system-tree changes" guarantee both hold across a crash immediately following such a checkpoint, including immediately after `CHECKPOINT_END_OF_RECOVERY`. |
| **Invariant** | `Always`: reuse the `sk-recovery-race` oracle (PK-row-count == distinct-SK-token-count + `orioledb_tbl_check()`), exercised via a checkpoint forced during active multi-worker recovery replay rather than live-DML. `Reachable()` to confirm workers were actually still lagging the leader at the moment the LSN was captured. |
| **Antithesis Angle** | Needs a standby lagging behind a busy primary so restartpoints land mid-replay (scheduling delay on specific recovery worker processes widens the leader-vs-slowest-worker gap). A simpler variant needing no standby: crash mid-DML, then check the invariant immediately after the automatic `CHECKPOINT_END_OF_RECOVERY`. |
| **Why It Matters** | `get_checkpoint_xlog_ptr()` has an explicit `/* FIXME: synchronize recovery workers */` comment — the same assumption class #855 violated, relocated from the live-DML path to the recovery/promotion path, and structurally unreachable by the existing harness (which only checkpoints a running instance, never crashes mid-DML into a promotion checkpoint). |
| **Instrumentation** | **Blocking, not optional** (evaluation, Implementability lens): an exhaustive `stopevents.txt` read confirmed there is no existing pin-point exposing "how far behind the leader is the slowest recovery worker at the moment a checkpoint captures its LSN" — the `Reachable()` half of the Invariant cannot be implemented with precision until a stopevent or counter surfacing this gap is added. |

**Open Questions:**

- Does a real fault sequence exist where the leader-vs-slowest-worker gap is large enough, at the exact LSN-capture moment, to flip correctness? `(partial: mechanism and reachability confirmed via code reading; timing-window magnitude not measured)`
- Exact ordering between `o_recovery_finish_hook`'s worker-join loop and the `CHECKPOINT_END_OF_RECOVERY` checkpoint's LSN capture — is end-of-recovery itself reachable while workers are still active, or only ordinary mid-recovery restartpoints? `(partial)`
- Does this require a multi-recovery-worker configuration to be reachable at all?

### recovery-sk-rebuild-desync — Secondary-index rebuild can diverge from PK content after any crash near an unrelated commit (highest-priority, likely still-open finding)

| | |
|---|---|
| **Type** | Safety |
| **Priority** | High — likely still-open silent PK/SK divergence that the existing structural oracle cannot detect |
| **Property** | Recovery's secondary-index rebuild never diverges from primary-key content after a crash — regardless of what the crashing transaction itself was doing, and even when the divergence is caused purely by recovery's replay of *other, cleanly-committed* transactions' SK entries. |
| **Invariant** | `Always`: the invariant is already checked by the existing `always()` in both `sk-recovery-race/driver.py` and `sk-recovery-race-chaos/driver.py` (PK-row-count == distinct-SK-token-count) — no new assertion is required. What's missing is a workload/fault shape that doesn't rely on the `sk_modify_pending` stopevent at all, since this bug reproduces via faults unrelated to that specific window (reproduced at four different injection points along the commit pipeline, 8-30% each). |
| **Antithesis Angle** | Sustained concurrent DML committing normally, plus an unrelated fault (crash/PANIC/SIGKILL) landing at effectively any point in the commit pipeline of some backend — exactly what Antithesis's generic fault injection is suited to provide without a deliberately placed stopevent. |
| **Why It Matters** | Two independently-written, unusually rigorous root-cause docs (`ISSUE_TOKEN_LEAK_COMMIT_ASSERT.md`, `ISSUE_TOKEN_LEAK_PRE_COMMIT_WAL_FINISH.md`) reproduce a permanent PK↔SK divergence, and **critically, both report `orioledb_tbl_check()` returning `true` while the divergence is present** — the existing structural oracle does not catch this class of bug at all; only the count-comparison half of the existing assertion would. No fix commit was found in git history for this specific bug — it should be treated as likely still open. |

**Open Questions:**

- What is the actual root cause in `src/recovery/recovery.c`/`src/btree/modify.c`? The docs' own hypothesis (SK-side delete-old-key step missing on `WAL_REC_UPDATE` replay for column-changing updates) was not independently traced. `(needs further code-reading investigation)`
- Is this bug present at the analyzed commit `a975c702`, or was it fixed by a commit whose message doesn't mention "token"/"SK leak" (invisible to keyword search)? `(partial: no plausibly-matching fix commit found; not confirmed absent by code tracing — treat as likely still open)`

### sk-extent-leak-after-crash — Repeated crash cycles leak an unreachable, unreclaimable extent from a secondary index's data file

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — confirmed unmerged/unfixed finding, but reachability on main needs empirical confirmation and today's impact is small/cumulative |
| **Property** | Every physical extent (block) in a B-tree data file is, at all times after crash recovery completes, accounted for as exactly one of "free" (in the `.map` free-extent list) or "busy" (reachable from the tree) — never neither. |
| **Invariant** | `Always(check_extents_reports_no_orphans)`: run `orioledb_tbl_check()`/`check_extents()` after crash recovery and assert zero "neither free or busy" extents — parsing the specific NOTICE text, not just the top-level boolean the existing harness currently checks. |
| **Antithesis Angle** | Repeated, sustained `SIGKILL` of the postmaster under concurrent DML load against a table with a secondary unique index (a `deep_kill`-style loop, not a single crash) — close to the existing `sk-recovery-race-chaos` topology, with the missing ingredient being repeated crash cycles in one run and inspecting the extent-accounting NOTICE output specifically. |
| **Why It Matters** | Found via an unmerged-branch doc (`extent_leak_issue.md`) whose investigation systematically ruled out "checker false-positive" as an alternative (three independent `check_walk_btree` patches all left the reproduction rate unchanged). Individually tiny (8 KB per leak) but cumulative under repeated crash-injection, and the same root cause could plausibly have an unobserved symmetric failure mode (a busy extent wrongly marked free — actual data loss, not just waste). Remains unmerged and unfixed as of this commit. |

**Open Questions:**

- Is this reachable on `main` at all, or entangled with the unmerged branch's own stress-harness scaffolding? `(needs human input / empirical confirmation on main)`
- Where does `a975c702`'s `check_walk_btree` currently stand relative to the three (unmerged, non-ancestor) patch attempts?
- Is the true root cause (recovery-side FSM bookkeeping) confirmed anywhere, or does the investigation end at "not yet resumed"?

### recovery-meta-lock-signal-barrier-deadlock — A standby recovery leader can permanently self-deadlock holding `oTablesMetaLock` across a `dbase_redo` ProcSignal barrier (confirmed open defect, not a regression target)

**Gap-fill addition (evaluation G5(a), Wildcard lens).** Found via a fresh
sweep of remote branches by committer-date; **confirmed open/unfixed at the
analyzed commit** (`git merge-base --is-ancestor 1df605da a975c702...` is
false — the fix on `origin/recovery-meta-buffering` is not an ancestor).
Unlike several other entries in this category (`recovery-finish-abort-
livelock`, `replica-xmin-monotonicity`), which are regression-guard targets
for bugs already fixed on `main`, **this is a live, still-open defect** —
frame any workload built for it as testing for an existing bug, not guarding
against a fix regressing.

| | |
|---|---|
| **Type** | Liveness (permanent standby freeze — a hard availability failure) |
| **Priority** | High — confirmed open, unfixed permanent standby-freeze defect |
| **Property** | A streaming standby's recovery leader never permanently stalls replaying WAL because `oTablesMetaLock` was held (via `o_tables_meta_lock_no_wal()`, on `WAL_REC_O_TABLES_META_LOCK`) across a `dbase_redo` record's `WaitForProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE)` call — i.e., a DDL statement bracketing systree changes with `WAL_REC_O_TABLES_META_LOCK`/`WAL_REC_O_TABLES_META_UNLOCK` never interleaves, on the WAL stream, with a concurrent `CREATE DATABASE`/`DROP DATABASE`/`ALTER DATABASE ... SET TABLESPACE` in a way that freezes standby replay indefinitely. |
| **Invariant** | `Always(standby_replay_progresses_within_bound)` — sample the standby's replay LSN/`pg_stat_replication` position at a steady cadence under a workload mixing OrioleDB DDL with database-lifecycle DDL, asserting it never stalls past a generous bound. `Sometimes(meta_lock_open_during_dbase_redo)` as a reachability companion confirming the dangerous interleaving was actually exercised (otherwise the `Always` claim could pass vacuously). |
| **Antithesis Angle** | Requires a primary+streaming-standby topology (the harness's largest documented gap). Concurrent `CREATE TABLE`/`CREATE INDEX ... USING orioledb` DDL (brackets systree changes with `META_LOCK`/`META_UNLOCK`) racing against `CREATE DATABASE`/`DROP DATABASE`/tablespace-move DDL on the primary, standby actively replaying — no stopevent strictly required if Antithesis's own fault-driven scheduling lands the interleaving organically; the team's own unmerged `before_o_tables_meta_unlock` stopevent (not present at this commit) would let a workload pin the race deterministically, the same way `sk-recovery-race` already does for the PK/SK checkpoint race. |
| **Why It Matters** | A **permanent, unrecoverable standby freeze** triggered by ordinary DDL concurrency — no crash, no corruption, just an unlucky WAL interleaving of two individually-ordinary DDL statement types. The mechanism was independently re-derived against the current worktree line-by-line (`LWLockAcquire`'s `HOLD_INTERRUPTS()` bracket masking `ProcessProcSignalBarrier` while the leader holds the lock), not merely accepted from the fix branch's own commit message. |

**Open Questions:**

- Why does the deadlock reproduce reliably on Linux CI but only act as a non-regression (non-hanging) check on macOS/PG18 locally, per the unmerged fix branch's own test comments? `(needs human input)`
- Does this same deadlock shape recur for any other `WaitForProcSignalBarrier` call reachable from a "regular" redo function's replay path while `oTablesMetaLock` is held open, or is `dbase_redo` the only trigger reachable today? `(partial: mechanism generalized correctly; exhaustive call-site enumeration not performed)`
- Is the fix on `origin/recovery-meta-buffering` actively targeted for merge, or stalled? Affects whether this should be framed as "regression guard, imminent fix" vs. "open defect, no active remediation." `(needs human input)`

### checkpoint-corrupted-tree-silent-skip — A checkpoint can silently exclude a corrupted tree from sys-tree bookkeeping and still report success (confirmed open defect, not a regression target)

**Gap-fill addition (evaluation G5(b), Wildcard lens).** Found via the same
branch sweep as the property above; **confirmed open/unfixed at the analyzed
commit** — two independent, identically-shaped fix commits exist on two
unmerged branches (`af851ce4` on `origin/checkpoint-io-error-fatal`,
`d482623e` on `origin/checkpoint_avoid_error_loops`), and `git merge-base
--is-ancestor` confirms neither is an ancestor of `a975c702`. As with the
entry above, **this is a live, still-open defect** — distinct from this
category's several regression-guard entries for already-fixed bugs; a
workload here tests for an existing gap, not a fix regressing.

| | |
|---|---|
| **Type** | Safety |
| **Priority** | High — confirmed open, unfixed defect: a checkpoint can silently lose crash-consistency coverage for a tree |
| **Property** | A checkpoint never completes and reports success while having silently excluded a tree from `SYS_TREES_SHARED_ROOT_INFO` bookkeeping due to an on-disk read failure (checksum mismatch or I/O error) during that tree's root-page load — either the checkpoint fails loudly and attributably, or the affected tree's exclusion is recorded somewhere a monitoring/verification pass could detect it; the exclusion must never be indistinguishable from "this tree was legitimately, benignly dropped mid-checkpoint by a concurrent `DROP`/`TRUNCATE`" (the case `o_btree_load_shmem_internal()`'s early-return comment was actually written for). |
| **Invariant** | `Always(checkpoint_failure_surfaces_loudly_or_is_recorded)`: after deliberately corrupting an on-disk root page (truncate/bit-flip a data file, mirroring the unmerged fix's own regression test) and forcing a checkpoint, assert that either the process terminates with a clear, corruption-attributed `FATAL`, or a subsequent `orioledb_tbl_check()`/`verify_orioledb()` pass flags the affected tree as inconsistent — today, as confirmed by direct tracing, **neither holds**: the process continues (`ERROR` only, not `FATAL`, in `evictable_tree_init_meta()`) and the silent exclusion is indistinguishable from benign concurrent deletion by any existing check. |
| **Antithesis Angle** | Direct disk-level fault injection (bit-flip or zero-fill a B-tree data file's root page while on disk but not buffer-resident) timed just before a `CHECKPOINT` needs to load that tree's root, repeated across multiple checkpoint cycles — reachable on the existing single-node harness (no standby needed), with `orioledb_checksums_enabled` at its default (`true`, never overridden in `test/antithesis/`). Pair a periodic forced `CHECKPOINT` with a periodic structural check of every table created, to surface a tree that quietly stopped being checkpointed N cycles ago. |
| **Why It Matters** | A **silent, permanent loss of checkpoint coverage** for a corrupted tree, masked as checkpoint success — the "wrong query results or lost writes" failure class `sut-analysis.md` §10 calls worst-case for a database engine, specialized to "a whole tree's crash-consistency guarantee silently degrades and nothing says so." The root-cause mechanism was independently re-traced against current `a975c702` code line-by-line (not merely accepted from the fix commits' own message): `evictable_tree_init_meta()`'s `ERROR` fires *before* `SYS_TREES_SHARED_ROOT_INFO` is (re-)inserted, so the next checkpoint's `o_btree_load_shmem_internal()` sees a missing entry, believes it's the benign concurrent-deletion case, and silently gives up — with the `false` return propagating unremarked through `perform_writeback_and_relock()`/`checkpoint_btree()`. |

**Open Questions:**

- Does `orioledb_tbl_check()`/`verify_orioledb()`, run independently of a checkpoint, actually detect that a tree's `SYS_TREES_SHARED_ROOT_INFO` entry is stale/missing relative to its last-known-good checkpoint — i.e., is there any existing oracle that would catch this today, even without a fix? `(needs further investigation — determines whether this property needs new instrumentation or can reuse the existing structural-check oracle)`
- Is the unmerged fix's chosen remedy (escalate to `FATAL`, i.e. crash the instance) the intended long-term approach, or a stopgap while a more surgical bookkeeping fix is pending? The two differently-named branches suggest active iteration on this exact tradeoff. `(needs human input)`
- Does the `FATAL` remedy reintroduce a crash-loop risk (`sut-analysis.md` §6's already-documented pattern) if the underlying corruption is persistent and re-checkpointed on every restart? Not traced — the fix's own test only checks the first FATAL/clean shutdown, not repeated-restart behavior. `(needs further investigation)`

---

## Category: Streaming Replication / GlobalXmin Coordination

`sut-analysis.md` §9 identifies "no primary/standby replication scenario under
fault injection anywhere" as the single largest Antithesis coverage gap. Every
property in this category requires that topology and targets a distinct
mechanism by which a primary's crash-recovery decisions fail to reach — or
actively corrupt — a streaming standby's own view of transaction/visibility
state. Two of these properties (recovery-finish-abort-livelock,
replica-xmin-monotonicity) independently correct stale "unfixed" claims in
`sut-analysis.md` after tracing git ancestry directly.

### recovery-finish-abort-livelock — Standby never learns a primary aborted an in-flight oxid during its own crash recovery (orioledb#876, fixed — regression target)

| | |
|---|---|
| **Type** | Safety (regression guard on a fixed, heavily-iterated bug) |
| **Priority** | Medium — regression guard on an already-fixed bug (#876), reused oracle, standby topology required |
| **Property** | After a primary crashes with in-flight transactions and a streaming standby has eagerly replayed their modifications as `COMMITSEQNO_INPROGRESS`, once the primary restarts and completes recovery, every such oxid is eventually resolved on the standby via the `WAL_REC_ROLLBACK` emitted by `o_emit_recovery_finish_rollbacks()` — no oxid stays permanently `INPROGRESS`, and no later conflicting modify livelocks in `oxid_get_csn()`. |
| **Invariant** | `Always(no_oxid_remains_inprogress_beyond_bounded_time_after_primary_recovery)`, complemented by `Sometimes(recovery_finish_aborted_oxids_nonempty)` so an `Always`-only check can't pass vacuously in a run that never exercises the aborting path. |
| **Antithesis Angle** | Long, buffer-overflowing transactions concurrent with primary crashes (`SIGKILL`) while a streaming standby is attached, continuing modifies against the same key ranges. Antithesis's fault injection explores crash timing the deterministic test's fixed recipe can't reach — e.g. crashing during the deferred-rollback emission window itself. |
| **Why It Matters** | **Corrects a stale claim in `sut-analysis.md` §2/§8**, which states this livelock is unfixed on `main` — two independent discovery passes confirmed via `git merge-base --is-ancestor` that the fix (`9bc39d3b`, issue #876) *is* an ancestor of `HEAD`/`main`, and that the cited "revert" (`3ea73f3d`) reverts an earlier, different attempt (`fb1a8acc`) on a side branch, not the version that shipped. This is now a regression-guard target for a real, previously-reproduced (~1-in-6-to-11) livelock. |

**Open Questions:**

- Is there a residual crash window between `recovery_finish()` populating `recovery_finish_aborted_oxids` (backend-local, not WAL-logged) and `o_emit_recovery_finish_rollbacks()` flushing it — does a second primary crash in that window lose the list and re-create the original livelock? `(partial: single-recovery-pass mechanism confirmed correct; double-crash-during-cleanup-window recursion not traced)`
- Does `test_recovery_finish_rollback_does_not_regress_replica_xmin` (added `57e587fe`, reverted `e8fc5d46`) currently exist on `main` in some form? `(needs a direct file check)`

### replica-xmin-monotonicity — Standby globalXmin must never regress below the frozen-slot high-water mark (orioledb#889, fixed — regression target)

| | |
|---|---|
| **Type** | Safety (regression guard on a fixed bug) |
| **Priority** | Medium — regression guard on an already-fixed bug (#889); Assert-only enforcement already crashes under the harness's cassert build |
| **Property** | `globalXmin`/`runXmin` on any recovering/standby process never moves backward, and specifically never drops below `writtenXmin` — the FROZEN-slot fast path in `oxid_get_csn()` must never mis-read a legitimately resolved xid as still in-flight because the horizon slid backward across it. |
| **Invariant** | `Always`: the two `Assert(xmin >= globalXmin)` sites in `update_run_xmin()`/`free_run_xmin()` (`src/recovery/recovery.c`) should be converted to (or paired with) an unconditional `always()` call for defense in depth, but the underlying "is this a no-op today" concern is resolved: Antithesis's target images build core Postgres with `--enable-cassert` (`test/antithesis/orioledb/Dockerfile`, confirmed by Implementability), so these `Assert()` sites already crash the process (a generically-detected failure) rather than silently passing through in the harness's actual build. Workload-side: periodically sample `orioledb_get_xid_meta()` on primary and standby, assert monotonicity plus a bounded lag between them. |
| **Antithesis Angle** | Long, low-volume transactions below the WAL-buffer-overflow threshold (invisible to the standby pre-crash) + bursts of short committed transactions + a `CHECKPOINT` + an unclean primary crash — mirrors the root-cause doc's no-injection reproducer (plain `SIGKILL`, ~12-30% repro rate per trial). Needs a streaming-standby topology. |
| **Why It Matters** | **Also corrects a stale `sut-analysis.md` claim** (same "unfixed" framing as #876, same investigation) — the fix chain (`ef8e93b9`, `a0d628c1`, both "Fixes orioledb/orioledb#889") is confirmed present on `main`. `globalXmin` regressing corrupts every subsequent visibility/liveness decision built on it, and the guarantee is now Assert-only — exactly the build-flag-dependent risk `sut-analysis.md` §4/§11 raises generically for this codebase. |

**Open Questions:**

- Is the fix chain actually complete, or could a fault ordering not covered by `9ec6d26a`'s specific test still regress `globalXmin`?
- `ef8e93b9`'s own commit message admits a **separate, still-open** "stuck low globalXmin" symptom — see `replica-globalxmin-catchup-lag`.

### replica-globalxmin-catchup-lag — Replica's globalXmin may never catch up to the primary's after recovery (companion, apparently still-open liveness issue)

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | Medium — plausible still-open liveness issue, but current failing status is unconfirmed pending a live test run |
| **Property** | After a primary crash/restart with an attached streaming standby under load, the replica's `globalXmin` converges (within a bounded, loose lag) to the primary's within a reasonable window, rather than staying pinned low indefinitely. |
| **Invariant** | `Sometimes()`-shaped: confirm convergence occurs, distinct from the safety property in `replica-xmin-monotonicity` (never moves backward) — this is about forward progress, not regression. |
| **Antithesis Angle** | Same topology and fault shape as `replica-xmin-monotonicity` (primary crash/restart under load with a streaming standby attached); sample `orioledb_get_xid_meta()` on both nodes repeatedly over a bounded polling window, mirroring the existing test's own construction. |
| **Why It Matters** | If `globalXmin` never catches up, undo space the primary has already reclaimed cannot be reclaimed on the replica, growing unboundedly under sustained write load — an availability/resource-exhaustion failure. The commit (`ef8e93b9`) that restored this test's docstring explicitly states the test **currently fails** on this symptom, and no `expectedFailure`/`skip` marker was found on it. |

**Open Questions:**

- Is this test actually failing today, or was it fixed by a later commit not yet identified? `(needs human input / needs test run — no keyword search hit a plausible fix commit)`
- Does a stuck-low `globalXmin` ever cause an actual visibility bug, or is its only effect undo-retention bloat? `(partial: framed as liveness-only by the source commit message; not independently re-derived)`

### replica-undo-reclaimed-too-early — Replica-side Assert crash when a B-tree page dereferences an already-reclaimed undo record

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — Assert-guarded replica crash path; shared-root-cause hypothesis with #889 explicitly unproven |
| **Property** | A B-tree page on a replica never dereferences an undo record location that has already been reclaimed by undo retention/trimming — the `Assert(UNDO_REC_EXISTS(undoType, undo_loc))` sites in `src/btree/page_contents.c:66,81` never trip in practice. |
| **Invariant** | `Unreachable()` on the TRAP itself (`failed Assert("UNDO_REC_EXISTS(...)")`), or an `Always()` wrapping the same underlying condition so it fires even in a non-assert (release) build, where the read would instead proceed silently against reused/freed content. |
| **Antithesis Angle** | Same topology and fault shape as `replica-xmin-monotonicity` (streaming standby + primary `SIGKILL` under load) — observed as an *alternative* outcome of the same chaos-hunt campaign that found the globalXmin livelock, not a separately-constructed scenario. |
| **Why It Matters** | If the assert were compiled out, this would be a genuine silent-corruption path (reading memory/disk content already reused for something else), not just a crash — but Antithesis's target images build core Postgres with `--enable-cassert` (confirmed via `test/antithesis/orioledb/Dockerfile`), so today this guard crashes the process rather than silently passing. The source doc frames the shared-root-cause hypothesis with the globalXmin bug explicitly as "hypothesis, not yet proven." |
| **Scope note** | Like the other undo-retention properties in this catalog (see `sk-fixup-undo-recycling-drop`, `undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant`), this implicitly tests only the `enable_rewind=false` branch of shared undo-retention logic — low-risk since `enable_rewind` is `PGC_POSTMASTER`, not runtime-fragile, and rewind is out of this catalog's scope regardless. |

**Open Questions:**

- Is the shared-root-cause hypothesis (same recovery-xmin/retain-bookkeeping fault as the #889 livelock) correct, or an independent undo-retention-scheduling bug? `(needs human input / needs dedicated investigation — the source doc's own words: "hypothesis, not yet proven")`
- Does this still reproduce at the analyzed commit, given the globalXmin fix chain post-dates the doc's original observation? Not verified — would require a dedicated hunt against current code.

### replica-undo-cleanup-bgwriter-spof — Bgwriter #0 is the sole, un-backed-up point of responsibility for advancing undo retention on a synced replica

**Gap-fill addition (evaluation G3, Coverage Balance + Wildcard lenses).**
`sut-analysis.md` §5/§11 named this risk but it never became a property;
this closes that gap. Not a regression target — a structural single-point-
of-responsibility observation about currently-shipping code.

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | Medium — solid structural SPOF finding in the Streaming Replication category; needs standby topology |
| **Property** | On a streaming/synced replica under sustained write load with no eviction pressure, `minProcRetainLocation`/undo-file cleanup eventually advances (observable via `orioledb_has_retained_undo()` returning `false` once outstanding writes quiesce) within a bounded time — even when the replica's `BGWriterNum == 0` process specifically is disabled, wedged, or repeatedly killed, not merely absent from a clean run. |
| **Invariant** | `Sometimes(bgwriter_0_stall_or_disable_injected)` paired with `Always(orioledb_has_retained_undo_eventually_false_within_bound)`, reusing the oracle already exercised by 13 deterministic replication tests (`test/t/replication_test.py`) — none of which fault-inject bgwriter #0 itself. The honest framing given today's code: this invariant is **expected to fail** when bgwriter #0 is disabled/permanently wedged, since there is no fallback path; the `Sometimes`+`Always` pairing makes that causal link explicit and reproducible rather than assumed. |
| **Antithesis Angle** | Requires a primary+streaming-replica topology. (a) Set `orioledb.debug_disable_bgwriter = true` and confirm `orioledb_has_retained_undo()` never clears despite quiescent writes and passing time — a config-mutation test; (b) more organically, inject a scheduling-delay/CPU-throttling/`SIGSTOP` fault targeting specifically the replica's bgwriter #0 PID during sustained primary DML, confirming retained-undo cleanup stalls in proportion with no alternate path resuming it. |
| **Why It Matters** | `bgwriter.c:200-214`'s "only first bgwriter does this to avoid unnecessary concurrency" comment is a deliberate, avoidable design choice (any other bgwriter, or the leader/startup process, could in principle do this bookkeeping) rather than a structural necessity — `grep -n update_min_undo_locations src/recovery/recovery.c` confirms recovery/replay never calls it directly, making bgwriter #0 the *sole* path on a replica. Unlike `replica-globalxmin-catchup-lag`'s stuck-low-horizon mechanism, this is an avoidable single point of failure with no coded fallback: undo retention pinned indefinitely is an availability/resource-exhaustion failure (unbounded undo-file growth). |

**Open Questions:**

- Is `orioledb.debug_disable_bgwriter` ever set in a real deployment (vs. only the deterministic test suite), making the "disabled" branch realistic outside testing? `(needs human input)`
- What is the realistic worst-case time for bgwriter #0 to become "wedged but alive" under organic write pressure alone (no fault injection)? `(needs further investigation — the eviction loop's own bound, `bgwriter_lru_maxpages * (BLCKSZ / ORIOLEDB_BLCKSZ)`, was located but not measured against realistic workloads)`
- Does anything on the primary side also depend on this exact `BGWriterNum == 0` path, or is the primary's own bookkeeping redundant with backend-commit-driven calls? `(partial: confirmed recovery.c never calls update_min_undo_locations directly, so the replica dependency is exclusive; primary-side redundancy not separately confirmed)`

### recovery-worker-commit-visibility-barrier — Cross-worker commit visibility is gated on the globally-slowest recovery worker, not per-transaction participants

**Gap-fill addition (evaluation G3, Coverage Balance lens).** Turns a claimed
guarantee from `doc/architecture/overview.mdx:153` ("we assume the
transaction to be committed and visible for readers only once all the
workers have completed all the pieces of work associated with that
transaction") into a property, per `validating-claims.md` — treated as a
claim to test, though the enforcement mechanism was traced and found real.

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — traced mechanism is correct today as an over-conservative side effect; risk is forward-looking, needs new instrumentation |
| **Property** | A recovering/replaying process (crash recovery or a streaming standby) never marks an oxid's CSN as committed (transitioning out of `COMMITSEQNO_INPROGRESS`/`COMMITSEQNO_COMMITTING`) before every recovery worker's `commitPtr` has advanced past that commit record's WAL position — i.e., the `finished_list`/`get_workers_commit_ptr()` deferred-CSN-write mechanism in `update_proc_retain_undo_location()` never releases a transaction's visibility ahead of the slowest worker actually finishing replay of everything dispatched up to that LSN. |
| **Invariant** | `Always`: needs new SUT-side instrumentation (this isn't observable from SQL alone) — instrument `update_proc_retain_undo_location()`'s drain loop to assert, at the moment an oxid is drained from `finished_list`, that every worker index in that oxid's `used_by[]` set has a `commitPtr >= this oxid's commit-record LSN`, an `Always()` directly on the invariant the mechanism is supposed to guarantee. |
| **Antithesis Angle** | Needs multiple recovery workers actually receiving pieces of the *same* transaction (a multi-row transaction with PK values hashing to different workers) combined with scheduling-delay/CPU-throttling fault injection targeting one specific worker to maximize leader-vs-slowest-worker skew right as the commit record is processed. A concurrent reader thread repeatedly checking whether all rows of a known transaction become visible together (never some-but-not-all) is the natural client-observable complement to the SUT-side instrumentation. |
| **Why It Matters** | Traced directly (not just accepted from the doc): the mechanism is real but implemented as global minimum-commit-pointer gating across *all* workers, not a targeted per-oxid participant ack — `used_by[]` only routes the commit/rollback control message, it's never consulted for "have all participants finished." It works correctly today as an over-conservative side effect, but a future optimization narrowing the wait to only participating workers could get the completion check wrong, producing a genuine MVCC anomaly (a reader seeing a transaction as fully committed while one of its rows hasn't replayed) — the exact failure class `sut-analysis.md` §10 calls worst-case. Historical precedent: `0cf76e17` ("Fix visibility of xids provided by checkpoint file in recovery workers") confirms this exact code area has produced real bugs before. |

**Open Questions:**

- Is there any existing stopevent or test specifically targeting "reader sees a transaction as committed while one of its rows, dispatched to a different worker, hasn't actually replayed yet"? None found — adjacent tests use the same worker-lag-forcing stopevent for a different purpose (forcing a stale undo-location PANIC). `(partial: adjacent test coverage found, not this specific anomaly)`
- Was `0cf76e17`'s fix ever converted into a permanent regression test, or does the class of bug it fixed remain untested going forward? `(needs further investigation)`
- Since the mechanism is global-WAL-position-gated rather than per-oxid-participant-gated, is there a plausible fault sequence where a non-participating worker's `commitPtr` advances past the commit LSN before a truly participating worker's does? Not identified as reachable, but not exhaustively ruled out. `(needs further investigation)`

### readiness-gate-standby-recovery-lag — pg_isready-based readiness cannot distinguish "caught up" from "far behind" on a future hot standby (corrects a sut-analysis lead for the current topology)

| | |
|---|---|
| **Type** | Reachability / Safety |
| **Priority** | Low — the reachable single-node half is already a passing regression check; the real standby-scoped concern has zero angle until that topology exists |
| **Property** | For the current single-node topology, `pg_isready` (and the harness's `depends_on: condition: service_healthy` gate) structurally *cannot* succeed before OrioleDB's recovery workers have fully drained — the literal "crash recovery just finished but pg_isready says ready anyway" concern in `sut-analysis.md` §5 is refuted by direct tracing of `RmgrCleanup()`/`worker_wait_shutdown()`'s position in the startup sequence. The real, still-open version: on a future hot/warm streaming standby (a currently-missing topology), is there any signal distinguishing "caught up enough to be an interesting fault target" from "still far behind," or does the same readiness gate treat both identically? |
| **Invariant** | `Always()` as a positive regression check for the single-primary topology today (client connection success implies "not in recovery"); the standby-scoped version cannot be implemented or asserted until a standby node exists in the harness. |
| **Antithesis Angle** | None possible today for the real concern — recorded so that whoever builds the top-priority replication-topology workload (`sut-analysis.md` §9) inherits this finding rather than reflexively reusing the existing `pg_isready` healthcheck pattern for a new standby service, which would silently carry the gap forward. |
| **Why It Matters** | On a hot standby, Postgres by design accepts read-only connections while recovery is still continuously ongoing — the entire point of hot standby — so `pg_isready` succeeding tells you nothing about replay lag there, unlike the single-node case. |
| **Testability** | needs harness config — the real (standby-scoped) concern cannot be implemented or asserted at all until the standby topology `deployment-topology.md` recommends actually exists; the single-node half is already a passing regression check today. |

**Open Questions:**

- Does `RmgrCleanup()`'s call site run for every recovery path, including `RECOVERY_TARGET_ACTION_SHUTDOWN`/`PAUSE` early-exit branches that appear to bypass it on that loop iteration? `(partial: plain crash-recovery-to-ready path confirmed; recovery-target/pause/promote-action interactions not fully traced)`
- Does the locally-checked-out patched-Postgres source match what the Antithesis image's Dockerfile actually builds (it fetches a tagged release via `PGTAG`)? `(needs verification)`

---

## Category: Recovery Worker Concurrency & Resource Boundaries

`src/recovery/recovery.c` is the single hottest file in the codebase by commit
count (159 commits per `sut-analysis.md` §8). This category covers the
parallel WAL-apply machinery itself — the `shm_mq`-based leader/worker
protocol, its liveness properties in both directions (leader waiting on
workers, workers waiting on a leader), and the idempotency/dedup assumptions
baked into how WAL records are redispatched and replayed.

### recovery-worker-idxbuild-stall — Recovery workers waiting on a stalled parallel index-build leader have no liveness bound

| | |
|---|---|
| **Type** | Liveness, with a Safety companion (interruptibility) |
| **Priority** | Medium — real unbounded-wait liveness gap, reachable in the existing single-node harness, but needs blocking new instrumentation |
| **Property** | If a parallel index-build leader/worker fails to advance `recovery_index_completed_pos` past a position other recovery workers are delayed on (`delay_if_queued_for_idxbuild()`), those other workers do not block correctness indefinitely — either the stalled leader is detected and recovered from within a bounded time, or an external actor can reliably interrupt and recover the whole recovery process via a signal. |
| **Invariant** | `Sometimes(recovery_worker_entered_idxbuild_wait_loop)` to confirm the contended path is reached, combined with `Always(idxbuild_wait_resolves_or_interrupt_is_honored_within_bound)` under fault injection that kills/delays the index-build leader specifically. |
| **Antithesis Angle** | Trigger parallel-recovery index build (concurrent index creation being replayed) combined with process-level fault injection targeting specifically the index-build leader/worker process (kill it, freeze its scheduling) mid-build — a good target for a process-kill primitive that selectively targets one process among several cooperating ones. **No standby topology is required to reach this**: `orioledb.recovery_pool_size`/`orioledb.recovery_idx_pool_size` both default to 3 (`PGC_POSTMASTER`, confirmed via `src/orioledb.c`), so parallel recovery workers and a parallel index-build sub-pool are already active during ordinary single-node crash recovery, with no config change and no second Postgres node — reachable in the existing single-node harness today. |
| **Why It Matters** | The individual `ConditionVariableTimedSleep` has a 1s timeout and interrupts are checked each iteration — this is not a hard, uncancellable hang — but the *outer* loop has no give-up bound: if `recovery_index_completed_pos` never advances (a crashed/hung leader), the loop polls forever. A stuck recovery process blocks all subsequent WAL replay for that instance — on a future standby this would also stall replication, but the property is fully reachable via plain single-node crash recovery already. |
| **Instrumentation** | **Blocking, not optional** (evaluation, Implementability lens): an exhaustive `stopevents.txt` read confirmed no existing pin-point marks entry into this wait loop or the specific "index-build leader stalled" state — `Sometimes(recovery_worker_entered_idxbuild_wait_loop)` cannot be implemented with precision until one is added. |
| **Antithesis Angle (cheaper alternative)** | Part of the cross-cutting "unbounded busy-wait" pattern (see `property-relationships.md` Cluster 9, alongside `sk-fixup-sentinel-spin-livelock`, `recovery-worker-stall-blocks-leader`, `checkpointer-startup-lock-drain-progress`): before reaching for container-level process-freeze fault injection, a cheaper first test is `pg_cancel_backend()`/`pg_terminate_backend()`/`statement_timeout` targeted at the specific worker process, since interruptibility is a first-class SQL-level concept that exercises `CHECK_FOR_INTERRUPTS()` more directly than inferring it from external timing. |

**Open Questions:**

- Is there any watchdog/leader-liveness mechanism elsewhere in recovery-worker supervision that would detect and recover from a permanently-stalled leader? `(partial: confirmed none inside the wait function itself; broader worker-supervision code not checked)`
- Does Postgres's own background-worker infrastructure provide automatic detection of a hung/crashed worker independent of this specific code path?

### recovery-worker-stall-blocks-leader — A single wedged (not exited) recovery worker silently degrades the whole leader's replay throughput

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | Medium — well-evidenced unbounded busy-wait, independently corroborated by an unmerged fix branch; needs blocking new instrumentation |
| **Property** | If one recovery worker among `recovery_pool_size_guc` becomes slow or wedged (without exiting) — lock contention, I/O stall, CPU starvation — the recovery leader's overall replay progress stalls in proportion, with no timeout-based detection or corrective action distinct from full process-exit detection. |
| **Invariant** | As implemented today there is no detection mechanism at all, so the honestly-checkable framing is `Reachable("leader blocked in worker_queue_flush/workers_synchronize past N seconds while the target worker's PID is still alive")` — confirm the stall condition is reachable and observable at all before a stronger `Always`/`Sometimes` claim about bounded recovery can be asserted meaningfully. |
| **Antithesis Angle** | Inject a stall on exactly one recovery worker (CPU throttling, `SIGSTOP`-then-resume, scheduling delay targeting one specific worker PID) while the leader is actively distributing records to all workers, then observe whether overall recovery throughput — not just that one worker's lane — degrades and for how long. |
| **Why It Matters** | Recovery/replication is already the largest Antithesis coverage gap and the hottest, most bug-dense area of the codebase by commit count. `workers_synchronize()`'s busy-poll has no backoff and no `CHECK_FOR_INTERRUPTS()`; only process-exit is detected, not liveness. Symmetric to `recovery-worker-idxbuild-stall` (workers waiting on a leader) — together these suggest the leader/worker protocol generally lacks wedged-peer detection in either direction. **Independently corroborated** by the evaluation pass's own branch sweep: `workers_synchronize()`'s missing `CHECK_FOR_INTERRUPTS()` was found again, independently, on `origin/nickb/fix_worker_wait_for_sync` (fix `eaeb556f`, dated the same day as the analyzed commit, not an ancestor) — confirming this is a real, well-targeted finding, and one that may already be close to a fix upstream. |
| **Instrumentation** | **Blocking, not optional** (evaluation, Implementability lens): no existing stopevent distinguishes "a worker is progressing slowly" from "a worker is genuinely wedged" — the `Reachable("leader blocked...past N seconds")` framing requires this to be added before it is checkable with precision rather than inferred from wall-clock timing alone. |
| **Antithesis Angle (cheaper alternative)** | Part of the cross-cutting "unbounded busy-wait, no `CHECK_FOR_INTERRUPTS()`" pattern (see `property-relationships.md` Cluster 9, alongside `sk-fixup-sentinel-spin-livelock`, `recovery-worker-idxbuild-stall`, `checkpointer-startup-lock-drain-progress`): `pg_cancel_backend()`/`pg_terminate_backend()`/`statement_timeout` targeted at the specific wedged worker process is a cheaper, more direct SQL-level test of whether `workers_synchronize()`'s busy-poll ever consults `CHECK_FOR_INTERRUPTS()` than inferring it from container-level freeze/resume timing. |

**Open Questions:**

- Is `workers_synchronize()`'s tight busy-poll called frequently enough in normal operation that its lack of backoff/interrupt-check is low-impact, or can DDL-heavy workloads (frequent relnode deletion) trigger it often enough to matter? Not measured.
- Does any *other* leader-side wait path in `recovery.c` have a similar shape (only these two call sites were examined; the file is 5227 lines)?

### recovery-worker-redispatch-consistency — Recovery-worker key routing is deterministic across a crash-restart, except when `recovery_pool_size` changes mid-incident

| | |
|---|---|
| **Type** | Safety (mostly reassuring finding; one residual edge case) |
| **Priority** | Low — mostly reassuring finding with one residual edge case; explicitly flagged lower priority than its category siblings |
| **Property** | A crash mid-replay never causes a WAL record to be re-delivered to a *different* worker than it would have been on any previous replay attempt, given that a recovery-worker crash triggers a full-instance restart-from-checkpoint (not a partial resume) and `GET_WORKER_ID(hash) = hash % recovery_pool_size_guc` is a pure function of the key and the GUC. |
| **Invariant** | `Always(orioledb_tbl_check()/PK-vs-SK consistency)` — same oracle the existing harness already uses, with the added step of changing `orioledb.recovery_pool_size` between deliberate crashes. |
| **Antithesis Angle** | Lower priority than the other properties in this batch. Restart the instance with a different `orioledb.recovery_pool_size` between deliberate crashes during sustained DML + automatic checkpoints (extending `sk-recovery-race-chaos`'s pattern). **No standby topology needed**: `recovery_pool_size` defaults to 3 (`PGC_POSTMASTER`), so multi-worker recovery dispatch is already active on ordinary single-node crash recovery — this is a single-node config-fuzzing property, not a replication-topology-gated one. |
| **Why It Matters** | This investigation was explicitly requested by the assigned focus ("could a crash mid-replay cause a WAL record to be re-delivered to a different worker... and does that worker's overwrite-callback handle that safely?") and largely comes back reassuring — recorded so the question is documented as investigated, not skipped, with one residual variable flagged for a deliberate config-fuzzing pass. |

**Open Questions:**

- Does the postmaster's crash-restart path actually re-read a possibly-edited `orioledb.recovery_pool_size` before respawning the startup process, or is the value pinned from original process start (making the residual edge case unreachable in practice)? Not checked.
- Is there genuinely no per-worker persisted state that survives a crash and assumes stable key-to-worker assignment across restarts? Only the in-memory dispatch function and worker registration flags were checked, not every on-disk artifact recovery workers touch.

### non-modify-wal-record-replay-safety — 16 of 19 WAL record types have no equivalent to the row-modify path's dedup-safety machinery

| | |
|---|---|
| **Type** | Safety (scope gap / lead, not a confirmed bug; **priority corrected upward** — see below) |
| **Priority** | High — priority explicitly corrected upward by the evaluation pass: a broad, cheap-to-reach silent-corruption surface across 84% of WAL record types |
| **Property** | Structural WAL record types (`WAL_REC_TRUNCATE`, `WAL_REC_BRIDGE_ERASE`, `WAL_REC_DATABASE_COPY`, and 13 others) that don't route through the version/oxid-checked dedup callbacks are nonetheless safe to replay twice across a recovery restart, the same way row-modify records are made safely idempotent by design. |
| **Invariant** | `Always(orioledb_tbl_check()/verify_orioledb() passes)` after forcing multiple recovery restarts specifically mid-DDL for these record types. |
| **Antithesis Angle** | Force recovery to restart multiple times (repeated targeted crashes of a recovery worker or the startup process) while `TRUNCATE`, bridge-index-rebuild-erase, or database-move DDL is in flight, then run the structural check. |
| **Why It Matters** | A recovery restart resumes from the same checkpoint's `replayStartPtr` on every attempt — *every* record type between that boundary and the current WAL end gets reprocessed on every restart, not just row-modify records. The row-modify path has purpose-built dedup callbacks precisely because naive re-application isn't safe; the other 16 types were never checked to the same depth. **Priority correction (evaluation, Antithesis Fit lens):** this property was originally underpriced relative to its actual scope — 16 of 19 WAL record types (84%) have no equivalent to the row-modify path's dedup-safety machinery, a broad and cheap-to-reach silent-corruption surface (any repeated-crash-during-DDL workload reaches it, no special topology or timing needed) fully comparable to the checkpoint/recovery top-tier findings elsewhere in this catalog (`sk-fixup-undo-recycling-drop`, `recovery-sk-rebuild-desync`). It should be prioritized alongside that tier, not treated as a lower-priority scope note. |

**Open Questions:**

- Is `o_truncate_table()` idempotent when called on an already-truncated tree? `(needs further code reading — out of scope for the originating pass)`
- Does `replay_erase_bridge_item()` tolerate being asked to erase an already-gone item?
- Does repeated `WAL_REC_DATABASE_COPY` replay across restarts interact with the already-acknowledged `MOVE DATABASE` race comment (`recovery.c:4019`, "XXX there is a race condition here")? `(needs human input / further investigation — only noticed as conceptually adjacent, not mechanistically connected)`

### sk-overwrite-callback-identity-dedup — Secondary-index redo dedup trusts oxid identity alone, not content, unlike the primary-index path

| | |
|---|---|
| **Type** | Safety (lead, not a confirmed bug) |
| **Priority** | Medium — plausible lead, not a confirmed bug; needs blocking new instrumentation to distinguish the dangerous branch at all |
| **Property** | The secondary-index redo dedup callback (`recovery_insert_overwrite_callback`), which skips a write whenever "same oxid, entry already present" regardless of content, never lets a fixup-synthesized SK entry silently win over a genuinely different, later real WAL record for the same (oxid, key) — i.e. the `doc/architecture/overview.mdx:147` idempotency claim ("applying SK changes twice does not affect final state") holds even when the pending-SK-fixup mechanism and ordinary WAL redo can both target the same key. |
| **Invariant** | `Always(existing_hash == incoming_hash)` whenever the overwrite callback's skip branch is taken — requires new instrumentation (a stopevent/counter distinguishing "chose Undo/skip" from "chose Update/apply," which doesn't exist today) to make this checkable at all. |
| **Antithesis Angle** | Repeatedly update the same row's secondary-key value back-and-forth within one transaction, with a checkpoint landing in the PK-applied/SK-pending window on every sub-update (not just once, unlike the existing `sk-recovery-race` driver), maximizing the chance of exercising the divergence path multiple times per transaction. |
| **Why It Matters** | The primary-index callback compares content/version before skipping; the secondary-index callback compares identity only — a strictly weaker check. If the two paths ever diverge in content for the same key, the divergence is a silent, structurally-valid-looking wrong secondary index entry that `orioledb_tbl_check()` cannot detect (it checks tree structure, not payload correctness). |
| **Instrumentation** | **Blocking, not optional** (evaluation, Implementability lens): the Invariant's own "chose Undo/skip vs. chose Update/apply" distinguisher requires new instrumentation that an exhaustive `stopevents.txt` read confirmed does not exist today — this property cannot be checked with precision (only inferred from final-state divergence, which the existing oracle can't detect either, per Why It Matters) until that instrumentation is added. |

**Open Questions:**

- Can a single oxid legitimately produce two different secondary-index target values for the same PK row inside the specific window `checkpoint_write_pending_sk_fixups()` samples? If not, this property may be unreachable in practice.
- Does `orioledb_tbl_check()`/`verify_orioledb()` have any check that would catch a structurally-valid-but-semantically-wrong SK entry, or would this only surface as a wrong query result on an index scan? `(needs human input)`

---

## Category: Checkpointer / Locking Concurrency

The patched checkpointer process now bootstraps deadlock-detection and
catalog/invalidation machinery it never needed in stock Postgres, because it
takes heavyweight relation locks OrioleDB requires. Both properties in this
category concern whether that patched process correctly participates in — and
makes progress through — Postgres's lock manager under adversarial timing.

### checkpointer-heavyweight-lock-deadlock — Checkpointer now takes heavyweight relation locks and must correctly participate in deadlock detection

| | |
|---|---|
| **Type** | Safety / Reachability |
| **Priority** | Medium — severe if triggered, but no confirmed reproduction and no matching existing test found |
| **Property** | When the checkpointer's heavyweight relation lock (`o_tables_rel_lock_extended(..., checkpoint=true)`) conflicts with a concurrent backend's lock on the same relation, the conflict resolves — either the checkpointer proceeds after the other lock releases, or Postgres's real deadlock detector (now correctly bootstrapped in the checkpointer process) breaks a genuine cycle. The checkpointer must never be permanently stuck holding partial checkpoint state while waiting on a lock nobody will release. |
| **Invariant** | `Sometimes(checkpoint_completed_after_lock_conflict)` to prove the interesting case is reached, plus `Always(no_process_wedged_forever)` as a liveness backstop. |
| **Antithesis Angle** | Concurrent DDL (`ALTER TABLE`, `TRUNCATE`) against a table while a `CHECKPOINT` is in flight, with scheduling-fault injection delaying one side just before it would release its lock; existing stopevents (`checkpoint_step`, `checkpoint_table_start`, `checkpoint_index_start`) can pin the checkpointer mid-lock-acquisition. |
| **Why It Matters** | Stock Postgres's checkpointer was never designed to take heavyweight locks or participate in the deadlock graph; the patch bootstraps `InitDeadLockChecking()`/`RelationCacheInitialize()`/`InitCatalogCache()`/`SharedInvalBackendInit(false)` specifically for this. If that bootstrap is subtly incomplete, a genuine deadlock involving the checkpointer could hang forever — a severe availability failure given checkpoints already gate WAL retention and clean shutdown. |

**Open Questions:**

- Has this specific interleaving (checkpointer's heavyweight lock vs. a concurrent DDL statement's opposite-order lock) ever been deliberately tested? No matching isolation/Python test name was found. `(needs human input)`
- Does `NO_LOG_LOCKMETHOD` (used for the checkpointer's `AccessExclusiveLock` acquisitions) change cross-lock-method deadlock-cycle detection semantics relative to a backend using the default lock method on the same relation OID?

### checkpointer-startup-lock-drain-progress — Checkpointer-vs-startup-process lock/sync-queue deadlock-avoidance has a self-documented but untested liveness bound

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | Medium — self-documented hard concurrency seam with no numeric liveness bound; needs standby topology |
| **Property** | On a standby replaying WAL, if the startup process holds `oTablesMetaLock`/`oSysTreesLock` SHARED while blocked on a full sync-request queue, and the checkpointer concurrently wants the same lock EXCLUSIVE, both processes still make forward progress — the checkpointer's `AbsorbSyncRequests()` + retry loop (`acquire_chkp_lock_drain()`) drains the queue so startup unblocks within a bounded number of iterations, not an unbounded stall. |
| **Invariant** | `Sometimes(checkpointer_entered_lock_drain_retry_loop)` combined with `Always(lock_drain_loop_terminates_within_bound)`, tracking iteration count/wall-clock time against a generous bound (the code's own comment claims "a few extra iterations"). |
| **Antithesis Angle** | Requires a standby topology: a primary generating steady DDL/relation-lock activity plus enough concurrent commits to fill the sync-request queue during replay, and a standby under CPU-throttling/scheduling-delay fault injection to widen the window where the startup process is parked mid-WAL-window. |
| **Why It Matters** | This is one of the clearest self-documented "known hard concurrency seam" comments in the codebase — an explicit deadlock scenario and its avoidance strategy — but the claimed liveness bound ("a few extra iterations") has no numeric backing in code: the loop uses a fixed 1ms backoff with no retry cap. If this drain loop's assumption is ever violated, replicas would freeze checkpointing indefinitely — compounding the codebase's other checkpoint-hang findings. |
| **Antithesis Angle (cheaper alternative)** | Part of the cross-cutting "unbounded busy-wait, no `CHECK_FOR_INTERRUPTS()`" pattern (see `property-relationships.md` Cluster 9, alongside `sk-fixup-sentinel-spin-livelock`, `recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-leader`) — unlike its three cluster-mates, this loop's own code *does* call `CHECK_FOR_INTERRUPTS()` each iteration, so a cheaper first test than the standby+CPU-throttling scenario above is a direct `pg_cancel_backend()`/`statement_timeout` against the startup process while it's parked in the drain wait, confirming the interruptibility contract holds before investing in the full adversarial-regeneration scenario. |

**Open Questions:**

- Is there an existing isolation test (`test/specs/*.spec`) or Python test (`replication_test.py`) that already exercises this exact interleaving deterministically? Not confirmed. `(partial: acknowledged the standby-topology gap exists; didn't confirm zero overlap with the existing Python suite)`
- Under adversarial (not just chance) fault injection that keeps regenerating sync requests indefinitely, does the loop still terminate, or does "a few extra iterations" implicitly assume bounded, not adversarial, request generation? This is the crux of what Antithesis should test and is currently unresolved.

### oxid-notify-all-proclock-panic — A hand-rolled lock-manager wait-queue "wake everybody" helper PANICs if it can't re-find its own PROCLOCK

**Gap-fill addition (evaluation G3, Coverage Balance + Wildcard lenses).**
`sut-analysis.md` §11 named `oxid.c:1262`'s PANIC as a lower-priority
trip-wire alongside siblings that *did* become properties; this one didn't.
Grouped here (rather than with the checkpointer-lock properties above)
because it shares this category's general "OrioleDB re-derives part of the
core lock manager's own bookkeeping outside the public API" subject matter,
even though the specific trigger (subxact abort / `INSERT ... ON CONFLICT`)
and lock type (`LOCKTAG_VIRTUALTRANSACTION`, not `LOCKTAG_RELATION`) are
unrelated to the checkpointer.

| | |
|---|---|
| **Type** | Reachability / Safety |
| **Priority** | Medium — fresh, single-node PANIC lead, but concrete reachability of the dangerous sequence is not yet traced |
| **Property** | Whenever `oxid_notify_all()` (called from subxact-abort rollback-to-savepoint and `INSERT ... ON CONFLICT` conflict-resolution paths, to forcibly wake a waiter blocked on the calling backend's own VXID without ending the outer transaction) finds a `LOCK` object registered for the calling backend's own virtual-transaction id, it always also finds that backend's own `PROCLOCK` on that lock — the `elog(PANIC, "failed to re-find shared proclock object")` guard at `oxid.c:1262` is never actually tripped by any reachable sequence. |
| **Invariant** | `Reachable("oxid_notify_all found a registered LOCK for own vxid")` as an exploration hint (confirms the less-common branch — lock exists, has waiters — is exercised, not just the `!lock` early return), paired with `Unreachable("oxid_notify_all PANIC: failed to re-find shared proclock object")` as the actual safety claim — modeled on the same `Reachable`+`Unreachable` pairing already used for `undo-wraparound-retry-cap`. |
| **Antithesis Angle** | Concurrent backends doing `INSERT ... ON CONFLICT DO UPDATE` against a shared, contended PK/secondary-key range (maximizing `wait_for_oxid()`/`VirtualXactLock` contention) combined with nested-subtransaction (`SAVEPOINT`/`ROLLBACK TO SAVEPOINT`) workloads exercising `SUBXACT_EVENT_ABORT_SUB`, under scheduling-delay fault injection widening the window between a waiter registering in the lock's wait queue and the owner calling `oxid_notify_all()`. A pure single-node concurrency property — no replication topology needed. |
| **Why It Matters** | If tripped, the failure mode is an immediate cluster-wide PANIC (crash-restart) triggered by ordinary DML concurrency (savepoints, upserts) — not by fault injection or a rare recovery/replication scenario. The trip-wire guards a lock/proclock consistency assumption OrioleDB re-derives manually by walking `LockMethodLockHash`/`LockMethodProcLockHash` directly, rather than enforcing it via `LockAcquire`/`LockRelease` — exactly the kind of narrow, hand-rolled concurrency surface likely to have an edge case its author didn't anticipate. No git history hit for "proclock"/"notify_all" in any commit message — a genuinely fresh lead, not a rediscovery of known history. |

**Open Questions:**

- Is the `!lock` early-return's "`/* Must be granted with fast path */`" comment actually wrong (does `LOCKTAG_VIRTUALTRANSACTION` really never use Postgres's fast-path locking), making the proclock-PANIC branch the operationally relevant one whenever a waiter is present? `(needs further investigation — reasoned from general Postgres lock-manager design; not independently re-verified against this patched tree's exact registration call site, since `orioledb_postgres` is out of scope)`
- Concretely, what sequence of events could cause the calling backend's own `PROCLOCK` to go missing while its `LOCK` still exists? Candidates (a race with the deadlock detector or `RemoveFromWaitQueue()`; a hashcode/partition-lock mismatch) were not traced to a concrete reachable sequence. `(needs further investigation — the central unresolved question for whether this is realistically triggerable or purely defensive)`
- Does the asymmetry with `oxid_notify()`'s sibling logic (works directly off `GetPGProcByNumber()`, skips the proclock re-find entirely, "no existing callers" per its own comment) suggest `oxid_notify_all()`'s manual re-find is defensive/paranoid rather than load-bearing, or the reverse? `(needs further investigation)`

---

## Category: Undo Log and MVCC Resource Boundaries

Undo is the backbone of OrioleDB's entire MVCC/rollback model — a circular,
bounded shared buffer whose sizing assumptions are enforced mostly via
`Assert()`. Both properties here concern whether concurrent demand on that
buffer can ever exceed its sizing assumptions, either through legitimate batch
operations or through adversarial scheduling around the buffer's wrap boundary.

### multi-insert-undo-capacity-invariant — Batched multi-row insert's undo-reservation cap assumes at most `max_procs` concurrent reservations

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — solid Assert-guarded invariant; cassert build already converts a violation into a crash rather than silent corruption |
| **Property** | Under concurrent batched multi-row inserts from up to `max_procs` backends, (a) the per-backend undo reservation never exceeds `2 * O_MAX_UNDO_RECORD_SIZE` (the `Assert()`-checked invariant in `undo.c:1879-1880` never trips), and (b) the Phase-2 monotonicity check + qsort fallback never lets an out-of-order key reach the leaf-probe helper, which would silently corrupt the downlink invariant per the code's own comment. |
| **Invariant** | `Always(orioledb_tbl_check_passes_after_concurrent_multi_insert)`, run periodically during/after a workload with intentionally non-monotone explicit-PK ordering (to force the qsort fallback) from many concurrent backends. |
| **Antithesis Angle** | Many simultaneous backends doing large `COPY`/multi-row `INSERT` batches with non-sequential PK values, scheduling delays around the per-iteration `reserve_undo_size()`/leaf-probe boundary — the interesting adversarial angle is whether background workers not counted in `max_procs` (bgwriter, autovacuum-equivalent, etc.) can also hold same-type undo reservations concurrently with backend multi-inserts, undersizing the shared buffer relative to actual demand. |
| **Why It Matters** | A shared-bounded-buffer-vs-concurrent-demand mismatch of this kind produces silent corruption, not a clean error — one backend's undo data could be overwritten before another backend expected it to be, a severe and hard-to-diagnose data-integrity failure given undo's central role. The `Assert()`-only enforcement is not a build-flag risk in practice: Antithesis's target images build core Postgres with `--enable-cassert` (confirmed via `test/antithesis/orioledb/Dockerfile`), so this guard already crashes the process rather than silently passing in the harness's actual build. |
| **Scope note** | Like the other undo-retention properties in this catalog (see `sk-fixup-undo-recycling-drop`, `undo-wraparound-retry-cap`, `replica-undo-reclaimed-too-early`), this implicitly tests only the `enable_rewind=false` branch of shared undo-retention logic — low-risk since `enable_rewind` is `PGC_POSTMASTER`, not runtime-fragile, and rewind is out of this catalog's scope regardless. |

**Open Questions:**

- Does `max_procs` (the sizing input to the undo buffer floor) include every process type capable of holding an undo-type reservation, or only regular backends? `(needs human input — the definition/assignment site of `max_procs` was not traced)`
- Has the Phase-2 monotonicity-verify scan and qsort fallback ever been fuzzed against adversarial (not just accidentally-unsorted) key sequences designed to probe comparator edge cases?

### undo-wraparound-retry-cap — Circular undo buffer's wraparound retry assumes at most one retry per call, under concurrent global allocation

| | |
|---|---|
| **Type** | Reachability / Safety |
| **Priority** | Medium — always-compiled PANIC guard, but reachability of the double-wrap condition is unproven, not disproven |
| **Property** | `get_undo_record()`'s circular-buffer wraparound retry never needs a second retry in the same call under concurrent, adversarial scheduling — the `elog(PANIC, "not enough reserved undo...")` guard is never actually tripped by the wraparound path (as opposed to a genuine caller bug). |
| **Invariant** | `Reachable("undo wraparound retry taken")` as an exploration hint (confirms the buffer-wrap boundary is actually being exercised, currently invisible from outside), paired with `Unreachable("get_undo_record PANIC: not enough reserved undo")` as the actual safety claim. |
| **Antithesis Angle** | Concurrent writers against a table with a small/near-floor `undo_circular_buffer_size` and low `max_procs` to make wrap-boundary crossings frequent, combined with scheduling-fault injection maximizing the chance that other backends' concurrent allocations land two wraps within one call's short retry window — a pure single-process test cannot exercise this at all. |
| **Why It Matters** | If reachable, the failure mode is a backend/instance PANIC (crash), not silent corruption — a real availability hit, and notably this guard is *always compiled in* (not `Assert()`-gated), making it a legitimate Antithesis target regardless of build flags, unlike several other invariants in this catalog. |
| **Scope note** | Like the other undo-retention properties in this catalog (see `sk-fixup-undo-recycling-drop`, `multi-insert-undo-capacity-invariant`, `replica-undo-reclaimed-too-early`), this implicitly tests only the `enable_rewind=false` branch of shared undo-retention logic — low-risk since `enable_rewind` is `PGC_POSTMASTER`, not runtime-fragile, and rewind is out of this catalog's scope regardless. |

**Open Questions:**

- Is the undo circular buffer's sizing floor (`max_procs * 2 * O_MAX_UNDO_RECORD_SIZE`) provably sufficient to make a double-wrap for one backend's single call combinatorially impossible, or merely empirically rare? `(partial: floor formula located, no proof of sufficiency examined)`
- Does `O_MODIFY_UNDO_RESERVE_SIZE`'s constant already account for a worst-case single wrap consumed by the first of two `get_undo_record()` calls sharing one reservation pool, leaving the second call's 2x cushion intact? Not traced through the arithmetic.

---

## Category: Resource Boundaries and Infrastructure Faults

**New category (gap-fill addition, evaluation G1, Coverage Balance lens).**
Before this pass, only 2/38 properties targeted resource limits, and both
were scoped to the undo buffer (see the category above) — despite
`sut-analysis.md` §9 naming infra-level faults (disk exhaustion,
shared-memory/backend OOM, process-count exhaustion) as squarely
Antithesis's unique value versus the existing deterministic test suite. The
four properties below fill that gap: two are concrete, directly-confirmed
control-flow bugs (process-count/worker-slot exhaustion); two are
verified-correct contracts worth a live regression check (disk exhaustion,
page-pool exhaustion). None require a replication topology — all four are
reachable in the existing single-node harness via config/GUC changes alone.

### recovery-idxbuild-registration-fallback-bug — The parallel index-build worker pool's registration-failure fallback is missing a `break` and has an off-by-one, unlike its sibling recovery-worker pool

| | |
|---|---|
| **Type** | Safety / Liveness (concrete control-flow bug, directly confirmed by reading; end-to-end runtime consequence not yet empirically reproduced) |
| **Priority** | High — concrete, directly-confirmed control-flow bug (missing break + off-by-one) reachable today via ordinary config |
| **Property** | When Postgres's background-worker slots (`max_worker_processes`) are exhausted partway through registering the parallel index-build worker sub-pool during crash recovery, the leader falls back to a self-consistent single-process (or reduced-pool) index-build mode — analogous to, and exactly as safe as, the main recovery-worker pool's already-correct fallback (`recovery_single = true` + `break`) — rather than leaving `workers_pool` state inconsistent, corrupting an unrelated pool slot's `queue` field, or later attempting to dispatch index-build work to a worker pool whose computed valid-index range is empty. |
| **Invariant** | `Always(index_build_pool_state_consistent_after_registration_failure)`: after any WARNING-logged index-build worker registration failure, assert (a) `recovery_idx_pool_size_guc` and the macro-derived `index_build_last_worker` describe a non-negative-size range consistent with what was actually registered, and (b) no `workers_pool[i].queue` for `i != index_build_leader` was ever assigned a `shm_mq_handle` obtained via `GET_WORKER_QUEUE(index_build_leader)`. Complemented by `Reachable("index-build worker registration fallback taken")` since this path has zero visibility today. |
| **Antithesis Angle** | Configure a low `max_worker_processes` relative to `orioledb.recovery_pool_size + orioledb.recovery_idx_pool_size` (both default to **3**, confirmed via `src/orioledb.c`, so both pools are active by default — not an opt-in configuration) so registration is guaranteed to exhaust available slots partway through the index-build sub-pool specifically. Trigger a crash with a concurrent `CREATE INDEX`/parallel-index-build-eligible DDL in flight so recovery actually attempts to spin up the index-build pool. A pure resource-limit workload, no fault injection beyond config + a crash. |
| **Why It Matters** | The main recovery-worker pool has a correct, deliberate degradation path for exactly this resource limit — but the structurally near-identical index-build pool's copy of that logic is missing the loop-terminating `break` and uses an off-by-one fallback value (`recovery_idx_pool_size_guc = 1` yields an *empty* valid-index range given the macro's arithmetic, not the intended one-worker pool). Directly confirmed by reading: the inner cleanup loop reuses the outer loop's own `i`, driving it to `index_build_first_worker - 1` before a stray `shm_mq_attach()` call executes using the leader's own (never-populated) handle and writes into the *wrong* pool slot. The plausible failure surface ranges from a harmless stray write to a stuck/hung recovery leader if later code dispatches index-build work assuming an attached pool that never was. Nothing in the existing test suite or Antithesis harness exercises `orioledb.recovery_idx_pool_size` against a constrained `max_worker_processes` at all. |

**Open Questions:**

- Does any later code path (index-build work dispatch to the leader's sub-workers) actually get reached with `recovery_idx_pool_size_guc == 1` but zero attached workers, and if so does it hang, error out, or silently no-op? `(needs further investigation — the dispatch-side code was not traced to a conclusion this pass)`
- Is the stray `shm_mq_attach()` call ever consequential, or is `workers_pool[index_build_leader]`'s own state re-established correctly elsewhere later, making this a harmless clobber of an already-unused slot? `(needs further investigation)`
- Does `RegisterDynamicBackgroundWorker()`'s failure mode ever partially succeed (a non-`NULL` handle for a worker that nonetheless never actually starts)? Not traced — assumed all-or-nothing per call, per documented behavior.

### bgwriter-worker-slot-exhaustion-silent — OrioleDB's statically-registered bgwriter worker(s) have zero fallback or self-detection if `max_worker_processes` denies their slot at postmaster start

| | |
|---|---|
| **Type** | Liveness (silent degradation), with a Reachability companion |
| **Priority** | Medium — real zero-fallback design gap, but only reachable via a specific max_worker_processes misconfiguration |
| **Property** | If `max_worker_processes` is insufficient to grant a background-worker slot to every one of OrioleDB's statically-registered bgwriter workers at postmaster start, the instance still starts (matching stock Postgres's documented "skip the worker, log a warning, keep running" behavior) — but OrioleDB itself never running any bgwriter for the rest of that postmaster's lifetime is neither retried nor surfaced as an OrioleDB-specific, alertable condition, and specifically the `BGWriterNum == 0` replica-undo-retention-advancement responsibility (`update_min_undo_locations`, see `replica-undo-cleanup-bgwriter-spof`) never executes at all, not merely less often. |
| **Invariant** | `Sometimes(zero_bgwriters_running_after_postmaster_start)` to confirm the exhaustion condition is reached, paired with `Always(minProcRetainLocation_advances_within_bound_when_at_least_one_bgwriter_is_running)` as a positive control, and `Sometimes(minProcRetainLocation_stalls_permanently_when_zero_bgwriters_running)` to make the degraded state's actual, observable consequence checkable rather than merely inferred from "the process isn't in the list." |
| **Antithesis Angle** | A pure resource-limit workload — configure `max_worker_processes` low enough (or hold other slots busy via `max_parallel_workers`/the recovery pools above) that OrioleDB's `bgwriter_num_workers` slot request(s) are denied at startup, then run sustained DML + periodic checkpoints (ideally with a streaming standby, given the replica-cleanup framing) and watch undo retention/file size grow unboundedly versus a control run. Antithesis's fault injection adds value on top by also covering the *dynamic* loss of the sole running bgwriter (kill the specific PID mid-run) as a runtime-triggered variant of the same "zero bgwriters running" state. |
| **Why It Matters** | Unlike the dynamic recovery-worker pools (`recovery-idxbuild-registration-fallback-bug`), which at least detect and log their own registration failure, `RegisterBackgroundWorker()` (the stock, static registration API this call uses) returns `void` — OrioleDB has **zero visibility** into whether its bgwriter's slot request succeeded, and there is no fallback code path at all. A user or automated deployment tool who sets `max_worker_processes` without accounting for OrioleDB's static + dynamic worker needs gets a running, seemingly healthy instance whose replica undo-cleanup liveness guarantee has quietly stopped holding, discoverable today only by noticing unbounded undo growth well after the fact. |

**Open Questions:**

- Does Postgres's static-bgworker-registration slot-exhaustion behavior hold identically in the patched Postgres core this project depends on? This pass deliberately relied on well-established stock `bgworker.c` behavior rather than consulting `orioledb_postgres`, per this pass's scope restriction. `(needs human input / needs a live repro to confirm against this specific patched build)`
- Is there any other, indirect signal (a metric, a `pg_stat_activity` row count) by which an operator could detect "zero orioledb bgwriters running" today, short of grepping startup logs? Not found in `doc/` or `src/` this pass — assumed absent, not exhaustively confirmed.
- How large is the practical exposure — does any common `max_worker_processes` consumer plausibly coexist with OrioleDB's default worker footprint (up to 7 slots: 1 bgwriter + 3 + 3 recovery workers) closely enough that a realistic deployment could hit this without deliberately misconfiguring `max_worker_processes`? Not measured.

### disk-write-enospc-fails-safe — OrioleDB's own on-disk writes (B-tree pages, undo buffers, checkpoint control/xid files) consistently fail safe on disk-full, but the contract is completely untested

| | |
|---|---|
| **Type** | Safety (verified-correct contract via static analysis across ~15+ call sites; the CRC-torn-write angle is a corollary, not independently re-derived by a live ENOSPC repro this pass) |
| **Priority** | Medium — verified-correct contract worth a live regression check; not itself a confirmed bug |
| **Property** | A disk-full (`ENOSPC`) condition encountered while OrioleDB writes any of its own on-disk artifacts (B-tree pages, undo-buffer eviction writes, the checkpoint control file, or checkpoint-time xid/free-extent/map files) is always detected via the actual write/sync return value — never silently treated as success — and escalates to a `FATAL`/`PANIC` carrying the real `errno`, rather than leaving a torn or partially-written file that a later read could misinterpret as valid. If the interrupted write specifically hits the checkpoint control file, the pre-existing CRC check catches the resulting torn mix of old/new bytes on the next startup read rather than accepting it. |
| **Invariant** | `AlwaysOrUnreachable(disk_write_failure_produces_FATAL_with_ENOSPC_errno, never_silent_success)` — best implemented as a deliberate low-disk-quota workload rather than waiting for organic `ENOSPC`. Paired with `Always(control_file_crc_check_rejects_a_deliberately_truncated_control_file)` as a narrower, directly-constructible regression test (write a valid control file, truncate/corrupt its tail, confirm `check_checkpoint_control()` raises rather than accepting it) — doesn't need real `ENOSPC` to falsify the CRC-catches-torn-writes half of the claim. |
| **Antithesis Angle** | Run the existing `sk-recovery-race-chaos`-style sustained DML + automatic-checkpoint workload against a data directory mounted on a deliberately small/quota-limited filesystem, so `write_page_to_disk()`/`write_checkpoint_control()`/the checkpoint xid-file writers are guaranteed to eventually hit `ENOSPC` under real concurrent load and adversarial timing (mid-checkpoint, mid-page-eviction, mid-undo-write). Confirm the instance always crash-restarts cleanly and recovery afterward resumes correctly from the last valid checkpoint. |
| **Why It Matters** | Disk-space exhaustion is explicitly named in `sut-analysis.md` §9 as unexercised, high-value Antithesis territory. Every one of ~15+ checked call sites (B-tree page writes, undo-buffer writes, checkpoint control file, 11 distinct checkpoint xid/free-extent/map-file writers) follows the same correct idiom: check the actual byte count/sync return, never assume success, escalate loudly with the real `errno`. But "verified correct by reading 15+ call sites once" is exactly the kind of claim that silently rots — a future call site added without copying the same check-and-FATAL idiom would reintroduce the worst possible failure class (silent data loss/corruption) for this project, and no existing test — deterministic or Antithesis — exercises real disk-full conditions at all. |

**Open Questions:**

- Does the Antithesis platform have a "disk full"/storage-quota fault-injection primitive, and if so how does it interact with a containerized data directory? `(needs human input from whoever operates the harness/platform)`
- Are there OrioleDB-managed write call sites beyond the ones enumerated (B-tree pages, undo buffers, checkpoint control/xid/map files) that weren't checked — e.g. logical-decoding temp files, `WORKER_UNDO_TEMP_FILE` recovery-worker temp files, or TOAST-related writes? `(partial: the highest-traffic, highest-blast-radius paths were checked; not exhaustively enumerated)`
- Is a torn-write on the checkpoint control file ever *not* caught by the CRC check (e.g. a coincidental CRC collision, or non-sequential OS/filesystem write ordering)? Treated as astronomically unlikely, not formally bounded this pass.

### page-pool-exhaustion-fails-safe — OrioleDB's in-memory B-tree page pool fails safe on exhaustion in the common case, but a documented nested-recursion assumption is untested

| | |
|---|---|
| **Type** | Safety (verified-mostly-correct contract; the multi-pool nested-recursion edge case is a genuine, unresolved lead rather than a confirmed bug) |
| **Priority** | Medium — verified-mostly-correct contract; the nested-recursion edge case is Assert-only and untested |
| **Property** | When OrioleDB's in-memory page pool (`OPagePool`, `orioledb.main_buffers` — distinct from Postgres's own `shared_buffers` and from the undo circular buffer covered elsewhere in this catalog) is genuinely exhausted under concurrent load, every backend attempting to reserve pages eventually receives a clean, catchable `ERROR` (`ERRCODE_OUT_OF_MEMORY`, "orioledb page pool is exhausted") — never a hang, a crash, or proceeding without having actually reserved the space it needed — and this holds even when the exhaustion is discovered through a nested reservation call for a different pool type (main pool eviction needing a TOAST/free-tree/catalog page from a second pool) rather than a single flat reservation. |
| **Invariant** | `Always(page_pool_exhaustion_yields_clean_ERROR_never_hang_or_crash)`, confirmed by a workload reserving enough concurrent pinned/dirty pages across enough distinct backends to guarantee `o_ppool_run_maintenance()`'s bounded clock-sweep finds nothing evictable. Paired with `Sometimes(nested_pool_reservation_path_taken_during_exhaustion)` targeting the `ppool_run_clock_depth > 0` branch specifically, to confirm the harder TOAST/free-tree/catalog nested case is actually exercised at all. |
| **Antithesis Angle** | A table with a TOASTed column and at least one secondary index (guaranteeing catalog/free-tree/TOAST page-pool traffic interleaves with main-pool traffic) plus a small `orioledb.main_buffers` and many concurrent long-running transactions each touching many distinct tables/rows. Antithesis's scheduling-fault injection widens the window between one backend's reservation-failure retry and another's eviction, maximizing the chance the nested multi-pool-type recursion path is actually reached. |
| **Why It Matters** | This is the concrete, checkable answer to "what happens when shared memory allocation for undo/B-tree structures fails under memory pressure" the evaluation gap asked for. The common case is well-engineered (a bounded clock-sweep with a genuine, non-infinite give-up signal) — but the code's own comments describe a nested-recursion depth assumption (`Assert(ppool_run_clock_depth <= 1)`, `Assert(pool != outer_pool)`) guarded only by `Assert()`, not a live check. If violated under real concurrent adversarial exhaustion across multiple pool types, the failure mode in a release build is unknown — potentially silently-wrong bookkeeping state rather than the intended clean-ERROR path. |

**Open Questions:**

- Can a real workload drive the nested (`ppool_run_clock_depth > 0`) recursion path concurrently with the *same* pool also being independently exhausted from a different backend, stressing the depth/identity assumptions rather than merely reaching the nested branch harmlessly? `(needs further investigation)`
- In a release build, if the recursion-depth or pool-identity assumption is violated, does `skip_ucm` bookkeeping (process-local state guarding a global flag) get left inconsistent for a *different* concurrent caller? `(needs further investigation — the crux of whether the Assert-only guard is load-bearing)`
- Does any GUC combination make genuine exhaustion realistically reachable under ordinary heavy concurrent load with default settings, or only via a deliberately tiny `main_buffers`? Not checked — the pool-sizing GUCs' own floor/minimum logic was not cross-checked against realistic concurrent-transaction-count scenarios.

---

## Category: WAL Format and Version Compatibility

OrioleDB registers its own binary WAL resource manager, read end-to-end by
two consumers today (crash recovery and logical decoding) plus a
display-only `pg_waldump` path. This category covers the version/format
contracts governing that shared parser: whether newer, older, and malformed
containers are all rejected or converted exactly as documented, on every
reading path, plus two adjacent binary-format contracts (checkpoint-control
file and on-disk page versioning) and one concrete ordering bug found while
tracing them.

### wal-recovery-rejects-future-version — Crash-recovery redo correctly rejects a WAL container newer than the binary understands

| | |
|---|---|
| **Type** | Safety (verified-correct contract; regression-guard value) |
| **Priority** | Medium — verified-correct regression-guard contract, actionable today via bit-flip fault injection without a build-matrix change |
| **Property** | The crash-recovery redo path never hands a WAL container from a version newer than the running binary understands to a record parser — recovery aborts cleanly (`elog(ERROR)` inside the redo callback, cluster refuses to come up) rather than misinterpreting bytes, matching the documented contract exactly. |
| **Invariant** | `AlwaysOrUnreachable` — verified correct today by tracing the full call chain (`wal_container_read_header` → `replay_check_version` → `orioledb_redo`'s handling of a `false` return), but not organically reachable in the current single-binary-version harness. |
| **Antithesis Angle** | Requires either a deliberately constructed two-version-in-one-run harness config, or direct bit-flip fault injection on the version tag byte of an in-flight WAL record. |
| **Why It Matters** | A regression here (a future WAL-format change adding fields without properly gating them by `container.version`) would let replay interpret bytes belonging to unknown fields as the next record's tag byte — silent stream desynchronization corrupting arbitrary subsequent replay, not just the one incompatible record. The "warn-only, falls through" branch in the shared header parser looked, on first read, like a possible gap; tracing it to its actual caller shows the gap is closed by an outer re-check specific to the recovery path. |
| **Testability** | needs harness config — the two-version-in-one-run harness variant doesn't exist today; the direct bit-flip-fault-injection alternative is buildable now with existing Antithesis primitives and doesn't require a build-matrix change, only a workload targeting the version-tag byte specifically. |

**Open Questions:**

- No config in `test/antithesis/` runs two different OrioleDB binary versions against the same data directory/WAL stream — without that, this stays a static-analysis-verified contract, likely to sit at "never reached" for a long time. `(partial: mechanism verified by static analysis; no reachability path identified in the current harness)`

### wal-decode-rejects-future-version — Logical decoding correctly rejects a WAL container newer than the binary understands

| | |
|---|---|
| **Type** | Safety (verified-correct contract; sibling to the recovery-side property) |
| **Priority** | Low — verified-correct contract with no logical-decoding consumer in the harness at all today |
| **Property** | Logical decoding of a WAL container from a version newer than supported throws a normal, session-scoped `ERROR` before any record payload bytes are interpreted — not a crash, matching the documented "logical decoding will fail and throw error, cluster continues" contract. |
| **Invariant** | `AlwaysOrUnreachable` — same reachability caveat as the recovery-side sibling; this specific outer-FATAL-vs-inner-ERROR distinction (see `wal-decode-malformed-container-fails-safe`) was traced to confirm the version-mismatch case takes the `ERROR` path, not the `FATAL` catch-all. |
| **Antithesis Angle** | Same as the recovery-side property, applied to a logical-decoding consumer — which does not exist in the harness today (would need to be added; `sut-analysis.md` §9 flags this generally). |
| **Why It Matters** | If `decode_check_version` were ever bypassed or its comparison inverted, a decoding backend would proceed to parse a payload layout it doesn't understand, given the parser's explicitly single-pass, no-lookahead design — a corrupted logical-replication stream rather than an early, attributable failure. |
| **Testability** | needs harness config — no logical-decoding consumer exists in the harness at all today; see R10/`deployment-topology.md`'s new Open Questions bullet on adding a logical-replication subscriber client. |

**Open Questions:**

- No reachability path in the current harness. `(partial: mechanism verified by static analysis only)`
- Could `elog(ERROR)` here, under some walsender configuration with no retry path, cascade into a slot being marked unusable indefinitely (a liveness question, distinct from the correctness question above)? Not traced — flagged as a follow-up if logical replication is ever added to the harness.

### wal-decode-malformed-container-fails-safe — Structural WAL corruption reaching logical decoding is FATAL, a harder failure than the documented contract implies

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Low — same missing-consumer gap as its sibling; not reachable until a logical-decoding consumer exists |
| **Property** | Structural corruption of a WAL container reaching logical decoding (unknown record tag, truncated payload) is handled with `elog(FATAL)` — tearing down the decoding backend entirely — rather than the recoverable `ERROR` the version-mismatch case uses; still backend-scoped (not a full-cluster crash), but operationally harder than the documented "throw error, cluster continues" framing implies for this specific failure shape. |
| **Invariant** | `Always(other_backends_continue_serving_queries_after_the_decoding_backends_FATAL)` (rules out full-cluster escalation) + `Always(no_logically_decoded_output_from_a_corrupted_record)` (rules out a silent wrong-decode). |
| **Antithesis Angle** | Real WAL-page/segment bit-flip fault injection targeted at a running logical-decoding slot — requires adding a logical-replication consumer to the harness, which doesn't exist today. |
| **Why It Matters** | The realistic trigger is on-disk or in-transit WAL corruption — exactly the kind of fault Antithesis's disk/memory fault injection produces. The severity difference (`FATAL` tearing down a session vs. a recoverable `ERROR`) matters operationally for anything polling that connection. |

**Open Questions:**

- Does `elog(FATAL)` in a logical-decoding backend ever cascade to a `HandleChildCrash`-style full-cluster restart under the patched Postgres, or does it always stay backend-scoped as in stock Postgres? `(needs human input or a live repro)`
- Could bit-level corruption of a record tag or version byte ever produce a valid-looking-but-wrong record/version rather than hitting the structural error paths — likely already ruled out by Postgres's own WAL-record CRC (which runs before OrioleDB's parser sees the bytes), but not independently confirmed for this patched xlog reader path. `(partial)`

### wal-older-version-seamless-conversion — The documented "older WAL version converts seamlessly" claim is real but untestable in the harness's current `IS_DEV=1` build

| | |
|---|---|
| **Type** | Safety / Meta (build-configuration gap; **priority lowered** — see below) |
| **Priority** | Low — explicitly build-variant-blocked: unreachable under the harness's unconditional IS_DEV=1 build, priority lowered by the evaluation pass |
| **Property** | WAL from an older, still-supported version (16, vs. current 17) is converted seamlessly at read time by every consumer, per the documented directional contract — genuinely implemented via per-record `>= 17` field gates in `wal_reader.c`, not a hollow doc claim. But it can only ever be exercised in a non-`IS_DEV` (production-style) build, and the Antithesis harness's Dockerfile builds `IS_DEV=1` unconditionally, under which *both* directions (including the legitimately-convertible older-version case) hit an intentional `elog(FATAL, "...Intentionally fail tests")` instead. **Confirmed independently by the evaluation pass** (Implementability lens): `wal_reader.c:420-447`'s `IS_DEV` branch is symmetric, FATALing on both the newer- and older-version case, and `test/antithesis/orioledb/Dockerfile:174-177` builds `IS_DEV=1` unconditionally on both `orioledb.so` compile steps — this is not fixed by the planned standby addition either, since a standby uses the identical image/build. |
| **Invariant** | Once reachable: `Always` — WAL written by an older-`ORIOLEDB_WAL_VERSION` build and replayed by a newer build produces byte-identical logical results as if replayed by the version that wrote it. Needs SUT-side instrumentation (a stopevent/counter recording how many records were actually converted) since "conversion happened and was correct" isn't observable from SQL alone — this would be the first SUT-side assertion in the codebase. |
| **Antithesis Angle** | Not reachable via workload/fault-injection variety alone. Requires either (a) a second orioledb build variant compiled without `IS_DEV` for a mixed-version scenario, or (b) a supported "poison"/override mechanism to force the older-version branch under `IS_DEV` for testing (does not currently exist). Recommend flagging to `antithesis-workload` as a build-matrix gap. |
| **Why It Matters** | This path has **zero test coverage today, in any build mode** (confirmed via grep across `test/t`, `test/sql`, `test/specs`, `test/antithesis` — no hits for `ORIOLEDB_WAL_VERSION`/`wal_version`). A bug in the per-record conversion gates (missing a field, wrong threshold, wrong substituted default) would silently apply wrong data rather than fail loudly — exactly the worst-case failure class for this project — and nothing would catch it. |
| **Testability** | needs build variant — structurally unreachable under the harness's current `IS_DEV=1` build; requires either a second non-`IS_DEV` build variant or a supported test-only override, neither of which exists today. **Priority correction (evaluation, Implementability lens):** because this property cannot be exercised at all until a build-matrix change lands, its near-term priority is lowered relative to properties reachable in the harness as it exists today — it should be flagged to whoever owns the harness build matrix as "blocked on build-matrix change," not left looking like an ordinary ready-to-build fault-injection target. |

**Open Questions:**

- Is a non-`IS_DEV` orioledb build variant feasible to add without breaking other existing workloads that depend on `IS_DEV`-only test hooks (`pg_stopevent_set()` is likely `IS_DEV`-gated)? `(needs human input — depends on harness build-matrix decisions)`
- Is a genuinely mixed-version WAL stream (old-version records followed by new-version records from the same binary after an in-place upgrade) even a real supported scenario for this project, or is `ORIOLEDB_WAL_VERSION` only meant to matter for a reader running different software than the writer? `(needs human input)`

### malformed-wal-container-double-finish — A commit-flow error can produce a WAL container with two finish records for the same oxid, crashing a replica (orioledb#865, fixed — residual variant unclear)

| | |
|---|---|
| **Type** | Safety (regression guard; residual variant status unclear) |
| **Priority** | Medium — regression guard on an already-fixed bug (#865); residual variant status against the shipped fix is unclear |
| **Property** | An `ereport(ERROR)` firing between a commit's `add_finish_wal_record(WAL_REC_COMMIT, ...)` and the following `flush_local_wal()` never produces a single on-disk WAL container carrying two finish records (`COMMIT` and `ROLLBACK`) for the same oxid — which crashes a streaming replica via `Assert("rec->oxid != InvalidOXid")` at `src/recovery/recovery.c:3712`. |
| **Invariant** | `Unreachable("WAL container with two finish records for the same oxid")` on the replica/recovery decode side — directly instrumenting the exact condition rather than waiting for the assert to crash the process, giving a signal even in non-assert builds. |
| **Antithesis Angle** | Needs a streaming-standby topology. Inject/force an error inside the commit-flow window on a primary with an attached standby (via a stopevent, an assertion, or Antithesis's own fault injection), assert the standby never TRAPs and never silently diverges from the primary's final row/aggregate state. |
| **Why It Matters** | A fix (`7d04814b`, then broadened by `4f4c365a`) is present at the analyzed commit, confirmed via `git merge-base --is-ancestor`. But the broader four-failure-mode investigation this bug came from (`streaming_replica_issue.md`, 575 lines) documents a *residual* rare (~5%) replica recovery-livelock variant that was studied against an earlier, different implementation (`200073b5`) than what actually shipped (`4f4c365a`) — whether the residual applies to the currently-shipped fix is unknown. |
| **Instrumentation** | **Blocking, not optional** (evaluation, Implementability lens): the Invariant's `Unreachable("WAL container with two finish records for the same oxid")` needs a direct instrumentation point on the replica/recovery decode side confirmed absent by an exhaustive `stopevents.txt` read — without it, this can only be observed indirectly via the replica's `Assert()` TRAP itself, giving no signal in a non-assert build and no positive confirmation the dangerous condition was ever *approached* without tripping. |

**Open Questions:**

- Does `4f4c365a`'s restructured critical section (wrapping the whole `undo_xact_callback` commit branch) actually close all four bug classes the broader doc enumerates, or only the one (`#1`) the narrower, earlier fix targeted? `(partial: fix commit for the exact #865 shape confirmed present and its intent matches; broader four-bug-class closure not independently re-verified against the current diff)`
- Does the doc's residual finding (Bug #2 class changing from "silent divergence" to a rare replica recovery-livelock post-fix) apply to the currently-shipped `4f4c365a` implementation, given it was investigated under a different, non-shipped implementation (`200073b5`)? `(needs re-investigation against 4f4c365a specifically, or a dedicated hunt)`

### checkpoint-control-version-gate-fails-safe — Checkpoint-control-file version/CRC/s3Mode gates are independent and correctly ordered

| | |
|---|---|
| **Type** | Safety (verified-correct contract; one secondary severity-asymmetry finding) |
| **Priority** | Medium — verified-correct, high-blast-radius contract; dormant today, worth a live regression check rather than an active risk |
| **Property** | The four sequential gates in `check_checkpoint_control()` (`controlFileVersion`, CRC, `binaryVersion`, `s3Mode`) all run independently on every control-file read — the sut-analysis worry that an earlier-passing gate (`ORIOLEDB_BINARY_VERSION`) could silently prevent a later, finer-grained version check from being consulted does not hold at the checkpoint-control-file level; the finer per-object version constants (`SYS_TREE`/`PAGE`/`COMPRESS`) were never gated by this check in the first place — they live at a different layer, checked unconditionally per-object at read time. |
| **Invariant** | Primarily a structural/regression-guard property, best falsified by a deliberate compatibility-break test (bump a version constant, start against an old data directory, assert `FATAL` + a specific errdetail substring) rather than organic fault injection. |
| **Antithesis Angle** | Deliberate version-bump build test rather than a fault-injection target. Separately worth normalizing: a severity asymmetry where a version mismatch is `FATAL` with an `initdb` hint, but a CRC mismatch — at least as serious a corruption signal — is only `ERROR`. |
| **Why It Matters** | The checkpoint-control file is "the single authoritative persistence-boundary record" per `sut-analysis.md` §2 — the highest-blast-radius binary-format contract in the codebase. Even though direct reading found the gates intact today, this is worth a live assertion so a future regression here is caught immediately rather than surfacing as unexplained downstream corruption. |
| **Testability** | needs build variant — falsifying this requires a deliberate version-bump build test (bump a version constant, start against an old data directory), not organic fault injection against a single running binary version. |
| **Cross-reference** | **Near-redundant with `page-version-mismatch-fails-safe`** (evaluation, Wildcard lens): both are "verified-correct-today, dormant, no live workload" findings discovered via the same "does an earlier-passing gate silently skip a later, finer check" investigation prompted by `sut-analysis.md` §2, differing only in which version constant/file layer they gate (checkpoint-control-file-level here; page/compression-format-level there). Both are kept as distinct properties because they gate genuinely different constants and different on-disk artifacts, but a workload/fixture built for one should be checked for reuse against the other before being built twice. See also `property-relationships.md` Cluster 6. |

**Open Questions:**

- Is `elog(ERROR, "Wrong CRC in control file")` ever caught/retried past by some `PG_TRY` wrapper in a way weaker than the `FATAL` used for the other three gates? `(needs follow-up: grep all call sites of get_checkpoint_control_data for PG_TRY/CATCH wrapping)`
- Would Antithesis's organic bit-flip disk corruption of the control file ever land specifically on the version fields (small) vs. the much larger CRC-protected remainder (which fails via the weaker `ERROR` path)? `(needs human input / empirical run)`

### page-version-mismatch-fails-safe — On-disk page/compression version mismatch fails safe today, but the documented "seamless conversion" path has never been implemented

| | |
|---|---|
| **Type** | Safety / Meta (dormant doc/implementation gap) |
| **Priority** | Low — dormant, forward-looking process note; not organically testable until a page/compression version is ever bumped |
| **Property** | A page or compression-format version mismatch always halts loudly (`elog(FATAL)`) rather than silently misreading page bytes under the wrong layout assumptions — but the documented promise of "seamless conversion for lower versions" has never been implemented for pages/compression specifically (`convert_orioledb_page_version` is a stub that itself `elog(FATAL)`s), because only one page/compress version has ever existed since introduction. |
| **Invariant** | Not organically testable today (single version in existence). Recorded as a process/coverage note: when `ORIOLEDB_PAGE_VERSION` is next bumped, add a fixture with an old-version page image and assert the conversion round-trips correctly under concurrent read load and, ideally, crash-mid-conversion fault injection. |
| **Antithesis Angle** | Not reachable today; this is a forward-looking process note rather than a workload to build now. |
| **Why It Matters** | The gap is currently dormant and safe (fails loudly, no misinterpretation risk today), but is a real forward-looking risk: when a real conversion function replaces the stub, a bug in it (an off-by-one in a newly-added field, or a conversion applied backward) is exactly the "misinterpreting bytes" failure class this focus targets, with zero existing test scaffolding to catch it. Sys-tree-level version conversion (`data_version`) has real historical precedent of correct directional conversion logic being implemented across ~7 call sites and then cleanly removed on each `ORIOLEDB_BINARY_VERSION` bump — the pattern is real and has worked before, just currently dormant for pages/compression. |
| **Testability** | needs build variant — not organically testable until `ORIOLEDB_PAGE_VERSION` is next bumped and a real conversion function exists to fixture-test; today only the single existing version is in use. |
| **Cross-reference** | **Near-redundant with `checkpoint-control-version-gate-fails-safe`** (evaluation, Wildcard lens): same "verified-correct-today, dormant, no live workload" shape, same discovery investigation, differing only in which version constant/layer is gated (page/compression-format here; checkpoint-control-file there). Kept distinct because the two gate genuinely different constants and artifacts — see that property's entry and `property-relationships.md` Cluster 6 for the full cross-reference. |

**Open Questions:**

- Is there any existing test that fabricates a wrong-page-version image on disk to verify the `FATAL` path itself (as opposed to the unimplemented conversion path)? Not found in `test/sql`, `test/t`, or `test/specs`. `(needs follow-up grep)`

### disk-leaf-header-read-before-validation — A disk-backed sequential scan reads a leaf page's header fields before checking the checksum/I/O-error result that should gate them

| | |
|---|---|
| **Type** | Safety (concrete ordering bug, directly confirmed) |
| **Priority** | High — concrete, directly-confirmed ordering bug reachable today via default-enabled checksums plus disk fault injection |
| **Property** | Whenever a leaf-page checksum failure or I/O error occurs during a disk-backed sequential scan (`load_next_disk_leaf_page`), no undo-stack read (`read_page_from_undo`) is attempted using that page's header fields (`header->csn`, `header->undoLocation`) before the `OReadPageResult` validation gate is checked — the tri-state result is supposed to gate whether the buffer's contents are trusted at all. |
| **Invariant** | A `Reachable`-anchored assertion correlating a preceding checksum failure with a `read_page_from_undo` call on the same scan, or (stronger, with new instrumentation) a direct `Always` assertion inside `load_next_disk_leaf_page` guarding the ordering. |
| **Antithesis Angle** | A disk-level bit-flip fault targeted at an on-disk B-tree leaf page during an active sequential scan. `orioledb_checksums_enabled` defaults to `true` and is never overridden anywhere in `test/antithesis/`, so this path is reachable today with no config change. |
| **Why It Matters** | On a checksum failure, the code reads `header->csn`/`header->undoLocation` from a buffer known to be corrupted and, if the (essentially random) `csn` field happens to compare `>=` the downlink's, calls `read_page_from_undo()` with a garbage `undoLocation` *before* the subsequent `ereport(ERROR)` ever fires. On an I/O error, the same read runs against a stale, unrelated prior page's contents (the scan buffer is reused, not zeroed on a short read). Every *other* caller of `read_page_from_disk()` checked in this pass gets the ordering right — this is a genuine, isolated inversion at one specific call site, distinct from the general "WARNING vs ERROR" checksum-severity framing elsewhere in the codebase. |

**Open Questions:**

- What does `read_page_from_undo` actually do with a garbage or foreign-page `undoLocation` — does it validate the range before dereferencing, or could this manifest as an out-of-bounds undo-log read/crash rather than a clean error? `(needs follow-up: read read_page_from_undo's body directly)`
- Is `scan->leafImg` guaranteed zeroed/safely initialized before the *first* call to `load_next_disk_leaf_page` in a scan (limiting the "stale foreign page" scenario to the second and later reads within one scan)? Not confirmed.

---

## Category: Isolation, Serializability & Row-Level Concurrency

OrioleDB does not implement SSI; `orioledb.serializable` instead selects
between a coarse table-lock mode, an error-rejection mode, and a silent
downgrade mode. This category covers whether that substitute mechanism is
correctly and completely wired in, whether the isolation-mode configuration
is self-consistent, and whether OrioleDB's own explicitly-documented
row-level concurrency divergences from stock Postgres stay exactly as
narrowly scoped as documented.

### serializable-table-lock-untested — The default SERIALIZABLE-substitute mechanism (table_lock mode) has never been exercised by any existing test or Antithesis config

| | |
|---|---|
| **Type** | Reachability / Meta (test-coverage gap on a load-bearing correctness mechanism) |
| **Priority** | High — the default SERIALIZABLE-substitute mechanism has zero test coverage anywhere in the harness today |
| **Property** | `orioledb.serializable = table_lock`'s heavyweight-`ExclusiveLock`-per-table mechanism (`o_serializable_lock_relation`, wired into 8 tableam entry points and the entire substitute for real SSI in this codebase) is exercised by at least one `SERIALIZABLE`-isolation transaction under concurrent fault injection — confirmed today that it never is, in any harness config: `JEPSEN_ISOLATION` is always `repeatable-read` across every workload config, and `orioledb.serializable` is never set anywhere in `test/antithesis/config/**`. |
| **Invariant** | No assertion exists yet. Recommend a jepsen-style workload variant with `JEPSEN_ISOLATION=serializable`/`JEPSEN_EXPECTED_CONSISTENCY_MODEL=serializable` and `orioledb.serializable=table_lock` set explicitly, verified via jepsen's own serializability checker (ideally wired to an `always()` per `jepsen-verdict-not-sdk-visible`). A cheaper companion variant: `Always` on every `SERIALIZABLE` attempt under `orioledb.serializable=error` being cleanly rejected, never silently accepted. |
| **Antithesis Angle** | Add/extend a jepsen workload variant exercising `SERIALIZABLE` isolation with scheduling-delay/disk-stall fault injection specifically targeting the lock-acquisition window at each of the 8 tableam entry points. |
| **Why It Matters** | If `o_serializable_lock_relation`'s coverage across those 8 entry points were ever incomplete (a new mutating path added later that forgets the call, or a DDL/COPY/TRUNCATE path that bypasses the tableam callbacks), two `SERIALIZABLE` transactions could interleave without the claimed mutual exclusion — the exact write-skew/serialization anomaly this mechanism exists to prevent, silently, since Postgres has no SSI backstop for OrioleDB tables. Given zero current test coverage, such a regression would currently be invisible to both the deterministic suite and Antithesis. |

**Open Questions:**

- Are there mutating access paths to OrioleDB tables that bypass all 8 of the enumerated tableam callbacks (only these 8 were grepped; not cross-checked against the full `orioledb_am_methods` vtable)? `(partial: 8 call sites found and confirmed load-bearing; full vtable cross-check not done)`
- Does `O_SERIALIZABLE_REPEATABLE_READ`'s "already equivalent to REPEATABLE READ" claim actually hold, or could leaving `XactIsoLevel` at `XACT_SERIALIZABLE` while internally treating it as repeatable-read create an observable difference? `(needs follow-up if this mode is ever prioritized — lower priority than `table_lock` since it's opt-in, not default)`

### serializable-error-mode-truncate-gap — `orioledb.serializable=error` mode's rejection is not enforced on one TRUNCATE path (low security relevance)

| | |
|---|---|
| **Type** | Safety (minor code inconsistency, low security relevance) |
| **Priority** | Low — minor, low-security-relevance inconsistency on a fully self-service GUC |
| **Property** | `orioledb.serializable=error` mode's `ERRCODE_FEATURE_NOT_SUPPORTED` rejection is enforced on every OrioleDB DML/DDL entry point checked, except `orioledb_relation_nontransactional_truncate()` (`TRUNCATE` of a table created/given new storage earlier in the *same* transaction) — reachable only via a session changing the GUC mid-transaction, since `orioledb.serializable` is `PGC_USERSET` (fully self-service). |
| **Invariant** | Low priority. If a future workload does exercise `orioledb.serializable=error` at all (see `serializable-table-lock-untested` — nothing does today), a `Reachable`/consistency check confirming this specific `TRUNCATE` path never raises the expected error, in contrast to every other DML path. |
| **Antithesis Angle** | Not recommended for workload investment given the framing caveat below. |
| **Why It Matters** | A genuine, reachable code-level inconsistency, but because the GUC is fully self-service (`PGC_USERSET`), it grants a session nothing it couldn't already get by leaving the GUC at `table_lock`/`repeatable_read` for the whole transaction — recorded because the assigned focus explicitly asked whether the rejection path is enforced consistently, and a real gap was found, not because this constitutes a privilege escalation. |
| **Testability** | needs harness config — no config in `test/antithesis/` ever sets `orioledb.serializable=error` at all today (see `serializable-table-lock-untested`); this property is only reachable once that workload variant exists. |

**Open Questions:**

- Is there any more consequential way to reach the unchecked `TRUNCATE` path without the session first deliberately creating this self-inconsistency (e.g. via a stored procedure or prepared-transaction boundary)? `(needs human input if the team wants this pursued further as a strict-mode correctness bug independent of the security framing)`
- Should `orioledb.serializable` be reclassified as `PGC_SUSET` if it's ever intended to function as an actual administrator-imposed policy rather than a per-session preference? `(needs human input — docs currently frame it purely as a behavior selector)`

### pk-update-chain-race-consistency — Verify the two documented row-level-concurrency divergences stay exactly as narrowly scoped as claimed

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — solid, directly implementable verification of a documented divergence; no new topology needed |
| **Property** | The two divergences from stock Postgres row-level concurrency that `doc/architecture/row-level-concurrency.mdx` documents as accepted, known behavior (a concurrent PK update errors out the concurrent updater; a delete-then-reinsert of the same PK value can cause a concurrent updater to silently update the newly-inserted row) never produce a result broader than documented — no torn/partial write straddling the delete and reinsert, no corrupted row content, under a wider range of interleavings than the doc's own two-session example constructs. |
| **Invariant** | `AlwaysOrUnreachable`: *if* the race is hit (updater lands on the reinserted row), the row content must be exactly what the reinsert wrote, never a mix. |
| **Antithesis Angle** | Directly implementable without new topology: N concurrent sessions doing PK-preserving delete+reinsert cycles on a shared key set, plus concurrent updaters on the same keys, with scheduling-delay fault injection widening the delete-to-reinsert window or the updater's read-to-lock-wait window — the doc's own two-line example is essentially already a test spec. |
| **Why It Matters** | The delete+reinsert scenario is a lost-update/wrong-row anomaly shape if it goes even slightly further than documented ("session 2's UPDATE silently succeeds against a different row than the one it read"). Per `validating-claims.md`'s discipline, a claimed guarantee should be verified to actually hold, not merely trusted because it's written down. |

**Open Questions:**

- Is "the updater lands on the reinserted row" really the only possible outcome, or are there rarer timing windows (recovery replay overlap, a third concurrent session) where the result is something the docs don't cover? Not investigated beyond the doc's own example — needs actual multi-session interleaving exploration, which is exactly what Antithesis fault injection is suited to attempt.
- Does this interact with the PK/SK checkpoint-fixup mechanism (`sk-fixup-undo-recycling-drop`) if the reinsert happens to land in the pending-SK window it covers? Not traced — plausible but unconfirmed interaction between two separately-documented races on the same index-organized-table architecture.

---

## Category: Catalog Cache & System-Level Concurrency

### o-sys-cache-invalidation-race — OrioleDB's own catalog-duplicate cache layers a fast-path pointer on top of Postgres's invalidation-delivery contract (speculative, lower confidence)

| | |
|---|---|
| **Type** | Safety (speculative — lower confidence than other properties in this catalog; **priority lowered further** — see below) |
| **Priority** | Low — speculative, explicitly lower-confidence lead with no concrete SQL-observable invariant formulated yet |
| **Property** | A backend never observes a table/type/catalog descriptor via `o_sys_cache_search()`'s fast-path (`last_fast_cache_entry`) that is stale relative to a concurrently-committed DDL change on the same object — the fast-cache short-circuit never returns data older than what a full catcache lookup would return at the same logical point. |
| **Invariant** | `Always` — not yet a fully concrete formulation. A workable version: concurrent DDL against one backend while a second repeatedly queries/uses the affected object, asserting the second backend's observed definition is never older than the last DDL commit it has otherwise causally observed. |
| **Antithesis Angle** | Concurrent DDL + concurrent DML/queries touching the same catalog objects, with scheduling-fault injection targeting the window between a DDL transaction's commit and other backends actually processing the queued invalidation (`AcceptInvalidationMessages()` timing) — the adversarial case is a backend that delays entering its next transaction/statement as long as possible while still using stale fast-cache data. |
| **Why It Matters** | If real, this is a silent-wrong-behavior bug (stale type/catalog definition used) — per `sut-analysis.md` §10, the worst class of failure for a database engine. This pass could not confirm the mechanism is actually exploitable, as opposed to Postgres's own well-hardened invalidation-delivery contract (which this cache reuses via `CacheRegisterSyscacheCallback`, not a reinvented mechanism) already fully covering it. **Priority correction (evaluation, Implementability lens):** this property's low evidence-confidence and low implementability (no concrete SQL-observable invariant formulated yet) are the same root cause, not independent concerns — both stem from the fast-path mechanism never having been traced to a conclusion. Lowered relative to properties with a concrete, checkable invariant already in hand; retained in the catalog as a documented speculative lead, not dropped, per the skill's "don't fabricate answers" guidance. |

**Open Questions:**

- Does `o_sys_cache_search()`'s fast-path check anything beyond pointer/key equality before trusting `last_fast_cache_entry`, or does it rely entirely on `invalidate_fastcache_entry()` having already cleared the pointer by the time of the read? `(needs human input or a deeper trace of the full function body, which was only partially read)`
- How do recovery workers interact with this cache during WAL replay of DDL — do they bypass it entirely (one data point, `Assert(!is_recovery_in_progress())` in the delete-undo callback, suggests yes for that path), or does some other part of the cache participate in recovery replay? `(partial: one data point found; not a full trace)` `(needs human input to resolve conclusively, given the low-confidence framing above)`

---

## Category: Lifecycle / Build & Version-Skew Harness Gaps

### orioledb-requires-preload-clean-failure — Every orioledb access path should fail cleanly, never crash or silently misbehave, if the extension wasn't preloaded

| | |
|---|---|
| **Type** | Safety (defensively-coded property — no documented guarantee was found to test; reconstructed from code) |
| **Priority** | Low — defensively-coded property protecting an already-working check; explicitly lower priority than most of the catalog |
| **Property** | Any attempt to access an orioledb-backed table or call an orioledb SQL function without `orioledb` present in `shared_preload_libraries` results in a clean, well-formed `ERROR`/`FATAL` — never a crash, never silent misbehavior, never inconsistent partial success. |
| **Invariant** | `Always()` — a config-mutation-style property (start Postgres with a config that omits `shared_preload_libraries`, or with a different binary, against a data directory that already has orioledb tables) rather than a runtime-fault-injection property. |
| **Antithesis Angle** | Lower priority than most properties in this catalog: this protects a check (`orioledb_check_shmem()`/`shared_segment_initialized`) that's already demonstrably present and working, rather than surfacing a suspected gap. |
| **Why It Matters** | The task's original framing (a documented conversion-back-to-heap workflow, or a documented ordering assumption for switching between patched and vanilla Postgres binaries) **does not exist anywhere in `doc/`** — this was validated as an absence, not assumed. The genuinely interesting residual risk is whether *every* access path to orioledb state goes through the `shared_segment_initialized` gate, or whether some path (e.g. planner-time catalog lookups) could be reached first in a config where `shared_preload_libraries` was forgotten. |
| **Testability** | needs harness config — a config-mutation scenario (start without `shared_preload_libraries`, or against a mismatched binary/data-directory pairing), not a fault-injection target against a running instance. |

**Open Questions:**

- Is there any orioledb table-access code path that could execute before `orioledb_check_shmem()`'s check fires? `(needs follow-up code reading in src/tableam/handler.c and planner-hook call sites)`
- Does an orioledb-to-heap conversion workflow exist at all, documented or not? No such workflow was found anywhere in `doc/`. `(needs human input: confirm with the team whether this is supported/planned, since if it doesn't exist, related crash-during-conversion questions are moot)`

### checkpoint-stats-view-pg-major-branch — The chaos driver's PG16-vs-PG17+ stats-view branching was recently buggy; regression-guard it

**Reframed (evaluation, Antithesis Fit + Implementability lenses): this is a
CI-hygiene note about the Antithesis *driver's own* Python code, not a
main-priority SUT property.** It is kept in the catalog because the bug it
guards against is real and already shipped once, but it should not be
sequenced or prioritized alongside the SUT-behavior properties above —
independently, both lenses flagged that `checkpoint_count()`'s bug
(`f0c818c1`) tests the chaos driver's own helper, not OrioleDB.

| | |
|---|---|
| **Type** | Safety (regression guard on an already-fixed, recent test-harness bug — a CI-hygiene concern, not a SUT-behavior property; see reframing note above) |
| **Priority** | Low — CI-hygiene regression guard on the Antithesis driver's own Python code, not a SUT-behavior claim |
| **Property** | The `sk-recovery-race-chaos` driver's `checkpoint_count()` helper correctly detects, at runtime (via `to_regclass('pg_stat_checkpointer')`), whether to query `pg_stat_checkpointer` (PG17+) or `pg_stat_bgwriter` (PG16) for checkpoint counts, rather than hardcoding a column set that doesn't exist on the "wrong" major version. |
| **Invariant** | Process/CI-hygiene check rather than a new SDK assertion: run the `sk-recovery-race-chaos` workload against all three supported PG majors (16/17/18) whenever `checkpoint_count()` or its callers change, and confirm the existing `sometimes(overlapped, ...)` liveness assertion fires on all three. |
| **Antithesis Angle** | A driver-side regression guard, not a SUT-side fault-injection target. |
| **Why It Matters** | This exact bug shipped and was fixed in this session's own recent git history (`f0c818c1`) — a concrete instance of the general "harness code bug masquerading as an environment/infra failure" pattern this focus targets: a hardcoded column reference would raise on the wrong PG major, most likely crashing the whole chaos driver process (loud) rather than silently weakening the assertion (quiet), since `checkpoint_count()` has no exception handling of its own. A grep across `test/antithesis/` confirmed no other stats-reading helper has the same hardcoded-version-reference bug today. |
| **Testability** | needs harness config — requires building/running against all three supported PG majors (16/17/18), which is a CI-matrix concern rather than an in-run fault-injection target. |

**Open Questions:**

- Does the team's CI/validation process actually run this workload against all three PG majors before merging driver changes, or only whichever `PG_MAJOR` a developer happens to set locally? Not found in `.github/workflows/` (scoped to the core extension's own `check.yml`, not the Antithesis harness). `(needs human input)`

---

## Category: Backup, Restore, and Major-Version Upgrade

**New category (gap-fill addition, evaluation G2/G6, Coverage Balance +
Wildcard lenses).** `sut-analysis.md` §9/§10 names backup/restore under fault
injection (`pg_rewind`, pgbackrest, wal-g, `orioledb.replay_until_lsn`) as a
workflow with zero properties, and separately names cross-major `pg_upgrade`
as a substantial, actively-developed, in-scope feature with zero
representation anywhere in this research despite touching the same
checkpoint-control-file version gate this catalog already treats as its
highest-blast-radius contract. The six properties below fill both gaps. Note
scope precisely: `pg_rewind` (standard PostgreSQL tool) is **in scope** and
is **distinct from the excluded OrioleDB `orioledb_rewind_*` feature**
(`src/rewind/`) — this category does not reopen that exclusion.

### backup-restore-lacks-structural-oracle — The existing pgbackrest/wal-g integration tests verify content equality after restore, never OrioleDB structural integrity

| | |
|---|---|
| **Type** | Safety / Meta — a property about the backup/restore tests' own oracle, not directly about OrioleDB's data |
| **Priority** | Medium — harness-integrity meta-property that strengthens the oracle for a top-named coverage gap, but doesn't itself find a bug |
| **Property** | Every OrioleDB relation restored from a pgbackrest or wal-g backup (full, incremental/block-incremental, delta, or any PITR target) passes `verify_orioledb()` (equivalently, `pg_amcheck`) with zero rows returned, in addition to matching the expected row content — a restore is not considered verified by content-equality alone. |
| **Invariant** | `Always(verify_orioledb_returns_no_rows_after_restore)`: call `SELECT * FROM verify_orioledb(<relation>::regclass, true)` (the thorough/`force_file_check` variant) on every restored/scratch node these tests already stand up, immediately alongside the existing content-fingerprint assertions — not a new topology, just a new check bolted onto scenarios the tests already construct. |
| **Antithesis Angle** | No new fault-injection angle by itself — strengthens the oracle used by scenarios the existing tests (and `backup-window-crash-untested`'s proposed new fault-injection scenarios) already construct or will construct, turning every existing and future backup/restore scenario in these two files into a candidate for catching structural corruption, not just content divergence. |
| **Why It Matters** | Confirmed by grep: `test/integration/pgbackrest_test.py` (622 lines) and `test/integration/walg_test.py` (581 lines) — both real, substantial integration suites (full/incremental backup, standby restore+promotion, PITR, delta/block-incremental diffing) — have **zero** occurrences of `verify_orioledb`/`orioledb_tbl_check`/`amcheck`; every correctness check is a scalar-column read or a full-table content fingerprint. `recovery-sk-rebuild-desync`'s own evidence file already documents a structurally-broken table that still returns byte-identical *content* via a plain query — a physical backup/restore round-trip (files copied out-of-band, block-incrementally reconstructed) has more opportunity than plain crash recovery to reproduce exactly that failure shape, silently, since neither integration suite's oracle would notice. |

**Open Questions:**

- Does `verify_orioledb()`'s thorough (`force_file_check=true`) mode have a runtime cost that would meaningfully slow the existing integration tests if added to all of them? `(needs a quick timing measurement)`
- Does `verify_orioledb()` share `tbl-check-oracle-transient-false-negative`'s documented transient false-negative window right after a checkpoint, and if so, does calling it immediately after a restore (before a settling checkpoint) risk a false alarm in this new context? `(needs investigation)`
- Should this be `Always` per-relation or aggregated per-database (`pg_amcheck -d mydb`, checking every checkable relation in one call)? A design choice for whoever implements the assertion.

### backup-window-crash-untested — Neither pgbackrest nor wal-g's integration tests combine their existing checkpoint/backup timing race with an actual process-level crash

| | |
|---|---|
| **Type** | Safety |
| **Priority** | High — extends the catalog's top-tier checkpoint-boundary-plus-crash shape to a named zero-fault-injection-coverage workflow, fully buildable on existing scaffolding |
| **Property** | A primary crash (or a killed backup-tool process) landing at any point between a physical backup's consistency-point checkpoint (`pg_backup_start`) and its finalization (`pg_backup_stop`) never produces a backup that, once restored, has row content or `verify_orioledb()` structural state different from what an uninterrupted backup would have produced for the same logical point in time — and a subsequent backup/restore attempt against the same repository still succeeds. |
| **Invariant** | `Always(restored_content_and_structure_match_expected_state)`: extend the existing `pgbackrest_test.py::test_integration` scenario (a `checkpoint_writeback` stopevent pins the checkpoint mid-writeback while a background backup runs) by adding an actual process fault — kill the primary's postmaster or the backup-tool subprocess itself while the stopevent holds the checkpoint — then restart/retry and assert the restored fingerprint plus `verify_orioledb()` (see `backup-restore-lacks-structural-oracle`) both match. Complement with `Sometimes(crash_landed_inside_backup_consistency_window)` so the assertion isn't vacuously satisfied by runs where the fault landed outside the window. |
| **Antithesis Angle** | Reuse the exact scaffolding `pgbackrest_test.py`'s `test_integration` already built (the `checkpoint_writeback` stopevent + background backup thread + `wait_checkpointer_stopevent`), but instead of only mutating data while the checkpoint is parked, have Antithesis's fault injection (or a driver-level `SIGKILL`) hit the primary or the backup-tool process during that exact parked window — a more organic way to explore "crash exactly during the backup's consistency checkpoint" than hand-scripting every kill point. |
| **Why It Matters** | A physical backup's consistency point *is* a checkpoint — `pg_backup_start()` forces/waits for one, which runs through OrioleDB's `CheckPoint_hook` exactly like any other checkpoint. This is the same "checkpoint boundary + crash" shape the "Checkpoint / Recovery Boundary Consistency" category already treats as high-value, previously unaddressed here because it requires an external backup tool rather than a bare `CHECKPOINT` command. Backup/restore is explicitly named (`sut-analysis.md` §9/§10) as the one workflow category with zero fault-injection coverage despite two substantial real-tool integration suites existing, and its severity is "concentrated at incident-response time" — exactly when a corrupted or non-restorable backup is most costly to discover. |

**Open Questions:**

- Does `pg_backup_start()`/`pg_backup_stop()` (core Postgres, no orioledb-specific `backup_label`/`BackupInProgress` code found anywhere in `src/`/`include/`) request any OrioleDB-specific checkpoint step beyond the standard `CheckPoint_hook` call, or is it exactly the same path unmodified? `(partial: confirmed no orioledb-specific backup-checkpoint code exists in this repo; the patched-Postgres side of `pg_backup_start` itself was not re-examined, per the scope restriction on `orioledb_postgres`)`
- Is a killed-mid-copy backup-tool subprocess actually reachable/meaningful in the current harness topology, which would need fault injection targeting a *client-side* process rather than the SUT process the rest of this catalog assumes? `(needs human input / harness design decision)`
- What does pgbackrest/wal-g's `--delta`/idempotent-retry actually do if pointed at a repository containing a partial, never-finalized backup? Not traced through either tool's own source (external, out of this repo's scope) — needs a black-box experiment or vendor documentation.

### pg-rewind-orioledb-fullcopy-correctness — `pg_rewind`'s documented "copies OrioleDB tables completely" fallback has zero test coverage, and is structurally opaque to incremental diffing in a way unique among this gap's tools

| | |
|---|---|
| **Type** | Safety |
| **Priority** | Medium — real structural gap (zero coverage, opaque to incremental diffing), but needs a new two-node divergence topology to become actionable |
| **Property** | A physical replica rebuilt via `pg_rewind` against a diverged primary is, after the rebuild completes and the node starts, indistinguishable in OrioleDB state from a replica built via a fresh `pg_basebackup` against the same primary at the same point — same row content, and `verify_orioledb()`/`pg_amcheck` report no structural issues on any OrioleDB relation. |
| **Invariant** | `Always(rewound_node_matches_fresh_basebackup)`: compare content fingerprints and `verify_orioledb()` results (see `backup-restore-lacks-structural-oracle`) against a fresh-basebackup baseline. `Reachable(pg_rewind_completed_against_diverged_orioledb_data)` as a companion, since a real timeline divergence involving OrioleDB tables doesn't exist anywhere in the current test suite and needs to be confirmed as actually constructed before the `Always` check carries weight. |
| **Antithesis Angle** | Needs a scenario the harness doesn't have today: two nodes that diverge (e.g. a promoted former-standby vs. its old primary, both having taken independent OrioleDB writes on different timelines) followed by a `pg_rewind` of the loser against the winner — a natural extension of the primary/standby topology `deployment-topology.md` already recommends, with an additional failover-then-rewind step. Combine with fault injection (kill `pg_rewind` mid-copy, kill the target node mid-post-rewind-recovery) for the fuller version, mirroring `backup-window-crash-untested`'s angle applied to `pg_rewind`. |
| **Why It Matters** | Directly validated, not just repeated from docs: OrioleDB's sole WAL-insertion function (`log_logical_wal_container()`, `src/recovery/wal.c:741-786`) never calls `XLogRegisterBuffer()` (confirmed via repo-wide grep — zero hits) — every OrioleDB WAL record is, from `pg_rewind`'s point of view, an opaque data blob with zero block references, structurally invisible to the generic block-reference mechanism `pg_rewind`'s incremental-diff path relies on. This is stronger than "a different format `pg_rewind` doesn't decode" — it's bypassed entirely, making the documented full-copy fallback the *only* path OrioleDB tables ever take through this tool. `pg_rewind` has literally zero test coverage in this repo (confirmed via grep), unlike pgbackrest/wal-g which at least have substantial, if fault-injection-free, integration suites — and unlike those tools (which at least see the files' bytes), `pg_rewind`'s entire incremental-diff mechanism never even engages with OrioleDB data. |

**Open Questions:**

- Does `pg_rewind`'s actual `filemap.c` file-classification logic (in the excluded `orioledb_postgres` source) definitely place `orioledb_data/`/`orioledb_undo/` into the generic "copy whole file if changed" bucket, as indirect evidence from this repo's own `src/s3/checkpoint.c:100` (a comment cross-referencing "the filter lists in pg_rewind's filemap.c") suggests, or could some specific file (e.g. the checkpoint control file, more similar in shape to `pg_control`, which `pg_rewind` **does** special-case) be misclassified? `(needs human input or a black-box pg_rewind run — cannot be resolved by further code reading, since the deciding code is in the excluded repo)`
- Is a genuine OrioleDB-relevant divergence-then-rewind scenario even reachable in the current harness, which has no second Postgres node today? `(depends entirely on whether the recommended primary/standby topology addition is built)`
- Does `pg_rewind`'s full-copy fallback correctly handle a file that exists on the diverged target but has been **removed** on the source (e.g. a recycled undo file or cleaned-up `.map`/temp file) — i.e., is file *deletion* reconciliation, not just copying, also verified correct? Not investigated.

### replay-until-lsn-catalog-desync — `orioledb.replay_until_lsn`'s self-disclosed "split-brain" mechanism has an unresolved two-tier catalog-vs-B-tree divergence and zero test coverage

| | |
|---|---|
| **Type** | Safety |
| **Priority** | High — self-disclosed dangerous last-resort mechanism with zero test coverage, fully constructible in the existing single-node harness |
| **Property** | After `orioledb.replay_until_lsn` triggers (recovery reaches or passes the configured LSN and permanently stops applying OrioleDB WAL records for the remainder of that Startup process's life — confirmed by direct tracing to be scoped entirely to OrioleDB's own `rm_redo` callback, not core Postgres's main replay loop or any other resource manager), any subsequent access to an OrioleDB table whose DDL (`pg_class`/`pg_attribute`) was modified by WAL between the cutoff LSN and the actual end of replayed WAL either (a) cleanly `ERROR`s with a diagnosable message identifying the OrioleDB/catalog descriptor mismatch, or (b) is fully and correctly usable if no such DDL occurred — it never silently uses a mismatched tuple descriptor against the frozen on-disk B-tree, and it never crashes the backend/instance. |
| **Invariant** | `AlwaysOrUnreachable(post_cutoff_ddl_table_access_fails_safe_or_is_absent)`: construct DDL (`ALTER TABLE ADD/DROP COLUMN`, `CREATE INDEX`) on an OrioleDB table committed strictly after the intended cutoff, start the instance with `replay_until_lsn` set to the pre-DDL LSN, query the affected table — assert no crash/PANIC and, if an error is raised, that it's a clean, attributable `ERROR`. Pair with `Reachable(replay_until_lsn_cutoff_warning_logged)` to confirm the cutoff path itself was exercised (it's a `WARNING`-only side effect). |
| **Antithesis Angle** | Fully constructible in the *existing* single-node topology — no replication/standby needed, unlike most of this catalog's highest-priority gaps. Sustained DML+DDL against OrioleDB tables, an unclean shutdown at a known WAL position, then a restart with `orioledb.replay_until_lsn` set to an earlier LSN deliberately chosen to land before some committed DDL — a config-mutation-style property (the GUC is `PGC_POSTMASTER`-only) rather than a live fault-injection target. |
| **Why It Matters** | This is a **self-disclosed, admittedly dangerous, last-resort mechanism** (the doc's own `:::warning[Data consistency]:::` block, comparable to `pg_resetwal`) with zero test coverage anywhere in the repository (confirmed: zero hits for `replay_until_lsn` across `test/t`, `test/sql`, `test/specs`, `test/antithesis`, `test/integration`) — named directly in the evaluation gap and explicitly in scope, unlike the excluded `orioledb_rewind_*` feature which has the same "docs pre-confess a risk but nothing tests it" pattern. The sharper consequence beyond the doc's own wording: because OrioleDB's own DDL/table-metadata bookkeeping (`o_tables`/`o_indices`) is itself replayed via the same WAL records as ordinary row data, the divergence is a **two-tier split** — `pg_class`/`pg_attribute` (heap) keeps advancing past the cutoff while OrioleDB's own table descriptor and on-disk B-tree layout freeze at the pre-DDL shape — and whether this fails safe (clean `ERROR` on next access) or fails silently (stale tuple descriptor against frozen B-tree) is unresolved by static reading. |

**Open Questions:**

- Does `o_tables_get()`/the table-open path in `src/tableam/handler.c` cross-validate OrioleDB's own table descriptor against `pg_class`/`pg_attribute` before using it, such that a post-cutoff DDL mismatch would be caught cleanly — or is there no such check? `(needs further code reading in src/catalog/o_tables.c and src/tableam/handler.c — only spot-checked this pass)`
- Does the mechanism apply per-database or cluster-wide, given the GUC and Startup process are both cluster-level? Not traced — the redo function's state machine has no visible per-database scoping in the code read this pass.
- After the cutoff triggers, can new writes to the *same* table whose earlier post-cutoff DDL was skipped succeed at all? Depends on the answer above — not investigated.
- Is there any operator-facing tooling (a query, a log summary) listing which objects were affected by a given cutoff, or is the single `WARNING` at first-record-skipped the entire operator-visible signal? Not found in `doc/` beyond the configuration reference.

### pg-upgrade-cross-major-cache-reset-gap — Cross-major `pg_upgrade` support (unmerged, active) self-acknowledges an incomplete audit of which OSysCache trees need a reset on major-version change

**Status: feature not present on `main`/`a975c702` — a forward-looking property about active, unmerged branch work** (`origin/pg_upgrade`, `origin/nickb/pg_upgrade_test`; `git merge-base --is-ancestor` confirms neither is an ancestor). Written up now, not discarded, because it is a substantial (11+2 commits, dated as recently as three days before the analyzed commit), actively-developed, in-scope feature that already found and iteratively fixed **two real crash bugs**, and directly touches the checkpoint-control-file version gate this catalog already treats as highest-blast-radius (`checkpoint-control-version-gate-fails-safe`).

| | |
|---|---|
| **Type** | Safety (forward-looking; the underlying architectural gap is real today even though the feature exposing it isn't merged) |
| **Priority** | Medium — substantive, implementer-acknowledged gap that already found two real crash bugs, but not present on main until the branch merges |
| **Property** | After a cross-major `pg_upgrade` (which carries `orioledb_data/`'s `SYS_TREES_*` caches over via a manual `cp -R`, per the sibling property below), a crash or checkpoint on the new-major cluster never catalog-freely deserializes a stale-format cache entry from an *un-audited* `SYS_TREES_*` tree and crashes recovery/the checkpointer with an `elog(FATAL)` or NULL-pointer dereference — the fix mechanism (`sys_tree_reset_on_major_upgrade()` unconditionally wiping and rebuilding specific trees from the catalog on first use) must cover every tree whose on-disk entries are actually PG-major-layout-dependent, not just the ones bugs have already been found in. |
| **Invariant** | Once the branch exists in the harness: `Unreachable(cache_deserialize_FATAL_or_crash_after_cross_major_upgrade)` — construct a pre-upgrade cluster with at least one expression index, one partial index, and one index referencing a `SECURITY DEFINER` SQL function (mirroring the branch's own test fixture); vary which catalog objects are exercised catalog-free after upgrade (custom collation/operator class/aggregate/enum/range type, not just class+proc cache which are already fixed) to probe the eleven not-yet-individually-audited trees. |
| **Antithesis Angle** | Not implementable today (feature doesn't exist on `main`). Once merged: build a two-major-version harness variant, run `pg_upgrade` + the manual data copy, then inject a crash (`SIGKILL`/`-m immediate`) or force a checkpoint at varying points **relative to** whether `orioledb_upgrade_refresh()`/`maybe_auto_upgrade_refresh()` has run yet — including the currently-untested ordering where the first-ever checkpoint is the automatic background one, occurring before any foreground session has issued a utility statement. The team's own CI script always triggers the refresh before its crash probe, so this ordering has never been exercised even by the implementers. |
| **Why It Matters** | Self-acknowledged, unresolved gap from the implementers themselves, not a static-analysis inference — two commit messages state verbatim that "proper cross-major handling of the version-dependent OSysCache trees... is a broader issue... left as follow-up" and that "only the database cache is reset on a cross-major restart" even though class+proc were also later found to need it. As of the last commit read, exactly 3 of 14 trees (`DATABASE_CACHE`, `CLASS_CACHE`, `PROC_CACHE`) are enumerated for reset, found reactively via two separate crash reproductions (a raw C-struct layout mismatch in the class cache; a silently-`NULL` serialized-node-tree in the proc cache with no read-time guard, unlike the index-expression path which was engineered correctly the first time). The eleven remaining trees have not been individually audited for whether their on-disk entries are actually version-dependent. A crash inside this window could plausibly degrade into a boot-loop rather than a single clean crash (recovery crashing triggers more crash-recovery), though this was not independently confirmed for the `pg_upgrade` case specifically. |

**Open Questions:**

- Which of the eleven not-yet-enumerated `SYS_TREES_*` caches actually persist PG-major-dependent binary layout (raw struct blits, or serialized node trees), and which are safely version-independent (plain scalar fields)? `(needs further investigation — the team's own commit messages treat this as open; a systematic audit was not performed here, and is arguably the team's job once they resume this branch)`
- Does a crash landing *before* the very first `maybe_auto_upgrade_refresh()` call in a cluster's lifetime still hit the documented clean-guard-error behavior, or does it reach an un-guarded catalog-free path? `(needs further investigation once the branch is resumed — the single highest-value residual question for Antithesis to probe once this feature exists in the harness)`
- Is this branch actively planned to merge to `main`, or exploratory/parked work? `(needs human input)`
- Does the proc-cache fix fully close the SQL-function proc-cache crash, or only the one specific repro constructed in its own test? No commit after re-runs or extends that probe. `(needs further investigation)`

### pg-upgrade-manual-data-copy-not-atomic — The documented manual `cp -R` of `orioledb_data`/`orioledb_undo` during `pg_upgrade` has none of `pg_upgrade`'s own resumability/atomicity guarantees (speculative, lower confidence)

**Status: speculative, lower confidence — reasoned from the documented procedure, not from an observed crash** (unlike the sibling property above). Same branch-ancestry caveat: concerns `origin/pg_upgrade`/`origin/nickb/pg_upgrade_test`, not present on `main`.

| | |
|---|---|
| **Type** | Safety (speculative — reconstructed from the documented procedure, not a demonstrated bug) |
| **Priority** | Low — explicitly speculative and lower-confidence, reasoned from the documented procedure rather than an observed crash |
| **Property** | If the operator-run `cp -R "$OLD_DATA/orioledb_data" "$NEW_DATA/orioledb_data"` step of the documented `pg_upgrade` procedure is interrupted partway (host crash, OOM-killer, disk-full, or an unchecked exit status), the new cluster either (a) refuses to start / raises a clear, attributable error identifying the incomplete copy, or (b) if it does start, every subsequent read of an affected relation fails loudly (checksum/short-read error) rather than returning wrong-but-plausible data — it never silently serves data from a partially-copied file. |
| **Invariant** | Not implementable today (feature doesn't exist on `main`, and the gap is in an un-instrumented shell procedure, not server code). Once/if prioritized: a startup-time (or `orioledb_upgrade_refresh()`-time) completeness check verifying every file referenced by the sys-trees' own metadata is present and at least as large as its last-known extent — no such check exists today in any form found in this pass. |
| **Antithesis Angle** | Distinct from every other property in this catalog: this needs the harness to model the *upgrade procedure* (interrupt the `cp -R` process, or the whole upgrade-runner container, at a randomized byte offset/file boundary) rather than fault-inject the long-running server process. |
| **Why It Matters** | Standard `pg_upgrade`'s own file transfer offers `--link`/`--clone`/`--copy` modes and is written to either complete a relation's transfer or leave the old cluster's copy untouched (retry is the documented recovery path) — **OrioleDB's storage bypasses all of that**: the checkpoint control file (`orioledb_data/control`) lives *inside* the manually, non-atomically copied tree. If the control file itself is left truncated, the existing (verified-correct, see `checkpoint-control-version-gate-fails-safe`) CRC gate would very likely catch it — a clean, attributable failure. But if a B-tree data file or undo segment is left truncated instead, no startup-time check reads every file in `orioledb_data/` to confirm completeness; the truncation surfaces only lazily, whenever a page at or beyond the truncation point is actually read — a detection *gap* (loud when it does trigger, but potentially much later, against unrelated workload traffic, making root-causing back to the upgrade event much harder than a normal crash-recovery failure). |

**Open Questions:**

- Does `pg_upgrade`'s own `--check` mode or extension points offer a hook OrioleDB could use to fold its data transfer into `pg_upgrade`'s own resumable/atomic-per-file machinery, rather than a fully separate manual step? `(needs human input / follow-up investigation once this branch is prioritized — would likely require consulting the patched-Postgres source, itself out of scope)`
- Is there already an operational runbook/automation layer (outside this repo) that wraps the documented manual steps with its own atomicity checks? Not knowable from this repo alone. `(needs human input)`
- Would a truncated file actually be silently unreadable-but-not-erroring in any realistic scenario (e.g. a copy tool that preallocates space, leaving a stale-but-valid-looking page at the new EOF)? Not verified. `(needs further investigation / empirical test if this property is prioritized)`

---

## Category: Test Oracle and Harness Integrity

These are meta-properties: not claims about OrioleDB's data or recovery
correctness, but claims about whether the *existing* Antithesis harness and
its oracles (`orioledb_tbl_check()`, the jepsen verdict, the chaos driver's
own control flow, coverage-guided search itself) can be trusted to surface a
real violation when one occurs. An unreliable oracle or an unwired signal
undermines every other property in this catalog that depends on it, silently
— which is why these are included as first-class properties rather than
process notes.

### tbl-check-oracle-transient-false-negative — The existing harness's correctness oracle has a known, documented transient false-negative window right after a checkpoint

| | |
|---|---|
| **Type** | Safety / Meta — a property about the oracle, not about OrioleDB's data |
| **Priority** | High — the entire existing harness's correctness signal rests on this oracle; a false-negative risks masking real #855-class regressions |
| **Property** | `orioledb_tbl_check()` (and, pending investigation, possibly its replacement `verify_orioledb()`) returns a result that reflects genuine structural consistency, not a transient artifact of "a checkpoint recently observed autonomous page writes (e.g. in-flight splits) and hasn't had a follow-up explicit `CHECKPOINT` to clear that per-page state." |
| **Invariant** | `Always(tbl_check_result_stable)`: call the checker twice in immediate succession with no intervening writes/checkpoints and assert agreement — disagreement on a quiescent table is direct proof of oracle nondeterminism, independent of whether real corruption exists. `AlwaysOrUnreachable(tbl_check_false_only_before_followup_checkpoint)`: if a pre-follow-up-checkpoint `false` is the *only* structural difference from a post-follow-up-checkpoint `true`, treat it as expected-and-benign rather than corruption. |
| **Antithesis Angle** | Doesn't need fault injection to reach the interesting state — reachable by ordinary DML + automatic checkpoints, which `sk-recovery-race-chaos` already does continuously. Antithesis's own scheduling jitter *increases* the chance of a checkpoint landing mid-split right at the exact moment the chaos driver's post-burst check (with no intervening explicit `CHECKPOINT`) fires — i.e. Antithesis is more likely than a clean CI run to trip this specific false-negative path, at exactly the moment triage would otherwise assume it found a real #855 regression. |
| **Why It Matters** | The entire existing Antithesis harness's `always()` correctness signal (both `sk-recovery-race` and `sk-recovery-race-chaos`) rests on this checker being trustworthy. The team's own test suite (`checkpoint_split_base_test.py`) documents this exact transient-false-negative window as expected behavior, on the current `main` branch, and the checker has a dense, twice-reverted fix history for precisely this mechanism (`sut-analysis.md` §8). A false alarm here consumes triage time and, worse, could train a team to dismiss future *real* violations as "the checker being flaky again." |

**Open Questions:**

- Does `verify_orioledb()` (the non-deprecated replacement, present since an unmerged deprecation branch) share the exact same transient-state window, or did the underlying `check_btree()` actually change? `(needs reading check_btree() and the exact commits touching its "autonomous page write" handling)`
- Has `sk-recovery-race-chaos` ever actually hit this false-negative window in a real run (a live risk), or is it bounded by how rarely automatic checkpoints land exactly mid-split at the moment the post-burst check runs (a theoretical one)? Not measured.
- Is `orioledb_tbl_check()` still callable in the image the Antithesis harness builds if the unmerged deprecation branch (`0bb61a3c`, which turns it into an `ERROR`-raising stub) ever merges — a one-way breaking risk for both existing SDK assertions? Confirmed yes as of the analyzed commit; worth flagging to whoever maintains the harness.

### jepsen-verdict-not-sdk-visible — The jepsen workload's serializability verdict is never wired into an Antithesis SDK assertion

| | |
|---|---|
| **Type** | Meta / Reachability — a property about the harness's verification pipeline |
| **Priority** | High — the harness's one check for the worst-case failure class (serializability anomalies) is currently invisible to Antithesis's own scoring |
| **Property** | Every jepsen run's serializability verdict (`results.edn`'s `:valid?`/`:anomalies`) is expressed as an explicit Antithesis SDK assertion outcome before the run ends, not only as a post-hoc file artifact that a human must remember to open. |
| **Invariant** | `Always(no_anomalies_found)` — parse `results.edn` after jepsen's own analysis completes (in `finally_jepsen-postgres` or a new post-processing step) and call `always(valid and not anomalies, "jepsen detected no serializability/consistency anomalies against orioledb", {...})` before container teardown. |
| **Antithesis Angle** | Doesn't change what jepsen does or what faults get injected — changes what Antithesis's own search *sees*. Today, a run that hits a genuine serializability violation looks, from Antithesis's scoring perspective, identical to a run that never got near one: no assertion fires either way. |
| **Why It Matters** | Per `sut-analysis.md` §10, "wrong query results or lost writes (serializability anomalies)" is the worst-case failure for a database engine, and jepsen is the one workload built specifically to catch it. An unwired verdict means the harness's single highest-severity-failure-class check is also its least actionable one — a low-effort, high-leverage fix relative to most other findings in this catalog (parsing an EDN file and calling one SDK function, vs. e.g. standing up a standby topology). |

**Open Questions:**

- What does jepsen's `results.edn` schema actually look like for this jepsen-postgres variant — is `:anomalies` always present/absent in a way a simple parser can key off reliably, or could a naive parser produce false negatives (a parse failure silently treated as "no anomalies")? `(needs a real jepsen run's results.edn to confirm the schema)`
- Should the assertion be `always()` per-run (one aggregate check) or per-anomaly-type (separate assertions for G0/G1/G2, etc.)? Deferred to whoever implements this in the `antithesis-workload` phase.
- Does jepsen's own process already fail the CI job / exit non-zero on an invalid result, making an SDK assertion partly redundant from a "does the test fail" perspective? `(needs human input / needs checking finally_jepsen-postgres's actual exit-code handling)`

### chaos-driver-skips-check-on-fault-landing — The chaos driver's own connection-loss handling silently skips its post-burst consistency check on exactly the runs where a disruptive fault most likely landed

| | |
|---|---|
| **Type** | Meta / Reachability — a property about the `sk-recovery-race-chaos` driver's own verification coverage |
| **Priority** | Medium — real coverage gap in the chaos driver's own verification, but the skip is at least logged today |
| **Property** | The chaos driver's post-burst consistency check (`assert_consistent(ctl_conn, "post-burst")`) is not systematically skipped specifically on the runs where a disruptive fault (a lost connection) landed during the burst — a `CONNECTION_LOST_ERRORS` exception during a burst should not silently suppress the very assertion meant to catch corruption caused by that same class of disruption. |
| **Invariant** | Minimum: `sometimes(connection_lost_during_burst, "sk-recovery-race-chaos burst was interrupted by a lost connection before post-burst consistency could be checked", {...})` — makes the skip visible and countable, without by itself fixing the gap. Stronger: reconnect and run `assert_consistent` anyway once the target is reachable again, upgrading this to a real `always()` check instead of a skip. |
| **Antithesis Angle** | Entirely about whether Antithesis's own fault injection (which this chaos workload depends on for constructing its race at all) is being *told about* the runs where it actually landed a hit. Right now, "the race never got constructed this run" and "the race was quite possibly constructed and then verification was skipped" look identical from Antithesis's outside view: a clean exit with a log line, no assertion outcome recorded either way. |
| **Why It Matters** | The exact fault conditions Antithesis is good at producing (process kills, scheduling delays severe enough to break a TCP connection) are also the conditions under which this driver silently opts out of checking anything — a false sense of "we're chaos-testing this" when the highest-signal runs may be quietly unverified. This is the same shape of problem `property-catalog.md`'s "Honest Summaries" guidance warns about, applied to test infrastructure: a property whose invariant only gets checked in the easy case is weaker than its name suggests. |

**Open Questions:**

- Is the singleton driver pattern invoked repeatedly over the course of one Antithesis run, or once per container lifetime? If invoked repeatedly, a single skipped check is less severe, but the *systematic bias* (skipped checks correlate with disruptive-fault runs specifically) still holds across the whole run's set of invocations. `(partial: confirmed the entrypoint pattern matches Antithesis's singleton-driver convention by file naming; did not confirm the exact re-invocation cadence)`
- Does the deterministic (non-chaos) `sk-recovery-race/driver.py` have the same gap? A quick structural read suggests no (it pins the race with `pg_stopevent_set`, so a lost connection there is unexpected and surfaces as an unhandled exception rather than a designed silent-return branch) — not independently re-verified line-by-line.
- Should the fix be "reconnect and check anyway" or "just make the skip visible"? A design decision for whoever implements the workload change (`antithesis-workload` skill), not prescribed here.

### core-postgres-hook-coverage-blind-spot — Antithesis's coverage-guided search has zero visibility into the patched-Postgres hook call sites that mediate OrioleDB/core state synchronization

| | |
|---|---|
| **Type** | Meta / Reachability — a property about the build/instrumentation pipeline's exploration-guidance coverage |
| **Priority** | Medium — low-cost, high-leverage instrumentation gap about search-guidance precision rather than a missed-bug risk |
| **Property** | The set of code locations Antithesis's coverage-guided search can distinguish as "explored" vs. "not yet explored" includes the patched-Postgres hook call sites that mediate OrioleDB/core-Postgres state synchronization (`xact_redo_hook`, `CheckPoint_hook`, `get_xidless_commit_lsn_hook`, `AcceptInvalidationMessagesHookType`, etc.) — not just the `orioledb.so`-internal logic downstream of them. Confirmed today: the Antithesis image's own Dockerfile comment states core Postgres is built with assertions but **not** sancov coverage instrumentation; only `orioledb.so` receives `-fsanitize-coverage=trace-pc-guard`. |
| **Invariant** | **Remedy reframed (evaluation, Implementability lens) to be same-repo-only**: the original framing ("markers at each orioledb-relevant hook call site in patched Postgres core") crosses this research pass's scope boundary (`/Users/artur/supabase/orioledb_postgres` is explicitly excluded). The corrected, implementable-today remedy stays entirely inside `orioledb.so`: place explicit `reachable()` markers at the point *OrioleDB's own hook callback function* is invoked (e.g., the entry of `orioledb_checkpoint_hook`-style functions this codebase already defines and registers against core's hook variables) — not at core Postgres's call site, which this analysis has no access to modify or evaluate. Since core Postgres invokes the hook variable, and the hook variable points at OrioleDB's own callback, a marker at the top of that callback still confirms "core actually called into OrioleDB here, at this moment" without touching a single line outside this repo. |
| **Antithesis Angle** | Coverage-guided fault injection uses code-coverage feedback to bias exploration toward less-explored paths; without any signal from inside core Postgres, the search has no basis to know whether a given run's fault timing actually varied *which hook fired when relative to OrioleDB's internal state transitions* — exactly the class of bug `sut-analysis.md` §1 flags as structurally most interesting. Same-repo `reachable()` markers at OrioleDB's own hook-callback entry points wouldn't give full per-branch coverage-guidance inside core Postgres itself, but would let exploration/triage tooling confirm whether a run actually exercised, e.g., the `CheckPoint_hook`-during-active-recovery-workers path (the exact scenario in `checkpoint-recovery-lsn-sync-gap`), entirely from within `orioledb.so`. |
| **Why It Matters** | A low-cost, high-leverage instrumentation gap: the team already pays the cost of vendoring/linking the Antithesis C SDK for `orioledb.so`; a handful of explicit markers at OrioleDB's own hook-callback entry points is a much smaller lift than full-binary sancov instrumentation of core Postgres, stays entirely same-repo, and targets the single highest-value additional concurrency surface beyond the already-covered PK/SK race. |

**Open Questions:**

- Is there a cost/complexity reason the team scoped instrumentation to `orioledb.so` only that isn't visible from the Dockerfile comment alone (e.g. core-Postgres coverage previously tried and found too noisy/slow, or LTO complications linking sancov into a binary built via the standard `./configure`/`make` path rather than PGXS)? `(needs human input from whoever wrote this Dockerfile)`
- Does a same-repo marker at OrioleDB's own hook-callback entry point give exploration/triage tooling a signal precise enough to distinguish "core called this hook once, early" from "core called this hook repeatedly, at varying points relative to recovery-worker state" — or would that finer distinction still require instrumentation on the core-Postgres side (out of scope), making the same-repo marker a partial, not full, substitute for the original remedy? `(needs further investigation — the same-repo marker resolves the scope-boundary concern but its precision relative to the original full remedy was not independently assessed)`

---

## Assumptions

- Every property in this catalog assumes the codebase state at commit `a975c702156cd449e9c0a8db6f8d9bf5bca4537d`; several properties (`recovery-finish-abort-livelock`, `replica-xmin-monotonicity`) exist specifically because two independent discovery passes found `sut-analysis.md`'s "unfixed" characterization of orioledb#876/#889 to be stale — always re-verify fix status against current `main` before treating any "regression guard" property as testing an open bug, or vice versa.
- `sut-analysis.md` itself was not edited as part of this synthesis pass (per explicit instruction) and still contains rewind/S3/patched-Postgres content that is now out of scope for further property work. Treat its §2 and §8 claims about orioledb#876/#889 specifically as **known-stale** — corrected by `recovery-finish-abort-livelock.md` and `replica-xmin-monotonicity.md` respectively — pending an actual edit to that file.
- Several properties (`checkpoint-abort-snapshot-standby-panic`, `sk-extent-leak-after-crash`, `malformed-wal-container-double-finish`, `recovery-sk-rebuild-desync`) are built on root-cause docs found on **unmerged remote branches** (`origin/add_stress_bank_account_test`, `origin/add_stress_test_pr`), read via `git show`, not present in this branch's worktree. These are strong leads (often with concrete repro rates and instrumented evidence), not confirmed facts about the analyzed commit's current behavior in every case — several explicitly note the underlying mechanism was *not* independently re-derived from first principles, only the doc's own account plus a check of whether cited fix commits are ancestors of `HEAD`. The gap-fill pass's own two branch-sweep additions (`recovery-meta-lock-signal-barrier-deadlock` on `origin/recovery-meta-buffering`; `checkpoint-corrupted-tree-silent-skip` on `origin/checkpoint-io-error-fatal`/`origin/checkpoint_avoid_error_loops`) and the two `pg_upgrade` properties (on `origin/pg_upgrade`/`origin/nickb/pg_upgrade_test`) are the same shape of evidence, with one difference worth noting: the branch-sweep pair's mechanisms **were** independently re-traced line-by-line against the current worktree (not merely accepted from the branch's commit message), so they carry higher confidence than this bullet's original four despite the shared "sourced from an unmerged branch" caveat.
- Every property that needs a primary/streaming-standby topology (a majority of the "Streaming Replication" properties, and several "Checkpoint / Recovery Boundary Consistency" properties) currently cannot be implemented at all in the existing Antithesis harness, which has no second Postgres node. This is `sut-analysis.md` §9's single largest flagged coverage gap and affects roughly a third of this catalog. **Correction (evaluation, Implementability lens):** no property in the "Recovery Worker Concurrency & Resource Boundaries" category actually requires a standby — `orioledb.recovery_pool_size`/`orioledb.recovery_idx_pool_size` both default to 3 (`PGC_POSTMASTER`), so parallel recovery/index-build worker pools are already active during ordinary single-node crash recovery; `recovery-worker-idxbuild-stall` and `recovery-worker-redispatch-consistency`'s entries below have been corrected to remove wording that implied otherwise.

## Open Questions (catalog-wide)

- ~~Are Antithesis's target build images compiled with C-level `Assert()` enabled?~~ **Resolved** (evaluation pass, Implementability lens): confirmed `--enable-cassert` in `test/antithesis/orioledb/Dockerfile` for core Postgres, which `orioledb.so` is built and linked against in the same image — so `Assert()` sites are not no-ops in the images the harness actually builds. This affected three properties directly (`replica-xmin-monotonicity`, `multi-insert-undo-capacity-invariant`, `replica-undo-reclaimed-too-early`), whose entries have been updated accordingly; each `Assert()`-only invariant still gives a stronger signal if paired with an explicit, unconditional Antithesis SDK assertion (a TRAP/crash is detected generically, but a wired assertion attributes the failure precisely), which remains worth doing as a hardening step, not because the signal is currently absent.
- **`orioledb_tbl_check()`/`verify_orioledb()` concentration risk.** Roughly 11 of the catalog's 53 properties (up to ~13 if two gap-fill properties whose open questions depend on it are counted) name `orioledb_tbl_check()`/`verify_orioledb()`/`check_extents()` as their sole or primary structural oracle: `sk-fixup-undo-recycling-drop`, `checkpoint-recovery-lsn-sync-gap`, `sk-extent-leak-after-crash`, `multi-insert-undo-capacity-invariant`, `recovery-worker-redispatch-consistency`, `non-modify-wal-record-replay-safety`, `tbl-check-oracle-transient-false-negative` (the property *about* the oracle itself), plus the gap-fill additions `backup-restore-lacks-structural-oracle` and `pg-rewind-orioledb-fullcopy-correctness` (both propose adding it as a new check) — `recovery-sk-rebuild-desync` and `checkpoint-corrupted-tree-silent-skip` are the two borderline cases (the former explicitly documents the oracle failing to catch its bug class; the latter's open question is precisely whether the oracle would catch it at all). This is a meaningful concentration: if this single oracle is ever unreliable in some *new* way beyond its already-documented transient-false-negative window (`tbl-check-oracle-transient-false-negative`), a large fraction of the catalog's assertions could silently stop being load-bearing at once. Recommend an independent, content-level cross-check (e.g., comparing actual row/index-entry bytes across a checkpoint boundary, not just the checker's own boolean/NOTICE output) be built for at least the highest-priority properties in this set, rather than relying on one oracle implementation for so much of the catalog's coverage.
- **Scope exclusions**: rewind and S3/decoupled-storage functionality were explicitly excluded from this research pass (see the top-of-file scope note). This is a deliberate narrowing by user decision, not a coverage gap discovered by analysis — but it means a substantial, independently-flagged defect-dense subsystem (rewind: 164 commits, zero Antithesis exposure, a plausible container-topology interaction with the harness's process model per `sut-analysis.md` §12) is not represented anywhere in this catalog. Any future work resuming that scope should start from `sut-analysis.md` §2/§6/§7/§8/§11/§12 rather than rediscovering it.
- **The primary/standby replication topology gap**: `sut-analysis.md` §9 identifies "no primary/standby replication scenario under fault injection anywhere" as the single largest Antithesis coverage gap in the existing harness, and a large fraction of this catalog's highest-value properties (the entire "Streaming Replication / GlobalXmin Coordination" category, `checkpoint-abort-snapshot-standby-panic`, `checkpointer-startup-lock-drain-progress`, `malformed-wal-container-double-finish`) cannot be tested at all until this topology exists. Building this topology should likely be the top implementation priority handed to `antithesis-setup`/`antithesis-workload` once this catalog is used.
- **`/Users/artur/supabase/orioledb_postgres` is no longer an active external reference** for this catalog, per explicit user instruction, even though several properties above (and the underlying `sut-analysis.md`) were originally informed by reading it during an earlier phase of this research effort, before the scope was narrowed. Findings that depended on that source (e.g., confirming patched-Postgres hook call sites, or the checkpointer's deadlock-checker bootstrap) are preserved in evidence files as historical record, but no further reading of that path should occur.
- **`orioledb_tbl_check()`'s reliability as a general-purpose oracle beyond the #855 shape it was built for** is itself now a cataloged property (`tbl-check-oracle-transient-false-negative`) rather than an assumption — but its finding (a documented, currently-live transient false-negative window) means every *other* property in this catalog that proposes reusing the existing `sk-recovery-race[-chaos]` oracle (`orioledb_tbl_check()` + PK/SK count comparison) inherits this caveat. Where a property's evidence file notes the oracle "does not catch this class of bug" (`recovery-sk-rebuild-desync`, `sk-overwrite-callback-identity-dedup`), the count-comparison half of the assertion, not the structural-check half, is doing the real work.
