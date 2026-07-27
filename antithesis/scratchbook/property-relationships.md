---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: In-repo documentation site (doc/architecture/*.mdx, doc/usage/*.mdx, doc/contributing/*.mdx) is the primary source of claimed guarantees and product framing; treated as leads to validate, not facts.
---

# Property Relationships: OrioleDB

Lightweight grouping of the 53 properties in `property-catalog.md` by shared
evidence, code paths, or failure mechanism. This is a map for prioritization
and triage correlation, not new analysis — see each property's own evidence
file (`properties/{slug}.md`) for the underlying trace. Per the scope
restriction noted in `property-catalog.md`, no cluster here touches rewind or
S3/decoupled-storage.

Updated after the evaluation pass (`evaluation/synthesis.md`) added 15
gap-fill properties to the original 38: Clusters 1-8 and the "Loosely
connected" section below are the original clustering, updated in place to
fold in new members where they share a mechanism (noted inline); Clusters 9,
10, and 11 are new.

## Cluster 1: orioledb#855 checkpoint-boundary family (PK/SK consistency)

**Members:** `sk-fixup-undo-recycling-drop`, `sk-fixup-sentinel-spin-livelock`,
`checkpoint-recovery-lsn-sync-gap`, `recovery-sk-rebuild-desync`,
`sk-extent-leak-after-crash`, `checkpoint-abort-snapshot-standby-panic`,
`recovery-meta-lock-signal-barrier-deadlock` (gap-fill addition),
`checkpoint-corrupted-tree-silent-skip` (gap-fill addition).

The largest and highest-priority cluster. All six share the same failure
shape (PK/secondary-index or extent-accounting divergence across a checkpoint
boundary) and largely the same oracle: the existing `sk-recovery-race[-chaos]`
harness's `always()` (PK-row-count == distinct-SK-token-count +
`orioledb_tbl_check()`). They differ in *which* timing window produces the
divergence:

- `sk-fixup-undo-recycling-drop` and `sk-fixup-sentinel-spin-livelock` are two
  distinct bugs (safety vs. liveness) in the *same function*,
  `checkpoint_write_pending_sk_fixups()` — testing one workload shape
  (widened checkpoint-to-replay gap with undo churn) is likely to also
  exercise the sentinel-spin code path if the workload includes
  self-created-table DML, since both fast paths live in the same per-proc
  loop.
- `checkpoint-recovery-lsn-sync-gap` is upstream of the whole fixup mechanism:
  it concerns the correctness of `toastConsistentPtr` itself, which
  `checkpoint_write_pending_sk_fixups()` uses as its boundary. **Suspected
  dominance**: if `checkpoint-recovery-lsn-sync-gap`'s workload (checkpoint
  during active multi-worker recovery) is built, it plausibly also increases
  the chance of triggering `sk-fixup-undo-recycling-drop`'s recycled-undo
  condition, since both need the same "checkpoint boundary racing recovery
  worker progress" shape — building one workload may cheaply extend coverage
  toward the other.
- `checkpoint-abort-snapshot-standby-panic` is a structurally distinct bug
  (in-flight-oxid snapshot in `finish_write_xids()`, not the SK-fixup path)
  but is the *next function called in the same checkpoint routine*
  (`o_perform_checkpoint()`) and requires the same standby topology as
  `checkpoint-recovery-lsn-sync-gap` — a single "checkpoint + standby +
  scheduling delay" workload is a natural chassis for both.
- `recovery-sk-rebuild-desync` and `sk-extent-leak-after-crash` are the two
  properties in this cluster explicitly confirmed to bypass
  `orioledb_tbl_check()`'s structural check (in `recovery-sk-rebuild-desync`'s
  case) or to require inspecting `check_extents()`'s specific NOTICE text
  rather than the boolean (`sk-extent-leak-after-crash`) — both are reminders
  that the shared oracle's boolean pass/fail is not sufficient for this whole
  cluster; see Cluster 6 (`tbl-check-oracle-transient-false-negative`) for the
  oracle-trust angle that cuts across all of them.
- `recovery-sk-rebuild-desync` is flagged as the single highest-priority,
  likely-still-open finding in this cluster (no fix commit found) — it should
  probably be the first workload built in this cluster if only one can be
  prioritized, since the existing chaos driver's workload shape (sustained
  DML + generic fault injection) is close to sufficient already.
- `recovery-meta-lock-signal-barrier-deadlock` and `checkpoint-corrupted-
  tree-silent-skip` (both gap-fill additions from a fresh branch sweep
  during evaluation) join this cluster by discovery method and severity
  shape (both are concrete, independently re-traced bugs on unmerged
  branches, structurally similar to `checkpoint-abort-snapshot-standby-panic`
  and `sk-extent-leak-after-crash`) rather than by sharing PK/SK-fixup code
  paths specifically. **Important distinction from the rest of this
  cluster**: unlike `recovery-finish-abort-livelock`/`replica-xmin-
  monotonicity` in Cluster 2 (regression guards for bugs already fixed on
  `main`), these two are **confirmed still-open defects** — `git merge-base
  --is-ancestor` confirms neither fix (`1df605da`; `af851ce4`/`d482623e`) is
  an ancestor of the analyzed commit. A workload built for either tests for
  an existing bug, not a fix regressing.

## Cluster 2: Streaming-standby xmin/livelock family (orioledb#876 / #889)

**Members:** `recovery-finish-abort-livelock`, `replica-xmin-monotonicity`,
`replica-globalxmin-catchup-lag`, `replica-undo-reclaimed-too-early`,
`replica-undo-cleanup-bgwriter-spof` (gap-fill addition),
`recovery-worker-commit-visibility-barrier` (gap-fill addition).

All four come from the same chaos-hunt investigation (root-cause docs on
`origin/add_stress_bank_account_test`) and require the same topology (primary
+ streaming standby, primary `SIGKILL` under concurrent load). Two
(`recovery-finish-abort-livelock` for #876, `replica-xmin-monotonicity` for
#889) independently corrected the same stale `sut-analysis.md` claim via
identical git-forensics methodology — the fix commits explicitly cross-
reference each other (`a0d628c1`'s own message: "All three #876 / #889 /
floor-seeding replication tests still pass"), confirming the two bugs share a
fix lineage even though they are distinct mechanisms. **Suspected
dominance**: a single workload (long transactions below the WAL-buffer
threshold + bursts of short commits + checkpoint + `SIGKILL`, per the shared
root-cause doc's own reproducer) is very likely to exercise both #876's and
#889's code paths simultaneously, since the doc's own test suite validates
them together. `replica-undo-reclaimed-too-early` was observed as an
*alternative* outcome of the exact same hunt campaign (not a separately
constructed scenario) — running the same workload with assert-enabled builds
is likely to surface whichever of the three bugs (livelock, xmin regression,
undo-reclaim assert) the specific fault timing happens to hit.
`replica-globalxmin-catchup-lag` is the odd one out directionally (a stuck-low
horizon, not a regression) but shares the identical topology and sampling
mechanism (`orioledb_get_xid_meta()` on both nodes) — cheap to check
alongside `replica-xmin-monotonicity` in the same test run.

This cluster and Cluster 1 both depend on the same missing primary/standby
topology (`sut-analysis.md` §9's top-priority gap) — see also
`checkpointer-startup-lock-drain-progress` and
`malformed-wal-container-double-finish` in Clusters 4 and 6, which share the
same topology dependency without sharing code paths. Building this topology
once unblocks roughly a third of the catalog at once.

`replica-undo-cleanup-bgwriter-spof` and `recovery-worker-commit-visibility-
barrier` (both gap-fill additions) join this cluster by topology and
"replica liveness/correctness under the same missing standby" theme, not by
sharing the xmin/livelock mechanism itself: `replica-undo-cleanup-bgwriter-
spof` is about a different subsystem entirely (`src/workers/bgwriter.c`'s
`BGWriterNum == 0` single-point-of-responsibility for undo-file cleanup, a
different oracle — `orioledb_has_retained_undo()`, not `orioledb_get_xid_
meta()`); `recovery-worker-commit-visibility-barrier` is about the
`finished_list`/`get_workers_commit_ptr()` cross-worker CSN-release gate,
distinct from both `globalXmin` horizon-tracking (this cluster's other four
members) and from the leader/worker distribution-side liveness concerns in
Cluster 3. All three "distinct mechanism, same topology" additions to this
cluster reinforce the same point already made above: whoever builds the
primary/standby topology should expect to validate several independent
mechanisms against it, not just the original #876/#889 pair.

## Cluster 3: Recovery worker leader/worker protocol family

**Members:** `recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-leader`,
`recovery-worker-redispatch-consistency`, `non-modify-wal-record-replay-safety`,
`sk-overwrite-callback-identity-dedup`.

All five concern the same `shm_mq`-based parallel-recovery leader/worker
protocol in `src/recovery/recovery.c` (the single hottest file in the
codebase). `recovery-worker-idxbuild-stall` (workers waiting on a leader) and
`recovery-worker-stall-blocks-leader` (leader waiting on workers) are
explicitly noted in their own evidence files as symmetric cases of the same
underlying gap — the leader/worker protocol generally lacks wedged-peer
detection in either direction. A single "kill/freeze one specific recovery
process mid-replay" fault-injection primitive, aimed at different targets
(the index-build leader vs. an ordinary worker), covers both. `non-modify-wal-
record-replay-safety` and `sk-overwrite-callback-identity-dedup` are both
about the *content*-correctness side of replay (does a record type replay
safely twice; does the SK-overwrite dedup trust identity too readily) rather
than the liveness side, but both are scope gaps discovered while examining
the same recovery-replay dispatch code (`apply_btree_modify_record()` and its
neighbors) that Cluster 1's `sk-fixup-undo-recycling-drop` also reads —
worth building alongside Cluster 1's workloads rather than as separate
harness work. `recovery-worker-redispatch-consistency` is the most reassuring
finding in this cluster (mostly rules out its own suspected bug) and is lower
priority than the other four.

**Correction (evaluation, Implementability lens):** despite the "Recovery
Worker Concurrency" framing, none of the five members above actually require
a standby — `orioledb.recovery_pool_size`/`orioledb.recovery_idx_pool_size`
both default to 3 (`PGC_POSTMASTER`), so this whole cluster's parallel
worker pools are already active during ordinary single-node crash recovery.
`recovery-worker-idxbuild-stall` and `recovery-worker-redispatch-
consistency`'s catalog entries previously read as if they might need the
(not-yet-built) standby topology; they don't, and have been corrected.

## Cluster 4: Checkpointer/lock-manager concurrency family

**Members:** `checkpointer-heavyweight-lock-deadlock`,
`checkpointer-startup-lock-drain-progress`.

Both stem from the same architectural fact: the patched checkpointer process
now bootstraps deadlock-detection and catalog/invalidation machinery it never
needed in stock Postgres, because OrioleDB requires it to take heavyweight
relation locks. `checkpointer-heavyweight-lock-deadlock` is the general case
(any concurrent backend contending for the same relation lock);
`checkpointer-startup-lock-drain-progress` is a specific, already-self-
documented instance of the same class (startup-process-on-a-standby holding
the lock while blocked on a full sync-request queue). The second property
requires a standby topology (Cluster 2's dependency); the first does not and
could be built first as a single-node precursor using the same stopevents
(`checkpoint_step`, `checkpoint_table_start`, `checkpoint_index_start`).

**Not a member, but catalogued in the same category — `oxid-notify-all-
proclock-panic`** (gap-fill addition): grouped with this pair in
`property-catalog.md` because it shares the category's general "OrioleDB
re-derives part of the core lock manager's own bookkeeping outside the
public API" subject matter, but it is a genuinely different mechanism —
`LOCKTAG_VIRTUALTRANSACTION` locks and hand-rolled wait-queue surgery
triggered by ordinary subxact-abort/`INSERT ... ON CONFLICT` DML, not
`LOCKTAG_RELATION` locks or the checkpointer process at all — kept out of
the "Members" list above (rather than added as a third member) to avoid
implying a shared fix/workload chassis that doesn't actually exist; it is
also cross-referenced from "Loosely connected" below.

## Cluster 5: Undo circular-buffer capacity family

**Members:** `multi-insert-undo-capacity-invariant`, `undo-wraparound-retry-cap`.

Both concern the same undo circular-buffer sizing assumption
(`max_procs * 2 * O_MAX_UNDO_RECORD_SIZE`, `src/transam/undo.c`), from two
different angles: `multi-insert-undo-capacity-invariant` is about whether a
single caller's reservation cap holds under concurrent multi-row inserts;
`undo-wraparound-retry-cap` is about whether the buffer's wrap-boundary retry
logic holds under adversarial *global* concurrent allocation across all
callers. Both share an unresolved open question about whether `max_procs`
correctly counts every process type (including background workers) that can
hold a same-type undo reservation — resolving that question once (tracing
`max_procs`'s definition site) would sharpen both properties simultaneously.

## Cluster 6: WAL/binary-format version-contract family

**Members:** `wal-recovery-rejects-future-version`, `wal-decode-rejects-future-version`,
`wal-decode-malformed-container-fails-safe`, `wal-older-version-seamless-conversion`,
`malformed-wal-container-double-finish`, `checkpoint-control-version-gate-fails-safe`,
`page-version-mismatch-fails-safe`, `disk-leaf-header-read-before-validation`.

All eight concern binary-format/version contracts, but at three distinct
layers that should not be conflated:

- **WAL container version/structure** (`wal-recovery-rejects-future-version`,
  `wal-decode-rejects-future-version`, `wal-decode-malformed-container-fails-safe`,
  `wal-older-version-seamless-conversion`, `malformed-wal-container-double-finish`)
  — all route through the same shared gate, `wal_container_read_header()`/
  `wal_parse_container()` in `src/recovery/wal_reader.c`, just exercised via
  different consumers (crash recovery vs. logical decoding) and different
  fault shapes (future version, malformed bytes, older version, or a
  same-oxid double-finish race). `wal-older-version-seamless-conversion` is
  the odd one out: it identifies that the harness's `IS_DEV=1` build makes
  the entire "older version" branch of this shared gate unreachable today,
  which is a build-matrix precondition for ever testing that specific
  direction — worth resolving before investing in the "older WAL version"
  half of this sub-cluster.
- **Checkpoint-control-file version** (`checkpoint-control-version-gate-fails-safe`)
  and **page/compression version** (`page-version-mismatch-fails-safe`) are a
  separate, lower-level pair — both verified-correct-today findings with
  forward-looking risk (a future version bump introducing a real, currently-
  nonexistent conversion path). Structurally similar shape to each other
  (both found via the same "does an earlier-passing gate silently skip a
  later, finer check" investigation prompted by `sut-analysis.md` §2), but
  not the same code path as the WAL-container sub-cluster above. **Explicit
  cross-reference (evaluation, R11):** these two are near-redundant in shape
  and were flagged by the Wildcard lens as candidates for merging — kept as
  two properties since they gate genuinely different constants/artifacts,
  but a workload/fixture built for one should be checked for reuse against
  the other; see the cross-reference note now present in both properties'
  catalog entries and evidence files.
- `disk-leaf-header-read-before-validation` was discovered *while* tracing
  the page-version work above, but is a distinct, concrete ordering bug (not
  a version-contract question) — it belongs here by discovery context, not
  by shared mechanism with the other seven.
- **Gap-fill cross-reference:** `pg-upgrade-cross-major-cache-reset-gap`
  (Cluster 10) directly touches `checkpoint-control-version-gate-fails-safe`'s
  own mechanism — the unmerged `pg_upgrade` branch bumps
  `ORIOLEDB_CHECKPOINT_CONTROL_VERSION` and adds a v1→v2 conversion path
  inside `check_checkpoint_control()`, a live instance of exactly the
  "finer-grained version gate" scenario this cluster already thinks about.
  Not added as a member here (it's a forward-looking, not-yet-merged
  feature, and belongs with the other `pg_upgrade` property by discovery
  context) but worth checking together whenever either is prioritized.

## Cluster 7: Isolation-mode / row-level concurrency family

**Members:** `serializable-table-lock-untested`, `serializable-error-mode-truncate-gap`,
`pk-update-chain-race-consistency`.

All three concern SQL-visible isolation/concurrency contracts, and the first
two share a literal root finding: no existing harness config ever sets
`orioledb.serializable` or requests `SERIALIZABLE` isolation at all.
`serializable-table-lock-untested` is the dominant property in this pair — it
is about the *default* mode (`table_lock`) being completely untested, and
building its recommended jepsen-variant workload (setting
`JEPSEN_ISOLATION=serializable`) is a prerequisite for `serializable-error-
mode-truncate-gap`'s already-low-priority scenario ever being reachable at
all. `pk-update-chain-race-consistency` is mechanistically unrelated (it's
about a documented delete+reinsert race, not the `orioledb.serializable` GUC)
but shares the same attention-focus origin (row-level concurrency claims in
`doc/architecture/`) and the same "verify a claimed guarantee stays exactly
as narrowly scoped as documented" methodology.

## Cluster 8: Test-oracle / harness-integrity meta-properties

**Members:** `tbl-check-oracle-transient-false-negative`, `jepsen-verdict-not-sdk-visible`,
`chaos-driver-skips-check-on-fault-landing`, `core-postgres-hook-coverage-blind-spot`.

All four are properties about the harness's own verification pipeline rather
than about OrioleDB's runtime behavior. They compose with, and qualify, every
other cluster above: `tbl-check-oracle-transient-false-negative` directly
weakens confidence in every Cluster-1 property that reuses
`orioledb_tbl_check()`'s boolean result as part of its assertion (a stale
checkpoint-adjacent `false` could be misread as a genuine Cluster-1
violation, or — worse in the other direction — genuinely corrupted structure
could theoretically be masked by the same known-flaky window, though this
second direction wasn't independently confirmed). `jepsen-verdict-not-sdk-
visible` and `chaos-driver-skips-check-on-fault-landing` are both "the check
exists but isn't wired up / gets skipped" findings, on two different
workloads (jepsen vs. `sk-recovery-race-chaos`) — same class of gap,
different code. `core-postgres-hook-coverage-blind-spot` is the most
structurally distinct of the four (it's about coverage-guided search's
*exploration* signal, not about an assertion's correctness or wiring), but is
grouped here because it's likewise a claim about the harness/build pipeline's
trustworthiness rather than about OrioleDB itself. **Suggested triage
practice**: if any Cluster 1-7 property reports a violation, check this
cluster's findings first (was the oracle in its known-flaky window? was the
relevant assertion even wired up to fire?) before treating the violation as a
confirmed new bug.

**Related gap-fill meta-property (not a member, different workload):**
`backup-restore-lacks-structural-oracle` (Cluster 10) is the same *kind* of
finding — an existing test suite's oracle (here, pgbackrest/wal-g's
integration tests) checking content equality but never OrioleDB structural
integrity — applied to a different workload than any of the four members
above. Kept in Cluster 10 by workload rather than added here, but the same
"a property whose invariant only gets checked in the easy case is weaker
than its name suggests" lesson from this cluster applies directly.

## Cluster 9: Resource-boundary / infrastructure-fault family (gap-fill addition)

**Members:** `recovery-idxbuild-registration-fallback-bug`,
`bgwriter-worker-slot-exhaustion-silent`, `disk-write-enospc-fails-safe`,
`page-pool-exhaustion-fails-safe`.

All four fill the same evaluation gap (G1: resource-boundary/infrastructure-
fault properties were thin — only 2/38 pre-existing properties, both scoped
to the undo buffer). They share a *theme* (what happens when a fixed-size
or externally-limited resource is exhausted) rather than one code path, and
split into two pairs by discovery shape:

- `recovery-idxbuild-registration-fallback-bug` and `bgwriter-worker-slot-
  exhaustion-silent` are both about `max_worker_processes`/background-worker
  slot exhaustion, but via two different registration mechanisms (dynamic
  `RegisterDynamicBackgroundWorker()` for the recovery/index-build pools vs.
  static `RegisterBackgroundWorker()` for the bgwriter) with two different
  failure shapes — the first is a concrete, directly-confirmed control-flow
  bug (missing `break`, off-by-one); the second is a design gap (zero
  detection/fallback at all, since the static API returns `void`). Both are
  reachable purely via `max_worker_processes` config, no fault injection
  needed to trigger.
- `disk-write-enospc-fails-safe` and `page-pool-exhaustion-fails-safe` are
  both "verified-mostly-correct contract" findings (the common case is
  well-engineered) with one residual untested edge: disk-full's torn-write-
  on-the-control-file scenario, and the page pool's nested multi-pool-type
  recursion depth assumption (`Assert`-only, not live-checked).

None of the four require a replication topology — all four are reachable in
the existing single-node harness via config/GUC changes (`max_worker_
processes`, a quota-limited filesystem, `orioledb.main_buffers`) rather than
process-level fault injection, making this cluster a good candidate to build
in parallel with (not blocked on) the standby-topology work Clusters 1/2/4
depend on — see the Biases section of `evaluation/synthesis.md` for the
same sequencing observation.

## Cluster 10: Backup, Restore, and Major-Version Upgrade family (gap-fill addition)

**Members:** `backup-restore-lacks-structural-oracle`, `backup-window-crash-untested`,
`pg-rewind-orioledb-fullcopy-correctness`, `replay-until-lsn-catalog-desync`,
`pg-upgrade-cross-major-cache-reset-gap`, `pg-upgrade-manual-data-copy-not-atomic`.

Fills evaluation gaps G2 (backup/restore under fault injection: zero
properties despite substantial pgbackrest/wal-g integration suites) and G6
(`pg_upgrade` cross-major support: a substantial, actively-developed,
in-scope feature with zero prior representation). Two sub-groups by
workflow, not by shared code path:

- **Backup/restore proper** (`backup-restore-lacks-structural-oracle`,
  `backup-window-crash-untested`, `pg-rewind-orioledb-fullcopy-correctness`,
  `replay-until-lsn-catalog-desync`): the first two both concern the
  *existing* pgbackrest/wal-g integration tests — one strengthens their
  oracle (content-equality → structural check), the other adds the one
  fault-injection angle (crash/kill mid-backup-consistency-window) neither
  test constructs. `pg-rewind-orioledb-fullcopy-correctness` covers a third,
  currently zero-coverage tool with a mechanism unique among the three
  (OrioleDB WAL carries no block-reference metadata at all, so `pg_rewind`'s
  incremental-diff path never even engages — full-copy is the *only* path).
  `replay-until-lsn-catalog-desync` is mechanistically unrelated to the
  other three (no external tool involved — a GUC-triggered internal
  redo-callback state machine) but shares the same "self-disclosed risky
  mechanism, zero test coverage" shape and the same evaluation gap.
- **`pg_upgrade`** (`pg-upgrade-cross-major-cache-reset-gap`,
  `pg-upgrade-manual-data-copy-not-atomic`): both concern the same unmerged,
  actively-developed cross-major-upgrade branches (`origin/pg_upgrade`,
  `origin/nickb/pg_upgrade_test`), neither present on `main`. The first is a
  self-acknowledged-incomplete audit of which `SYS_TREES_*` caches need a
  reset on major-version change (backed by two real, already-fixed crash
  bugs found during the branch's own development); the second is a lower-
  confidence, reasoned-not-observed gap in the documented manual `cp -R`
  step's lack of atomicity. See Cluster 6 for `pg-upgrade-cross-major-
  cache-reset-gap`'s direct mechanism overlap with `checkpoint-control-
  version-gate-fails-safe`.

None of this cluster's properties are implementable today in the sense of
"build a workload now" — the backup/restore quartet needs either new
harness scaffolding (a fault-injection point inside an existing integration
test) or a new topology (the standby, for `pg-rewind-orioledb-fullcopy-
correctness`); the `pg_upgrade` pair is blocked on the feature branch
merging at all.

## Cluster 11: Cross-cutting "unbounded busy-wait, no `CHECK_FOR_INTERRUPTS()`" pattern (evaluation R14)

**Members:** `sk-fixup-sentinel-spin-livelock` (Cluster 1),
`recovery-worker-idxbuild-stall` (Cluster 3), `recovery-worker-stall-blocks-
leader` (Cluster 3), `checkpointer-startup-lock-drain-progress` (Cluster 4).

**Not a new set of properties** — this cluster names a pattern that recurs
*across* three existing clusters without ever being called out as one
finding (per the Wildcard evaluation lens): a polling/wait loop that either
lacks `CHECK_FOR_INTERRUPTS()` entirely (`sk-fixup-sentinel-spin-livelock`,
`recovery-worker-stall-blocks-leader`'s `workers_synchronize()`) or has no
outer give-up bound despite checking interrupts per-iteration
(`recovery-worker-idxbuild-stall`, `checkpointer-startup-lock-drain-
progress`). All four call sites sit in checkpoint/recovery/checkpointer
coordination code specifically — the inconsistency clusters there, not
randomly across the codebase.

**Practical implication for whoever builds these four workloads**: every
one of the four properties' "Antithesis Angle" originally reached only for
container/process-level fault injection (`SIGSTOP`, CPU throttling,
scheduling delay) to test interruptibility. `pg_cancel_backend()`/
`pg_terminate_backend()`/`statement_timeout` targeted at the specific
backend/process PID is a strictly cheaper and more direct SQL-level test of
the same underlying question (does this loop ever consult
`CHECK_FOR_INTERRUPTS()`) — each of the four catalog entries now includes
this as an additional Antithesis Angle note. Building one "target this PID
with a cancel/timeout" test primitive serves all four rather than four
bespoke ones.

## Loosely connected / standalone properties

- `oxid-notify-all-proclock-panic` (gap-fill addition) is catalogued
  alongside Cluster 4 (checkpointer/lock-manager concurrency) by subject
  matter (OrioleDB re-deriving core lock-manager bookkeeping outside the
  public API) but is mechanistically unrelated to either of that cluster's
  two members — see Cluster 4's note above. No other property in this
  catalog shares its trigger (subxact-abort / `INSERT ... ON CONFLICT`) or
  lock type (`LOCKTAG_VIRTUALTRANSACTION`).
- `readiness-gate-standby-recovery-lag` shares Cluster 2's topology
  dependency (it's explicitly about a *future* standby's readiness signal)
  but doesn't share a bug mechanism with any Cluster 2 member — it's a
  process/config note for whoever builds the standby topology, not a
  workload to run on its own.
- `checkpoint-stats-view-pg-major-branch` is a regression guard on the
  `sk-recovery-race-chaos` driver itself (Cluster 1's workload vehicle) but
  concerns a harness bug unrelated to any of Cluster 1's actual data-
  integrity findings — grouped with Cluster 8 in spirit (a harness-
  reliability finding) but kept separate in the catalog because it's a
  version-compatibility finding about test code, not about the SUT.
- `o-sys-cache-invalidation-race` and `orioledb-requires-preload-clean-
  failure` are both genuinely standalone: no other property in this catalog
  shares their code path (the catalog-duplicate cache, and the shared-memory
  preload gate, respectively), and both are explicitly lower-confidence/
  speculative findings in their own evidence files.
