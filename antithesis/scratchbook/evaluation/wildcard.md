---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: in-repo documentation site, consulted as background if needed
---

# Wildcard Evaluation: OrioleDB Property Catalog

Lens: Wildcard (no fixed domain). Task: question the framing the other three lenses
accepted, find missing perspectives, cross-cut their domains, and report anything
that feels wrong without fitting a category. This is a critical evaluation — the
goal is to find real problems, not to validate the catalog.

Method: read all five required documents in full, then used `git` directly
(remote-branch listing, `git log`, `git show`, `git merge-base --is-ancestor`) to
independently verify claims and to look for evidence the 11 discovery agents'
methodology might have systematically missed. `orioledb_postgres` was not
consulted, per the scope restriction. Rewind and S3 are not reopened as topics;
where they appear below it is only because an *in-scope* property or mechanism
touches them, per the task's explicit carve-out for that angle.

---

## Headline finding: the catalog's own discovery methodology missed real, already-diagnosed bugs sitting on active branches in the exact subsystems it targets

The catalog was assembled by grepping/reading `main`'s git history plus two
specific unmerged branches (`origin/add_stress_bank_account_test`,
`origin/add_stress_test_pr`) that happened to contain written root-cause docs.
That's a good technique, but it's incomplete: it treats "unmerged branch with a
root-cause doc" as the only source of pre-diagnosed bugs, rather than scanning
*all* active branches for recent commits touching the same hot files
(`recovery.c`, `checkpoint.c`, `undo.c`) the catalog already identifies as the
highest-value territory. I ran `git branch -r --sort=-committerdate` and spot-
checked a handful of the most recent branches against those files. In about 20
minutes this surfaced three concrete, real bugs — confirmed via
`git merge-base --is-ancestor <fix> a975c702` to be **present, unfixed, on the
exact commit this catalog analyzes** — none of which appear anywhere in
`sut-analysis.md` or `property-catalog.md`:

1. **Standby recovery-leader self-deadlock via `oTablesMetaLock` held across
   replay of unrelated WAL records** (`origin/recovery-meta-buffering`, fix
   `1df605da`, dated 2026-07-24 — 3 days before the analyzed commit; **not** an
   ancestor of `a975c702`). Commit message, verbatim: "The standby recovery
   leader held `oTablesMetaLock` (SHARED) from a replayed
   `WAL_REC_O_TABLES_META_LOCK` until the matching `WAL_REC_O_TABLES_META_UNLOCK`.
   Unrelated records interleaved into that WAL window (notably `dbase_redo`,
   which runs `WaitForProcSignalBarrier(SMGRRELEASE)`) were then replayed while
   the leader held the LWLock — and `LWLockAcquire`'s `HOLD_INTERRUPTS` makes
   `ProcessInterrupts` (hence `ProcessProcSignalBarrier`) a no-op, so the leader
   could never absorb its own barrier and self-deadlocked (replay froze; the
   standby never caught up)." The team already wrote a deterministic repro
   (`test/t/meta_lock_deadlock_test.py`, a `before_o_tables_meta_unlock`
   stopevent) and confirmed the freeze. This is precisely the failure shape
   `checkpointer-startup-lock-drain-progress` circles around (same lock family,
   same "standby replay freezes forever" outcome) but is a *different, already-
   confirmed* root cause the catalog's own text characterizes only as a
   speculative, untested liveness bound ("the claimed liveness bound... has no
   numeric backing in code"). The catalog undersells its own best lead in this
   area by not finding this.
2. **`workers_synchronize()`'s busy-loop has no `CHECK_FOR_INTERRUPTS()`**
   (`origin/nickb/fix_worker_wait_for_sync`, fix `eaeb556f`, dated 2026-07-27 —
   the *same day* as the analyzed commit; **not** an ancestor). This is the
   exact bug `recovery-worker-stall-blocks-leader` describes almost verbatim
   ("`workers_synchronize()`'s busy-poll has no backoff and no
   `CHECK_FOR_INTERRUPTS()`"). Independent confirmation that the property is
   real and well-targeted — but also a sign it is likely to be fixed within
   days of any workload being built around it, and should probably already be
   framed as "regression guard, imminent fix" rather than "open gap," the same
   correction the catalog already had to make for orioledb#876/#889.
3. **A checkpoint can silently skip a corrupted tree from sys-tree bookkeeping
   and still report success** (`origin/checkpoint-io-error-fatal` /
   `origin/checkpoint_avoid_error_loops`, fix `af851ce4`/`d482623e`, dated
   2026-07-22/27; **not** an ancestor). Commit message: "When
   `read_page_from_disk` fails in `evictable_tree_init_meta`, the ERROR came
   before the `sharedRootInfo` was inserted into
   `SYS_TREES_SHARED_ROOT_INFO`. On the next checkpoint cycle
   `o_find_shared_root_info` returns NULL, `tree_is_under_checkpoint` returned
   true..., and `o_btree_load_shmem_internal` gave up, so the corrupted tree
   was silently skipped and the checkpoint succeeded." This is a genuine
   silent-corruption-adjacent bug — a checkpoint reporting success while
   quietly excluding a corrupted tree — in exactly the failure category
   `sut-analysis.md` §10 calls the worst-case for a database engine, and it has
   no cataloged property anywhere close to it (`disk-leaf-header-read-before-
   validation` is a different call site and a different failure shape —
   ordering of a read vs. a validation check during a *scan*, not a
   checkpoint's tree-load path silently dropping a tree from its own
   bookkeeping).

**Why this matters beyond "three more bugs to add":** it's evidence the
catalog's search process itself has a gap, not just its output. A systematic
`git log --all --since=<cutoff> -- src/recovery/ src/checkpoint/ src/transam/`
sweep (or equivalent) across *all* branches — not just the two that happened to
contain prose root-cause docs — would very likely surface more of these, and
each one found this way is unusually high-confidence evidence (the team has
already reproduced it, sometimes with a purpose-built deterministic test)
compared to the catalog's more common "confirmed by direct code reading, not
independently re-derived" caveat. This is cheap to do and should be a
standing step before any future revision of this catalog, not a one-off.

**Suggested action:** add at least the `oTablesMetaLock` self-deadlock as a new
property (it's arguably higher-priority than several existing Cluster 4
entries — it has a real repro, a real fix, and directly threatens the
single largest coverage gap, i.e., anything requiring the not-yet-built standby
topology); re-run the branch sweep before `antithesis-workload` starts
implementation, since several of these fixes may land on `main` between now
and then and would otherwise silently invalidate whichever properties assumed
them still-open.

---

## Finding: `orioledb_tbl_check()` is the load-bearing oracle for roughly a quarter of the catalog, and its blast radius is underweighted relative to its own documented instability

Counting properties whose stated invariant leans on `orioledb_tbl_check()`
(alone or paired with the PK-count/SK-token-count comparison): all six of
Cluster 1 (`sk-fixup-undo-recycling-drop`, `sk-fixup-sentinel-spin-livelock`,
`checkpoint-abort-snapshot-standby-panic`, `checkpoint-recovery-lsn-sync-gap`,
`recovery-sk-rebuild-desync`, `sk-extent-leak-after-crash`), plus
`recovery-worker-redispatch-consistency`, `non-modify-wal-record-replay-safety`,
and `multi-insert-undo-capacity-invariant` — nine properties, roughly a quarter
of the 38. `sut-analysis.md` §8 independently documents that the checker itself
has had real correctness bugs (a fix reverted twice for misreporting phase-1
splits as leaks), and the catalog's own `tbl-check-oracle-transient-false-
negative` property documents a live, currently-present transient false-negative
window. `recovery-sk-rebuild-desync`'s evidence goes further and shows the
checker returning `true` while real PK/SK divergence is present — i.e., the
oracle can miss the exact class of bug several of these nine properties are
built to catch, not just occasionally cry wolf.

The catalog is aware of this (Cluster 8's "suggested triage practice": check
the oracle's own findings before treating any Cluster 1-7 violation as
confirmed). But awareness is framed entirely as a *triage* aid ("was the
oracle in its known-flaky window before you believe the failure"), never as a
*coverage* risk — the possibility that a future regression in
`orioledb_tbl_check()`/`check_walk_btree()` (which the codebase's own revert
history says is plausible) would silently zero out confidence in nine
properties at once, with no independent signal to notice it happened. None of
the nine properties, nor the oracle-integrity cluster, proposes an
independent secondary check (e.g., a periodic full content-level comparison of
PK-scan values against SK-index-scan values, not just token/row counts, or a
cross-check against `verify_orioledb()`'s independent implementation when it
exists) for even a sample of runs.

**Suggested action:** treat oracle hardening as a prerequisite deliverable, not
a 1-of-38 property with equal weight to everything else — e.g., wire a
periodic content-level cross-check (not just counts) into at least the
Cluster 1 workloads, and track `orioledb_tbl_check()`/`verify_orioledb()`'s own
diff history as an ongoing signal (a regression there should raise a flag
independent of whatever SUT property it happens to be serving that day).

---

## Finding: a recurring "unbounded busy-wait with no `CHECK_FOR_INTERRUPTS()`" pattern is scattered across three categories/clusters as four separate properties, and every one of them proposes only container-level fault injection when a cheaper SQL-level primitive tests the same mechanism more precisely

`sk-fixup-sentinel-spin-livelock` (checkpoint.c:1023-1041), `recovery-worker-
idxbuild-stall` (workers waiting on a stalled index-build leader),
`recovery-worker-stall-blocks-leader` (`workers_synchronize()` — see the
headline finding above, now confirmed real), and `checkpointer-startup-lock-
drain-progress` (`acquire_chkp_lock_drain()`) are four different call sites,
but the exact same shape of bug: a polling loop that either lacks
`CHECK_FOR_INTERRUPTS()` entirely, or has no outer give-up bound. The
relationship doc notes the symmetry between the first two recovery-worker
properties explicitly, but the clustering is organized by *code path /
shared oracle*, so this shape recurs across three different clusters
(Cluster 1, Cluster 3, Cluster 4) without ever being named as one cross-
cutting pattern. That's a real gap in the "map for prioritization" the
relationships doc is supposed to be: a reader triaging by cluster would build
four separate bespoke workloads without ever being told these four share one
underlying finding ("this codebase's wait loops are inconsistent about
`CHECK_FOR_INTERRUPTS()`, and the inconsistency clusters specifically around
checkpoint/recovery/checkpointer coordination") that could inform one cheaper,
generic test primitive: freeze/kill one specific named process during its
documented wait window, and confirm both (a) a wall-clock/iteration bound and
(b) that `SIGTERM`/query-cancel is actually honored.

Separately, and more concretely actionable: every one of these four
properties' "Antithesis Angle" reaches for container/process-level fault
injection (`SIGSTOP`, CPU throttling, scheduling delay) to test whether the
wait loop is *interruptible*. But interruptibility in Postgres is a
first-class SQL-level concept — `pg_cancel_backend()`, `pg_terminate_backend()`,
`statement_timeout` — that exercises `CHECK_FOR_INTERRUPTS()` far more directly
and cheaply than trying to freeze a process from outside and observe whether
it eventually resumes. None of the four properties mentions this as an option,
even though it's strictly simpler to construct (no new fault-injection
primitive needed, just an ordinary SQL call from a workload client) and tests
the mechanism the bug is actually about (does this loop ever call
`CHECK_FOR_INTERRUPTS()`) more directly than inferring it from outside via
timing.

**Suggested action:** add a cross-cutting note (a ninth cluster, or a callout
in Cluster 8) naming this pattern explicitly, and add "target this wait loop
with `pg_cancel_backend()`/`pg_terminate_backend()` against the specific
backend/process PID, not just container-level freeze" to all four properties'
Antithesis Angle sections — cheaper to build and more diagnostic than the
process-freeze approach alone.

---

## Finding: `pg_upgrade` (cross-major upgrade support) is a substantial, actively-developed, in-scope feature with zero representation anywhere in the research — and it directly touches machinery the catalog already calls high-blast-radius

Three remote branches (`origin/pg_upgrade`, `origin/upgrade_actions`,
`origin/nickb/pg_upgrade_test`) implement "cross-major `pg_upgrade` of
OrioleDB clusters" — the lead commit (`63e7fdc1`, "Support cross-major
pg_upgrade of OrioleDB clusters") plus follow-on fixes total ~1,550 lines
across `src/catalog/o_tables.c`, `o_indices.c`, `ddl.c`, `sys_trees.c`, and
`src/checkpoint/control.c` (72 changed lines) — i.e., it modifies exactly the
checkpoint-control-file version/CRC/gate logic that
`checkpoint-control-version-gate-fails-safe` already calls "the single
authoritative persistence-boundary record... the highest-blast-radius binary-
format contract in the codebase." There is also a dedicated CI matrix
(`eb4c3c73`, "add cross-major pg_upgrade test matrix for OrioleDB") and a
history of cross-major recovery crashes being found and fixed
(`147ad4b8`, `87039a9b`) — meaning pg_upgrade is not a hypothetical future
feature but an already-nontrivially-buggy one under active stabilization,
right now, in the same repository.

This is squarely in scope — it is neither rewind nor S3 — yet it appears
nowhere in `sut-analysis.md`'s "Version Compatibility" focus (§2, Cluster 6),
which covers WAL/page/binary-format version contracts but never pg_upgrade or
`ALTER EXTENSION ... UPDATE` (the `sql/*_prod.sql` upgrade scripts that
`CLAUDE.md` itself describes as a first-class build artifact). No test in
`test/sql`, `test/t`, or `test/specs` references `ALTER EXTENSION` at all
(confirmed via grep). This is exactly the "SUT analysis missed something that
changes what properties matter" angle the task asked me to look for: an
entire upgrade pathway, under active development, touching the catalog's own
named highest-blast-radius contract, invisible to 11 parallel discovery
passes because none of them appear to have swept branch names/recent commit
titles for "upgrade" the way they swept for bug-fix keywords like "leak" or
"livelock."

**Suggested action:** flag to whoever owns SUT-analysis maintenance: once (or
if) `pg_upgrade` support lands on `main`, add a property along the lines of
"cross-major `pg_upgrade` never corrupts checkpoint-control-file version
gating, and a crash mid-upgrade fails safe rather than leaving a
partially-converted catalog" — this would be a new, legitimately high-value
addition, not a revision of an existing property, since nothing today tests
the upgrade path at all.

---

## Finding: a named liveness risk from `sut-analysis.md` (bgwriter single point of responsibility) was dropped between SUT analysis and the property catalog with no visible reasoning

`sut-analysis.md` §5 states, specifically: "bgwriter undo-location advancement
for replica cleanup has a single named point of responsibility
(`BGWriterNum == 0`, `src/workers/bgwriter.c:200-214`) — if that specific
worker stalls, crashes, or is disabled (`orioledb.debug_disable_bgwriter`),
the claimed replica-cleanup liveness may not hold." This is precisely the
shape of finding the catalog otherwise loves (a single named point of
responsibility, a documented liveness claim, a GUC that can disable the very
mechanism) — the same shape as `recovery-worker-stall-blocks-leader` or
`checkpointer-startup-lock-drain-progress`. It never became a property. It
isn't in "loosely connected / standalone properties" either. The only trace
of it left in the catalog is a passing aside inside
`multi-insert-undo-capacity-invariant`'s Antithesis Angle text ("whether
background workers not counted in `max_procs`... can also hold same-type
undo reservations"), which is a different question (undo-buffer sizing, not
bgwriter liveness). This looks like a finding that fell through the
deduplication/synthesis step (47 evidence files → 38 properties) without a
recorded reason, unlike every other dropped/corrected claim in this catalog,
which gets an explicit note (e.g., the #876/#889 corrections, the "16 of 19"
framing). Given the catalog's own "Honest Summaries" ethos (referenced by
`chaos-driver-skips-check-on-fault-landing`), a silently dropped finding is
worth flagging even without evidence it was wrong to drop.

**Suggested action:** either add it back as a property (it fits Cluster 2/4's
shape and topology needs), or note explicitly in the catalog why it was
excluded (e.g., "subsumed by X" or "judged too speculative because Y") so a
future reader doesn't have to rediscover the gap between the two documents.

---

## Finding: the undo-retention split on `enable_rewind` (`UNDO_REC_EXISTS`, `minProcRetainLocation` vs. `minRewindRetainLocation`) means several in-scope properties implicitly test only one of two retention regimes, and the catalog's text never says so

At least four in-scope properties depend on undo-retention correctness:
`sk-fixup-undo-recycling-drop` (explicitly keys off
`apply_one_pending_sk_fixup()`'s `UNDO_REC_EXISTS(...) == false` branch),
`replica-undo-reclaimed-too-early` (the `Assert(UNDO_REC_EXISTS(...))` sites in
`src/btree/page_contents.c`), `undo-wraparound-retry-cap`, and
`multi-insert-undo-capacity-invariant`. I checked the actual macro
(`include/transam/undo.h:356`):

```c
#define UNDO_REC_EXISTS(undoType, location) ((location) >= pg_atomic_read_u64(enable_rewind ? &get_undo_meta_by_type((undoType))->minRewindRetainLocation : &get_undo_meta_by_type((undoType))->minProcRetainLocation) || ...
```

— i.e., the exact retention floor these properties reason about literally
bifurcates on `enable_rewind`, and the same bifurcation recurs at every other
undo-retention call site (`undo.c:642,1764,2360,2378,2531`, `btree/undo.c:940,2097`,
`btree/insert.c:822`, `btree/merge.c:158`). Since `enable_rewind` is
`PGC_POSTMASTER` (confirmed: only settable at server start, not toggleable at
runtime by any session — so this is *not* a runtime-fragility risk, just a
static one), and the deployment topology/every harness config leaves it at its
`false` default, every one of these four properties is, as scoped,
*exclusively* exercising the `minProcRetainLocation` branch. That is a
legitimate and correct consequence of the user's rewind exclusion — not a bug
in the scoping decision. But none of the four evidence files or catalog
entries say this explicitly; they describe the invariant in terms that read
as unconditional ("the referenced undo location is never recycled out..."),
when what's actually being tested is one specific, GUC-selected code path
through a macro that has a structurally parallel twin the catalog will never
exercise. This is the "adjacent risk that isn't itself a rewind/S3 property"
the task asked about: not a gap in coverage of rewind itself, but an unstated
precondition baked into four in-scope properties, which matters if the two
branches of `UNDO_REC_EXISTS` ever diverge in a way specific to one side (a
bug only present in the `minProcRetainLocation` floor computation, or one
only present in `minRewindRetainLocation`) — the catalog's framing gives no
signal either way since it never names the branch it's actually testing.

**Suggested action:** add one sentence to each of the four properties' evidence
files: "this property, as scoped, only exercises the `enable_rewind=false`
(`minProcRetainLocation`) branch of the shared retention macro; the
`enable_rewind=true` branch is out of scope per the user's rewind exclusion
and untested by this property in either configuration." Cheap, and prevents a
false sense that the finding is retention-mechanism-agnostic when it isn't.

---

## Finding: at least 6 of the 38 catalog entries are not claims about OrioleDB's runtime behavior at all, and this is never surfaced as a portfolio-composition fact

The four explicit meta-properties (`tbl-check-oracle-transient-false-negative`,
`jepsen-verdict-not-sdk-visible`, `chaos-driver-skips-check-on-fault-landing`,
`core-postgres-hook-coverage-blind-spot`) are properties about the *harness's*
verification pipeline. `checkpoint-stats-view-pg-major-branch` is a regression
guard on a Python test-driver bug that already shipped and was already fixed
in this repo's own recent history (`f0c818c1`) — its own text says "A
driver-side regression guard, not a SUT-side fault-injection target." That's
six of thirty-eight (~16%) that are not testing OrioleDB at all, but testing
the test infrastructure around it. This isn't wrong to include — several are
genuinely valuable (see below) — but the catalog presents "38 canonical
properties" as one flat count without ever breaking out how many are
SUT-behavior claims vs. harness-integrity claims. A reader skimming for "how
much of OrioleDB does this cover" would over-count by roughly a sixth.

A sharper version of this same point: `core-postgres-hook-coverage-blind-spot`
doesn't actually have a pass/fail invariant at all — its own "Invariant"
section describes *adding instrumentation* ("explicit `reachable()` markers at
each orioledb-relevant hook call site"), not a claim that can be true or
false. And its own Open Questions section concedes that implementing its
proposed remedy requires patching the (explicitly out-of-scope)
`orioledb_postgres` source — meaning this property cannot be acted on by
whoever consumes this catalog without first crossing a boundary the user
explicitly closed. That combination (no testable invariant + remedy requires
out-of-scope repo access) makes it a weaker fit for "property catalog" than
for "a recommendation to file with whoever owns patched Postgres." I'd
reframe it out of the 38-count, or at minimum flag it distinctly rather than
list it as a peer of, say, `recovery-sk-rebuild-desync`.

**Suggested action:** in a future revision, split the catalog's summary count
into "SUT-behavior properties" (32) and "harness/process-integrity properties"
(6), and consider relocating `core-postgres-hook-coverage-blind-spot` to a
"recommendations for the patched-Postgres maintainers" appendix rather than
counting it as a checkable property.

---

## Finding: `checkpoint-control-version-gate-fails-safe` and `page-version-mismatch-fails-safe` are close enough to be one property, not two

Both are "verified-correct-today, but dormant and forward-looking only"
findings: both conclude the relevant gate correctly fails safe (`FATAL`) today
purely because only one version of the relevant format has ever existed, both
say the interesting bug (a broken *conversion* path) can't be tested until a
version bump actually happens, and both recommend the same remedy shape ("when
the version is next bumped, add a fixture with an old-version image and test
the conversion, ideally under crash-mid-conversion fault injection"). The
relationship doc notices the parallel shape ("Structurally similar... found
via the same... investigation") but stops short of asking whether they should
be merged. Given neither has any actionable Antithesis workload today, and
both share the exact same "add this test *later*, when a bump happens"
recommendation, keeping them as two separate 38-count catalog entries
overstates the portfolio's present-day actionable size for a marginal
organizational benefit (they do differ in file/layer, which is a fine reason
to keep as two *sections* within one property, but a thinner reason to count
them as two independent top-level properties).

**Suggested action:** low priority — consider merging into one
"version-bump conversion paths are structurally sound today but completely
untested for every one of the five version knobs in the codebase" property
with two named sub-targets, rather than two separate catalog rows.

---

## Finding: supported PG-major-version (16/17/18) is never treated as a fuzzed dimension for any of the 34 SUT-behavior properties, despite the harness's own history proving this dimension finds real bugs

`checkpoint-stats-view-pg-major-branch` exists *because* the chaos driver
itself shipped a real bug from not handling PG16-vs-PG17+ divergence
(`pg_stat_bgwriter` vs. `pg_stat_checkpointer`) — direct, concrete proof that
"does this behave the same across supported majors" is a real bug-finding
axis in this codebase's test surface. Yet none of the 34 SUT-behavior
properties propose running the same fault-injection workload across more than
one PG major as part of its own check — PG-major only appears in the catalog
as a property of the *test harness's own code*, never as a workload parameter
for testing OrioleDB's C code against hook-site or behavior differences
across majors (e.g., the patched-Postgres hook call sites in
`core-postgres-hook-coverage-blind-spot` are themselves per-major patches;
whether the same race window exists identically on PG16 and PG18 is
unaddressed anywhere).

**Suggested action:** lower priority than the other findings above (this is
more a "worth a line in Assumptions" than a new property), but worth a
one-line addition to the catalog's Open Questions: whether any single
property's workload should be run across all three supported majors rather
than whichever one the default Dockerfile picks, given the harness's own
proof that major-version divergence produces real, silent bugs when
untested.

---

## Passes (things checked and found correct)

- Spot-checked five of the catalog's `git merge-base --is-ancestor` claims
  (`9bc39d3b` for #876, `ef8e93b9`/`a0d628c1` for #889, `7d04814b`/`4f4c365a`
  for #865) directly — all confirmed ancestors of `a975c702` as claimed. The
  catalog's git-forensics methodology (used to correct `sut-analysis.md`'s
  stale "unfixed" framing) is sound and its specific conclusions check out.
- `enable_rewind` is `PGC_POSTMASTER` (confirmed by reading
  `DefineCustomBoolVariable` in `src/orioledb.c`) — not session-settable, so
  the undo-retention-branch finding above is a static scoping fact, not a
  runtime-fragility risk. Good news distinct from the finding itself.
- `wal-recovery-rejects-future-version` / `wal-decode-rejects-future-version`
  are a legitimate sibling pair (different consumers, same shared gate), not
  redundant — worth keeping as two entries, unlike the control/page-version
  pair flagged above.
- The Cluster 1 ↔ Cluster 8 cross-reference (oracle instability qualifying
  every property that reuses `orioledb_tbl_check()`) is explicit and
  well-reasoned as far as it goes; my finding above is about its blast-radius
  framing, not an error in the cross-reference itself.
- The catalog's explicit, repeated self-correction discipline (flagging its
  own claims as "needs human input," "not independently re-derived," "treat as
  a strong lead, not a confirmed fact") is applied consistently across nearly
  every property — a genuinely good practice that made this evaluation easier,
  since the catalog rarely overclaims certainty it hasn't earned.
- `test/antithesis/` and `doc/` were checked for any `ALTER EXTENSION`/
  `pg_upgrade` test coverage (none found) and for any documentation of a
  supported extension-upgrade workflow (none found) — consistent with the
  finding above that this is a genuine, currently-untested gap rather than
  something quietly covered elsewhere.

## Uncertainties

- I did not exhaustively sweep all ~90 remote branches for further
  already-fixed bugs — only a handful of the most-recently-committed ones in
  `recovery.c`/`checkpoint.c`/`transam/`-adjacent territory. The three bugs
  found above are very likely not the only ones a full sweep would surface;
  I stopped once the pattern was clearly established, in the interest of time,
  not because I ran out of leads.
- Whether `pg_upgrade` support is close enough to merging that it should
  actively reshape near-term prioritization (vs. being a "note for later") is
  a judgment call outside what git history alone can answer — would need
  human input on the team's actual roadmap.
- I could not determine how much the bgwriter single-point-of-responsibility
  finding's disappearance between `sut-analysis.md` and the catalog was a
  deliberate, reasoned exclusion vs. an accidental drop during the 47→38
  deduplication pass — no merge-log entry for it was found in any evidence
  file, but I did not read all 40 evidence files end-to-end looking for one.
- Whether Antithesis's coverage-guided search would actually behave
  differently if `core-postgres-hook-coverage-blind-spot`'s recommended
  `reachable()` markers were added is outside what static analysis can
  confirm — this is really an Implementability-lens question about the build
  pipeline, flagged here only because the property's *scope-crossing* remedy
  (requiring edits to the excluded `orioledb_postgres` repo) is what makes it
  odd as a catalog entry, independent of whether the underlying idea is good.
