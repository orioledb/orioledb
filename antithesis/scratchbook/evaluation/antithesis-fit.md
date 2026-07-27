---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: in-repo documentation site, consulted as background if needed
---

# Antithesis Fit Evaluation: OrioleDB Property Catalog

## Lens

For each of the 38 properties in `property-catalog.md`, the question asked is:
does testing this property require exploring a state space (timing,
interleaving, partial failure, combinatorial fault sequences) that a
deterministic, fixed-input test cannot cover? Properties that are fully
checkable with one fixed sequence of operations are flagged as
unit/integration-test territory rather than Antithesis territory, even if
they are legitimate things to test. The inverse is also checked: properties
where the catalog's priority framing may undersell how well-suited the
property actually is to fault-injection exploration.

The catalog's own four "Test Oracle and Harness Integrity" properties are
meta-properties about the harness's verification pipeline, not about
OrioleDB's runtime behavior. They are addressed in a dedicated section because
the lens's core question ("does this need Antithesis's fault injection to be
reached") does not map onto them cleanly — see below.

---

## Catalog-wide observations

### 1. The catalog already self-flags most of the weak-fit properties honestly

A notable, mostly-positive finding: for a large fraction of the properties
that turn out to be poor Antithesis fits, the catalog's own "Antithesis Angle"
field already says so, explicitly and in similar language to what this review
would otherwise conclude independently:

- `wal-older-version-seamless-conversion`: "Not reachable via workload/fault-injection variety alone."
- `checkpoint-control-version-gate-fails-safe`: "best falsified by a deliberate compatibility-break test... rather than organic fault injection."
- `page-version-mismatch-fails-safe`: "Not reachable today; this is a forward-looking process note rather than a workload to build now."
- `serializable-error-mode-truncate-gap`: "Not recommended for workload investment given the framing caveat below."
- `orioledb-requires-preload-clean-failure`: "a config-mutation-style property... rather than a runtime-fault-injection property."
- `checkpoint-stats-view-pg-major-branch`: "A driver-side regression guard, not a SUT-side fault-injection target."
- `readiness-gate-standby-recovery-lag`: "None possible today for the real concern" (recorded for future inheritance, not as an active workload).

This means the catalog is largely honest about scope-fit rather than
inflating every entry's Antithesis relevance. The main value this review adds
is (a) confirming those self-assessments are correct, (b) making the pattern
visible in aggregate (seven separate properties independently reach the same
conclusion, suggesting the catalog authors could have grouped/deprioritized
them as a class rather than presenting all 38 with uniform formatting weight),
and (c) flagging the smaller number of cases where the self-assessment is
*not* present or is arguably too generous.

**Suggested action:** Consider adding a lightweight "Antithesis-testable now /
later / not applicable" tag to each entry (the catalog has the information to
support this already, scattered across "Antithesis Angle" prose) so a reader
implementing the workload can immediately separate the ~7 build-matrix/unit-test
entries from the ~28 genuine fault-injection targets, rather than discovering
the split by reading every entry closely.

### 2. Assertion-type calibration is generally good; no astronomically-unlikely `Sometimes` found

Scanning every `Sometimes`/`Reachable` invariant in the catalog for the
specific failure mode the lens asks about (a probabilistic assertion that
would need luck no realistic run duration provides): none found. The catalog
consistently pairs a `Sometimes`/`Reachable` "did we actually get into the
interesting window" check with an `Always` correctness check on top of it
(`sk-fixup-sentinel-spin-livelock`, `recovery-finish-abort-livelock`,
`checkpointer-heavyweight-lock-deadlock`, `recovery-worker-idxbuild-stall`,
`checkpoint-recovery-lsn-sync-gap`), and where a workload shape is described,
it's a deliberately constructed scenario (specific table shape, specific
stopevent, specific fault target) rather than "run generic load and hope,"
which is the right pattern for keeping a `Sometimes` from being vacuous. This
is a genuine strength of the catalog, not just an absence of a problem.

### 3. Reliance on `Assert()`-only invariants is a cross-cutting risk to Antithesis Fit specifically, and the catalog's mitigation is inconsistent

Three properties (`replica-xmin-monotonicity`, `multi-insert-undo-capacity-invariant`,
`replica-undo-reclaimed-too-early`) rest on a C-level `Assert()` that is a
no-op in non-assert builds — the catalog flags this as a catalog-wide open
question ("Are Antithesis's target build images compiled with C-level
`Assert()` enabled?"). This bears directly on Antithesis Fit: if assertions
are compiled out, the *state space Antithesis explores* may be identical
whether or not the invariant is violated — the run simply produces no signal
either way, silently. `replica-xmin-monotonicity` mitigates this by proposing
an independent workload-side sample-and-compare check (`orioledb_get_xid_meta()`
polling) that doesn't depend on the `Assert()` firing at all — a good pattern.
`multi-insert-undo-capacity-invariant` and `replica-undo-reclaimed-too-early`
do not describe an equivalent independent check; their invariants as written
lean on the `Assert()`/TRAP itself as the signal. Given `core-postgres-hook-coverage-blind-spot`'s
confirmed finding that the Dockerfile builds core Postgres `--enable-cassert`
(so this specific risk is probably moot for these three — assertions likely
*are* enabled), this is a lower-urgency finding than the catalog's phrasing
implies, but the catalog's own open question doesn't mention the Dockerfile
evidence that would resolve it, which looks like a missed cross-reference.

**Suggested action:** Cross-reference `core-postgres-hook-coverage-blind-spot`'s
Dockerfile finding (`--enable-cassert` confirmed) into the catalog-wide open
question about `Assert()`-only invariants — it looks like independently-run
discovery passes didn't share this piece of evidence with each other.

---

## The four meta-properties: judged differently, as instructed

`tbl-check-oracle-transient-false-negative`, `jepsen-verdict-not-sdk-visible`,
`chaos-driver-skips-check-on-fault-landing`, and
`core-postgres-hook-coverage-blind-spot` are not properties of OrioleDB; they
are properties of the test harness's own verification pipeline. Applying the
lens's literal question ("does this need Antithesis's fault-injection state
space to be reached?") to each individually gives four different answers,
which is itself worth surfacing rather than scoring them as a block:

- **`tbl-check-oracle-transient-false-negative`** — explicitly does *not* need
  fault injection to reach ("Doesn't need fault injection to reach the
  interesting state... reachable by ordinary DML + automatic checkpoints").
  This is fully unit-testable: force an in-flight split via existing
  mechanisms, call the checker twice with no intervening checkpoint, assert
  agreement. Antithesis's actual marginal contribution here is different from
  the lens's usual framing — it's not "explores a space a fixed test can't
  reach," it's "Antithesis's own scheduling jitter makes this already-known
  transient state *more* likely to occur incidentally during unrelated chaos
  runs, contaminating triage of a different property." That is a real and
  valuable thing to test for, but it is not the kind of value this lens is
  built to detect, and the property would be better described as "verify this
  known deterministic edge case doesn't get misattributed during chaos runs"
  than as a state-space-exploration target.
- **`jepsen-verdict-not-sdk-visible`** — has no state-space dimension at all.
  It is a wiring/plumbing task (parse `results.edn`, call `always()`) with
  no timing or concurrency component; "explored vs. not" doesn't apply. This
  is best understood as an implementation TODO for `antithesis-workload`, not
  a property that benefits from being evaluated through an Antithesis-fit
  lens — there is nothing to fit or not fit, since no fault injection touches
  this code path at all (it runs once, at teardown, over a file already on
  disk).
- **`chaos-driver-skips-check-on-fault-landing`** — genuinely does need
  Antithesis's fault injection to occur "in the wild" for the failure mode
  described to matter in practice (a connection lost specifically because a
  real fault landed mid-burst, as opposed to a deliberately-simulated
  `psycopg2.OperationalError` in a unit test of the driver's exception
  handling). Both are possible verification strategies; a unit test of the
  driver's control flow would catch the code-level gap just as reliably and
  more cheaply than waiting for Antithesis to land a real fault there. The
  property conflates two different things: "does the driver's except-branch
  correctly skip the check" (unit-testable, deterministic) and "how often does
  this branch actually get hit during real chaos runs, and does it correlate
  with high-signal windows" (an Antithesis-run analytics question, not a
  pre-run test). The `sometimes()` marker recommended in the invariant serves
  the second framing well; the underlying code defect would be caught faster
  by the first.
- **`core-postgres-hook-coverage-blind-spot`** — is the clearest case of "not
  really a property" in the whole catalog. It has no pass/fail invariant; its
  own "Invariant" field describes an instrumentation strategy (add
  `reachable()` markers), not a claim to verify. It is a recommendation about
  improving the fuzzer's own exploration-guidance signal, most similar to a
  build/tooling ticket. It does not fit the Sometimes/Always/Reachable/
  Unreachable typology the rest of the catalog uses, and grading it against
  "is this in Antithesis's sweet spot" is a category error — it's not a thing
  Antithesis tests, it's a thing that changes what Antithesis can see while
  testing everything else. Its content is valuable, but it should probably
  be filed separately from "properties" (e.g., as an instrumentation/coverage
  recommendation document) rather than as catalog entry #38 alongside safety/
  liveness claims about the SUT.

**Suggested action:** Split these four out of the property catalog's main
numbered list into an explicitly separate "harness self-checks" section (the
catalog already groups them under one category heading, which helps, but they
are still formatted identically to SUT properties with Type/Invariant/
Antithesis-Angle/Why-It-Matters fields that don't quite fit three of the four
as-is). At minimum, `core-postgres-hook-coverage-blind-spot` should not carry
an "Invariant" field that isn't actually an invariant.

---

## Property-specific findings

### Weak fits: unit/integration-test territory misfiled as Antithesis workload targets

- **`wal-recovery-rejects-future-version`** and **`wal-decode-rejects-future-version`**
  — both explicitly require either a two-binary-version harness or direct
  bit-flip fault injection on one specific byte of one specific record's
  version tag to be reachable at all; both are typed `AlwaysOrUnreachable`
  and both are described as "verified correct today by tracing the full call
  chain" — i.e., the interesting question (is the code path correct) has
  already been answered by static analysis, and the residual value is purely
  regression-guard, best served by a deterministic fixture test (a hand-crafted
  WAL record with a bumped version byte) rather than by search/exploration.
  There is no concurrency or partial-failure dimension to either property at
  all. **Action:** recommend these become `test/t/*.py` regression tests, not
  Antithesis workload items; the catalog's own open questions ("no reachability
  path identified in the current harness") already point this way but the
  properties are still presented alongside genuinely fault-injection-shaped
  ones without a clear signal to a workload implementer that these two need a
  fundamentally different delivery mechanism (a crafted fixture, not a fault).

- **`checkpoint-control-version-gate-fails-safe`** — mostly the same
  diagnosis (a deliberate version-bump build test, not organic fault
  injection), correctly self-flagged by the catalog itself. One partial
  exception: the secondary finding about `ERROR` (CRC mismatch) vs. `FATAL`
  (version mismatch) severity asymmetry *does* have a genuine organic-fault-injection
  angle — a bit-flip corruption landing in the CRC-covered region (large) vs.
  the version-field region (small) is exactly the kind of thing Antithesis's
  disk-corruption primitive produces "for free," and the property's own open
  question ("would Antithesis's organic bit-flip disk corruption ever land
  specifically on the version fields... vs. the much larger CRC-protected
  remainder?") already asks the right question. So this property is a mixed
  bag: primarily unit-test territory, with one sub-question worth carrying
  into a fault-injection workload as an observational check rather than a
  dedicated test.

- **`readiness-gate-standby-recovery-lag`** — for the current (single-node)
  topology, the invariant is essentially a structural tautology proven by
  code-tracing (`pg_isready` succeeding structurally implies not-in-recovery,
  confirmed by reading `RmgrCleanup()`/`worker_wait_shutdown()`'s position in
  the startup sequence) rather than something that benefits from search. The
  property's honest framing ("Antithesis Angle: None possible today") already
  says this. Recorded correctly as a forward-looking note for whoever builds
  the standby topology, not miscategorized as an active workload target.

- **`recovery-worker-redispatch-consistency`** — the evidence file
  (`properties/recovery-worker-redispatch-consistency.md`) reaches a mostly
  reassuring conclusion by pure code reading (`GET_WORKER_ID(hash)` is a pure
  function of key + GUC; full-restart-on-crash model means no stale
  per-worker state survives). The one residual variable (does the postmaster
  re-read a possibly-edited `recovery_pool_size` GUC between a crash and
  restart) is a config-mutation question with a small, enumerable set of
  answers (yes/no) — it doesn't need combinatorial timing exploration to
  answer, just one deliberate experiment: change the GUC, crash, restart,
  check. The catalog already prices this correctly as "lower priority than
  the other properties in this batch," which matches this lens's assessment;
  flagged here mainly to confirm the low priority is well-founded, not to
  contest it.

- **`orioledb-requires-preload-clean-failure`** — the catalog's own framing
  ("a config-mutation-style property... rather than a runtime-fault-injection
  property," "Lower priority... protects a check that's already demonstrably
  present and working") is accurate and this review agrees. Worth noting
  explicitly that this property was reconstructed from code reading after the
  task's original documented-claim premise turned out not to exist in `doc/`
  (a validated absence, per the evidence file) — the property is legitimate to
  keep as a low-priority regression check, but its presence in the catalog is
  really "we looked for a claim, didn't find one, and are recording a
  defensively-coded check instead" rather than a targeted Antithesis lead.

- **`checkpoint-stats-view-pg-major-branch`** — this is the clearest instance
  in the whole catalog of a property that doesn't test the SUT at all; it
  tests whether a Python test-harness helper function has a hardcoded,
  version-specific SQL column reference. The evidence file confirms the bug
  was real, already fixed (`f0c818c1`), and the remaining risk is a CI-hygiene
  gap ("run this workload against PG16/17/18 before merging driver changes"),
  not something Antithesis's fault injection interacts with in any way.
  Recommend this be tracked as a CI-matrix action item for the harness repo,
  not as catalog entry alongside SUT correctness properties — its inclusion
  is defensible as documentation of a near-miss, but it doesn't belong in an
  "Antithesis property catalog" in the same sense as, say, `recovery-sk-rebuild-desync`.

- **`serializable-error-mode-truncate-gap`** — already correctly deprioritized
  by the catalog itself ("not recommended for workload investment"); the
  defect is reachable only via a single deliberate self-inconsistent GUC
  change mid-transaction, a fixed sequence with no adversarial timing
  component. Agrees with the catalog's own framing.

### Strong fits, correctly prioritized (Passes, no action needed)

- `recovery-sk-rebuild-desync` — already flagged by the catalog as highest
  priority; genuinely reproduces via faults unrelated to any specific
  stopevent (8-30% repro at four distinct commit-pipeline injection points),
  and critically, the existing structural oracle (`orioledb_tbl_check()`)
  does *not* catch it — only the count-comparison half does. This is close to
  a textbook example of Antithesis's value proposition: broad, generic fault
  injection surfacing a bug no deterministic test targeting a specific window
  would find.
- `undo-wraparound-retry-cap` — the catalog's own framing ("a pure
  single-process test cannot exercise this at all") is correct, and the
  guard being unconditionally compiled in (not `Assert()`-gated) makes it a
  legitimate target regardless of build-flag uncertainty elsewhere in the
  catalog. Good fit, correctly prioritized.
- `checkpointer-startup-lock-drain-progress` — the property's own framing
  explicitly distinguishes "does the loop terminate under chance timing"
  from "does it terminate under *adversarial*, sustained fault injection
  that keeps regenerating sync requests" — exactly the distinction this lens
  is meant to draw out, already drawn by the catalog itself.
- `multi-insert-undo-capacity-invariant`, `checkpointer-heavyweight-lock-deadlock`,
  `recovery-worker-idxbuild-stall`, `pk-update-chain-race-consistency`,
  `sk-fixup-undo-recycling-drop`, `sk-fixup-sentinel-spin-livelock`,
  `checkpoint-abort-snapshot-standby-panic`, `checkpoint-recovery-lsn-sync-gap`,
  `replica-undo-reclaimed-too-early` — all describe genuine
  concurrency/timing/partial-failure state spaces with deliberately
  constructed (not purely lucky) workload shapes. No concerns.
- `disk-leaf-header-read-before-validation` — a concrete, already-confirmed
  ordering bug reachable via Antithesis's standard disk-bit-flip fault
  primitive with checksums on by default and never overridden in any harness
  config — essentially free coverage once the primitive fires during an
  active scan. Good, low-effort fit.

### Underestimated by the catalog (inverse check)

- **`non-modify-wal-record-replay-safety`** — the catalog frames this as "a
  scope gap / lead, not a confirmed bug" without assigning it strong
  priority language, but the underlying claim is broad: 16 of 19 WAL record
  types have never been checked for replay-idempotency to the depth the
  row-modify path has, and *every* crash-restart during recovery
  automatically reprocesses all of them from the last checkpoint's
  `replayStartPtr` — meaning ordinary repeated-crash fault injection already
  exercises this surface "for free" once a `TRUNCATE`/bridge-rebuild/
  database-move DDL happens to be in flight, no special construction needed.
  This combines a wide, cheap-to-reach surface with a silent-corruption
  failure class (wrong data applied, not a loud error) — exactly the
  profile the catalog itself says is worst-case for this project (§10 of
  `sut-analysis.md`). Given how little dedicated engineering the workload
  needs (repeated crashes during ordinary DDL-mixed load, which the harness's
  existing chaos infrastructure already approximates), this looks
  under-prioritized relative to its potential yield. **Suggested action:**
  consider elevating this closer to the checkpoint/recovery category's
  top-tier findings rather than leaving it framed as a lower-confidence
  "lead."
- **`serializable-table-lock-untested`** — the catalog already flags this as
  high-value (zero coverage anywhere, including deterministic isolation
  tests, on the default and only real substitute for SSI on this storage
  engine), so this is more a confirmation than a new finding: this review
  agrees the catalog's own priority framing undersells it slightly less than
  it might first appear, since the deterministic-test gap and the
  Antithesis-fit gap are both real and additive here — closing the basic
  coverage gap (does the lock actually get taken at all 8 entry points) is
  arguably an isolation-test task first, with Antithesis's marginal
  contribution being combinatorial interleaving across those 8 entry points
  under concurrent load and fault injection, which is a real and separate
  value-add worth keeping in the catalog rather than folding into a
  deterministic test alone.
- **`wal-decode-malformed-container-fails-safe`** — reasonably good fit
  (real bit-flip corruption of a running logical-decoding stream is squarely
  Antithesis's disk/memory fault-injection strength), but its dependency on
  adding a logical-replication consumer to the harness (which doesn't exist)
  means it currently reads as lower-urgency than its actual fit warrants once
  that consumer exists. Not mis-prioritized exactly, but worth flagging that
  once logical decoding is added to the harness for any reason, this property
  should move up the queue rather than being an afterthought.

---

## Passes (checked, no concerns)

- Assertion-type calibration (`Sometimes` always paired with a correctness
  `Always`, deliberately-constructed workload shapes rather than "load and
  hope") is consistently good across the catalog. No astronomically-unlikely
  timing scenarios found.
- The catalog's own self-assessment of Antithesis-fit is largely accurate
  where present (see catalog-wide finding #1) — spot-checked against evidence
  files for `checkpoint-stats-view-pg-major-branch`,
  `orioledb-requires-preload-clean-failure`, and
  `recovery-worker-redispatch-consistency`; all three evidence files support
  the catalog summary's framing.
- `recovery-sk-rebuild-desync`, `undo-wraparound-retry-cap`,
  `checkpointer-startup-lock-drain-progress` are all strong, well-matched
  Antithesis targets with no changes suggested.
- The existing harness's assertion inventory (`existing-assertions.md`)
  confirms the two live driver files use `Sometimes`/`Reachable`/`Always`
  appropriately for the one property they currently cover (orioledb#855);
  no calibration issue found in what's already implemented.

## Uncertainties

- Whether Antithesis's target build images have C-level `Assert()` enabled
  is only partially resolved: `core-postgres-hook-coverage-blind-spot`'s
  Dockerfile reading confirms `--enable-cassert` for **core Postgres**, but
  this review did not independently verify whether `orioledb.so` itself is
  built with assertions in the same image (the Makefile/build flags for the
  extension build step weren't re-traced here) — the three properties
  (`replica-xmin-monotonicity`, `multi-insert-undo-capacity-invariant`,
  `replica-undo-reclaimed-too-early`) whose core check is an `Assert()` inside
  orioledb's own C code (not core Postgres) still carry residual risk if the
  extension itself is built without cassert even though core Postgres is.
- Whether the "singleton driver invoked repeatedly vs. once per container
  lifetime" question (relevant to `chaos-driver-skips-check-on-fault-landing`'s
  severity assessment) has been resolved — the evidence file marks this
  explicitly unconfirmed, and it affects how much weight to put on that
  meta-property's real-world impact.
- Whether the team has any CI process running `sk-recovery-race-chaos`
  against all three PG majors (relevant to whether
  `checkpoint-stats-view-pg-major-branch` needs any further action beyond
  what's already fixed) — not found in `.github/workflows/`, flagged as
  needing human input by the evidence file itself, not independently
  resolved by this review.
