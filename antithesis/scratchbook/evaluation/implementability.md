---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: in-repo documentation site, consulted as background if needed
---

# Implementability Evaluation: OrioleDB Property Catalog

Evaluation lens: **can each of the 38 cataloged properties actually be checked**,
given (a) the deployment topology as it exists today plus the one planned
addition (`test.orioledb-standby` + `standby-consistency-client`), (b) the
codebase's current instrumentation (zero SUT-side/C-level Antithesis
assertions anywhere, confirmed by `existing-assertions.md`), and (c) the
workload's ability to construct the necessary preconditions within a
plausible Antithesis run.

Ground truth independently re-verified against the repo at this commit
(not just taken from the catalog/evidence files) before writing this
evaluation:

- `test/antithesis/orioledb/Dockerfile`: core Postgres is built with
  `--enable-cassert` (assertions **are** compiled in); `orioledb.so` is built
  `IS_DEV=1`. **This resolves a catalog-wide open question**: `Assert()`-only
  invariants (globalXmin monotonicity, undo-reservation cap,
  `UNDO_REC_EXISTS`) are not no-ops in the images the harness actually
  builds — an `Assert()` firing crashes the process, which Antithesis detects
  generically even with no SDK assertion wired to it.
- `src/recovery/wal_reader.c:420-447`: confirmed the `IS_DEV` branch is
  symmetric ("Intentionally fail tests" `FATAL` fires for *both* the
  newer-version and the older-version case), while non-`IS_DEV` only FATALs
  on newer and *converts* on older. Since the harness Dockerfile always
  builds `IS_DEV=1`, this directly confirms `wal-older-version-seamless-conversion`'s
  claim that its subject code path is structurally unreachable in the
  current/planned harness.
- `src/orioledb.c`: `orioledb.recovery_pool_size` and
  `orioledb.recovery_idx_pool_size` both default to **3** (`PGC_POSTMASTER`),
  unmodified by any existing workload config. Parallel recovery workers and
  parallel index-build workers are therefore active during **ordinary
  single-node crash recovery today**, with no config change — this matters
  because several properties in the "Recovery Worker Concurrency" category
  were written as if they might need the (not-yet-built) standby topology;
  they don't.
- `test/antithesis/config/docker-compose.base.yaml` / `health-checker/main.go`:
  confirmed the `pg_isready` healthcheck gates `service_healthy`, and the
  Go health-checker itself performs zero I/O against Postgres (writes the
  `antithesis_setup` marker unconditionally on process start) — matches
  `readiness-gate-standby-recovery-lag.md`'s finding exactly.
- `stopevents.txt` (35 entries) confirmed exhaustively: no stopevent exists
  today for the sentinel spin (`WaitingSkUndoLoc`), `get_checkpoint_xlog_ptr()`,
  the parallel-recovery-worker apply loop, the overwrite-callback
  skip/apply branch, the commit-flow injection point
  (`orioledb-after-finish-wal-rec` does not exist as a stopevent name — it
  was only ever present on the unmerged branch's own scaffolding), the
  `WAL_REC_TRUNCATE`/`BRIDGE_ERASE`/`DATABASE_COPY` handlers, the undo
  wraparound retry, the multi-insert qsort fallback, the checkpointer
  lock-drain loop, or either recovery-worker stall path. This is the
  concrete evidence behind Finding 2 below.
- `src/orioledb.c:507-519`: `orioledb.serializable` confirmed `PGC_USERSET`.
  `test/t/checkpoint_split_base_test.py:113-120` confirmed verbatim
  (`# no errors, can be true or false` immediately followed by a second,
  later `CHECKPOINT` before asserting `true`).

---

## Findings

### Finding 1 (catalog-wide, high severity) — ~24% of the catalog is hard-blocked on the standby topology, which does not exist yet

**Properties affected:** `checkpoint-abort-snapshot-standby-panic`,
`recovery-finish-abort-livelock`, `replica-xmin-monotonicity`,
`replica-globalxmin-catchup-lag`, `replica-undo-reclaimed-too-early`,
`malformed-wal-container-double-finish`, and (with caveats below)
`checkpointer-startup-lock-drain-progress`, `checkpoint-recovery-lsn-sync-gap`,
`readiness-gate-standby-recovery-lag`.

**Concern:** `deployment-topology.md` proposes exactly one new component
(`test.orioledb-standby` + `standby-consistency-client`) and is explicit that
this is a real, currently-unmet prerequisite, not a hypothetical. I confirmed
this directly: `test/antithesis/` has no second Postgres service anywhere in
`config/setup/**` or `config/workload/**`, and no client container connects
to more than one Postgres host. None of these 9 properties can be exercised
at all until that topology is built — this is not a probabilistic
reachability concern like most other findings below, it's a hard
precondition.

Within this group, two nuances change the practical picture:

- `checkpoint-recovery-lsn-sync-gap` has a genuinely standby-free fallback
  explicitly given in its own evidence file: crash the instance mid-DML and
  check the PK/SK oracle immediately after the automatic
  `CHECKPOINT_END_OF_RECOVERY`. This variant is implementable **today**, on
  the existing single-node topology, with no new component — it just tests a
  narrower slice of the same FIXME (the leader-vs-worker gap at end-of-recovery
  specifically, not at an arbitrary mid-replication restartpoint). The
  standby-dependent variant (restartpoints on a lagging streaming replica)
  is strictly better at forcing the leader/worker gap wide, but isn't
  required to get some signal.
- `checkpointer-startup-lock-drain-progress`'s own root-cause comment says
  "on a hot standby," but the actual mechanism (startup process holding
  `oTablesMetaLock` SHARED while the checkpointer wants it EXCLUSIVE) is not
  intrinsically standby-specific — restartpoints can fire during **plain
  crash recovery** on a single primary too, if recovery takes long enough
  relative to `checkpoint_timeout`/`max_wal_size`. This is a weaker,
  harder-to-construct fallback (no in-flight client DML exists on the
  recovering node to keep churning `oTablesMetaLock`-relevant traffic, since
  no client connections are accepted until recovery finishes), so the
  standby variant is materially better here, but I would not call this one
  "impossible without a standby" the way the other 6 genuinely are.

**Evidence:** `deployment-topology.md`'s own component table; direct
`find`/`grep` over `test/antithesis/config/**` confirming no second Postgres
service exists; the six evidence files' own "Antithesis Angle" sections,
which uniformly open with "Requires a primary+streaming-standby topology" or
equivalent.

**Suggested action:** Treat standby-topology construction as the single
highest-leverage prerequisite, exactly as `deployment-topology.md` already
recommends — building it unlocks 6-9 properties at once, more than any other
single piece of harness work in this catalog.

### Finding 2 (catalog-wide, medium-high severity) — a large fraction of "Antithesis Angle" sections quietly assume new C-level instrumentation that does not exist, contradicting an implicit "just build a workload" framing

**Properties affected (instrumentation is load-bearing, not cosmetic):**
`sk-fixup-sentinel-spin-livelock`, `sk-overwrite-callback-identity-dedup`,
`recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-leader`,
`malformed-wal-container-double-finish`, `checkpoint-recovery-lsn-sync-gap`,
`recovery-finish-abort-livelock` (weaker without it).

**Properties affected (instrumentation is a nice-to-have; checkable today
via existing oracle/log/crash-detection, but with materially less precision
or a weaker vacuous-pass guard without it):** `sk-fixup-undo-recycling-drop`,
`undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant` (the
qsort-fallback-reachability half), `checkpointer-startup-lock-drain-progress`
(iteration-count bound), `o-sys-cache-invalidation-race`,
`core-postgres-hook-coverage-blind-spot`.

**Concern:** `existing-assertions.md` establishes, and I independently
re-confirmed by reading `stopevents.txt` in full (35 entries, none matching
any of the above), that **zero** SUT-side (C-level) Antithesis
instrumentation exists anywhere in `src/`/`include/` today. Every property in
the first list above proposes, as part of its own "Antithesis Angle" or
"SUT-side instrumentation" section, adding a **new** stopevent, counter, or
`reachable()`/`unreachable()` call inside OrioleDB C code — this is real,
non-trivial new development work in the SUT itself, sanctioned by CLAUDE.md
(`stopevents.txt` is a source input meant to be edited), but it means these
properties are not "write a Python driver against the existing surface"
tasks the way `sk-recovery-race`-style properties are. Concretely:

- `sk-fixup-sentinel-spin-livelock`'s core claim
  (`Sometimes(checkpoint_entered_sentinel_spin_wait)`) has **zero** external
  signal today — not even a `DEBUG` log line (contrast with the sibling
  `sk-fixup-undo-recycling-drop`, which at least has a `DEBUG2` log the
  workload could grep). Without a new stopevent or counter, a workload
  cannot confirm this specific wait was ever entered, only that checkpoint
  duration was long — a weak, easily-confounded proxy.
- `sk-overwrite-callback-identity-dedup`'s invariant
  (`existing_hash == incoming_hash` at the moment the dedup skip branch
  fires) is **definitionally** internal state — there is no SQL-observable
  proxy for "the overwrite callback chose to skip, and here is what it
  would have applied instead." Without new instrumentation this property
  cannot be checked at all, only its hypothetical downstream symptom (a
  wrong query result) could be checked, and the property's own open
  questions doubt that symptom is even reachable in practice.
- `recovery-worker-stall-blocks-leader`'s own evidence file states this
  directly: "the 'leader blocked on one worker' state is not
  distinguishable from 'recovery is just slow' via any externally-observable
  signal today... this is one of the properties where SUT-side
  instrumentation is closest to essential rather than merely nice-to-have."
- `malformed-wal-container-double-finish`'s real-world reproducer used a
  named injection point (`orioledb-after-finish-wal-rec`) that I confirmed
  does not exist as a stopevent on `main` — it lived only on the unmerged
  branch's own scaffolding. Reproducing this on the standby topology
  therefore needs a *new* stopevent at the exact commit-flow window between
  `add_finish_wal_record` and `flush_local_wal`, compounding with the
  standby-topology prerequisite from Finding 1.

**Suggested action:** When scoping implementation work, budget the
first-list properties as "new C instrumentation + workload," not just
"workload." Prioritize the second list's instrumentation as incremental
hardening once the underlying property is already partially checkable.

### Finding 3 (property-specific, high severity) — `wal-older-version-seamless-conversion` is unreachable in the current and planned harness, for a confirmed build-configuration reason, not merely a workload gap

**Property affected:** `wal-older-version-seamless-conversion`

**Concern:** I independently re-read `src/recovery/wal_reader.c:420-447` and
confirmed the property's own claim precisely: under `IS_DEV` (which
`test/antithesis/orioledb/Dockerfile:174-177` unconditionally builds with),
**both** directions of a WAL version mismatch hit
`elog(FATAL, ..."Intentionally fail tests")` — including the
lower-than-current case that non-`IS_DEV` builds instead convert seamlessly.
This means the one code path this property targets (the per-record
`>= ORIOLEDB_WAL_VERSION` conversion gates) can **never** be exercised by any
workload or fault-injection variety in the current harness, because the
build itself intercepts the scenario before the claim-under-test's code runs.
This is not fixed by the planned standby addition either — a standby uses
the identical image/build.

**Evidence:** Direct reading of `wal_reader.c` (quoted above) plus the
Dockerfile's `IS_DEV=1` build flags on both `orioledb.so` compile steps
(lines 174-177). Confirmed no test anywhere in `test/t`, `test/sql`,
`test/specs`, or `test/antithesis` references `ORIOLEDB_WAL_VERSION`/
`wal_version` (zero test coverage in any build mode, corroborating the
evidence file).

**Suggested action:** This property should be flagged to whoever owns the
harness build matrix as requiring either (a) a second, non-`IS_DEV` build
variant, or (b) a supported test-only override to force the lower-version
branch under `IS_DEV`. Absent either, this property should be marked
"blocked on build-matrix change" rather than left in the catalog looking
like an ordinary fault-injection target — as currently written it reads as
implementable-with-effort when it is actually implementable-with-zero-effort-
because-unreachable.

### Finding 4 (property-specific, high severity) — `core-postgres-hook-coverage-blind-spot`'s proposed remedy crosses the explicit scope boundary of this research pass, though a same-repo partial mitigation exists

**Property affected:** `core-postgres-hook-coverage-blind-spot`

**Concern:** The property's own suggested fix — adding explicit
`reachable()` markers at hook *call sites* (`xact_redo_hook()`'s invocation
inside `xact_redo()`, `CheckPoint_hook()`'s invocation inside
`CreateCheckPoint()`, etc.) — requires patching
`/Users/artur/supabase/orioledb_postgres`, the patched Postgres source. This
task's own scope restriction explicitly excludes that path from further
consultation or modification, and the evidence file itself acknowledges the
boundary-crossing ("this analysis was instructed not to consult further...
does not have write access to evaluate the cost of"). As written, this
property cannot be implemented within the current engagement's scope at all
— it would need either a scope re-opening or a different owner
(patched-Postgres maintainers).

However, there is a same-repo partial mitigation the evidence file does not
call out: the hook *implementations* (the OrioleDB-side functions the hooks
invoke, e.g. `orioledb_redo`'s handling of the `xact_redo_hook` call, or the
`CheckPoint_hook` target) live in `orioledb.so`, which already receives full
`-fsanitize-coverage=trace-pc-guard` instrumentation. An explicit
`reachable()` at the top of each hook *implementation* (no
`orioledb_postgres` edit needed) would confirm "this hook fired at all"
without crossing the scope boundary — it just can't give the finer
"hook-fired-relative-to-core-Postgres's-own-internal-state" correlation the
property's fuller framing wants, since that specific timing relationship is
only visible from the call-site side.

**Suggested action:** Split this property in two: a same-repo,
in-scope "hook reachability" version (implementable now, in orioledb.so),
and the original "call-site-relative-to-core-state" version explicitly
marked as requiring the excluded repo and a different owner.

### Finding 5 (property-specific, medium severity) — `o-sys-cache-invalidation-race`'s low evidence-confidence and low implementability are not orthogonal here; they share the same root cause

**Property affected:** `o-sys-cache-invalidation-race`

**Concern:** The task specifically asks whether low evidence-confidence
(explicitly flagged in the catalog as "speculative — lower confidence than
other properties") also implies low implementability, or whether the two are
independent axes. For this property they are **not** independent: the
evidence file's own honest assessment states the invariant "needs a
concrete, checkable formulation before it's implementable, which this pass
did not fully produce." The vagueness that lowers confidence in the
underlying mechanism (a plausible but unconfirmed staleness window in
`o_sys_cache_search()`'s fast-path pointer) is the *same* vagueness that
leaves no concrete SQL-observable signal proposed for what "stale" would
even look like to a workload — e.g., no candidate query, no candidate wrong
answer shape, no distinguishing marker between "invalidation hasn't been
delivered yet" (expected, benign, standard Postgres catcache-timing
behavior) and "the fast-path pointer specifically bypassed a delivered
invalidation" (the actual claimed bug). Until that formulation exists, there
is nothing for a workload or a SUT-side assertion to check — this is a
genuine implementability gap traceable directly to the same shallow-pass
confidence caveat, not a separate concern.

**Evidence:** `o-sys-cache-invalidation-race.md`'s own "Invariant" section
("not yet a fully concrete formulation") and "Honest Assessment of
Confidence" section.

**Suggested action:** Before investing workload effort, this property needs
a follow-up code-reading pass (reading `o_sys_cache_search()`'s full body,
per its own Open Questions) specifically to produce a concrete, falsifiable
invariant — not a workload-construction problem, a specification problem.

### Finding 6 (property-specific, medium severity) — `wal-decode-malformed-container-fails-safe` needs a *logical decoding* consumer, a topology addition distinct from (and not covered by) the planned physical streaming standby

**Property affected:** `wal-decode-malformed-container-fails-safe` (and,
more weakly, its siblings `wal-decode-rejects-future-version`, which shares
the same reachability caveat but is lower severity since it's a
verified-correct contract rather than a suspected gap)

**Concern:** `deployment-topology.md`'s one proposed addition is a
**physical** streaming standby (`pg_basebackup -R`). This property's
"Antithesis angle" requires "real WAL-page/segment bit-flip fault injection
targeted at a running logical-decoding slot," which needs a **logical**
replication consumer (a walsender in logical mode plus a subscriber/decoding
client) — a different feature surface entirely, not provided by adding a
physical standby. This is a topology gap the catalog itself flags ("would
require adding a logical replication consumer to the harness, which does not
exist today") but it's worth stating plainly: this is a *second*,
independent topology addition beyond the one `deployment-topology.md`
recommends, and building the physical standby does nothing to unlock this
property.

**Evidence:** `wal-decode-malformed-container-fails-safe.md`'s own
"Antithesis angle" section; confirmed no logical-replication consumer
appears anywhere in `test/antithesis/config/**`.

**Suggested action:** Track this as a distinct, lower-priority topology
request separate from the standby work — probably not worth building until
the standby-dependent properties (a larger cluster of higher-confidence
findings) are addressed first.

### Finding 7 (property-specific, low-medium severity) — two "version-gate" properties are structurally a one-off build-variant smoke test, not a fault-injection target, and don't fit Antithesis's continuous-fuzzing model

**Properties affected:** `checkpoint-control-version-gate-fails-safe`,
`page-version-mismatch-fails-safe`, and to a lesser extent
`orioledb-requires-preload-clean-failure`

**Concern:** All three evidence files say this themselves (e.g.
"structural/regression-guard property more than a fault-injection target...
the natural way to falsify it is a deliberate compatibility-break test," and
"a config-mutation-style property... rather than a runtime-fault-injection
property"). These are legitimate regression checks, but they don't fit the
usual Antithesis workload loop (sustained load + generic fault injection
across many runs) — they need a deliberately bumped version constant or a
deliberately mismatched config, checked once per relevant code change, more
like a CI smoke test than a continuously-run property. Antithesis can host
this (a workload container that starts, checks the FATAL/errdetail, and
exits), but it will produce a constant, uninteresting pass on every ordinary
run since nothing in normal operation ever varies the version constants —
worth flagging so it isn't miscounted as "N more properties actively
searched by fault injection" when scoping coverage.

**Evidence:** The three evidence files' own "Antithesis Angle" sections.

**Suggested action:** Implement these as a one-time build-variant smoke
test (or fold into the core extension's own `make installcheck`/CI matrix)
rather than a standing Antithesis workload container, to avoid diluting the
fault-injection budget on a property that fault injection cannot meaningfully
vary.

### Finding 8 (property-specific, low severity) — `checkpoint-stats-view-pg-major-branch` is not a SUT property; it's a CI-process recommendation, already fixed

**Property affected:** `checkpoint-stats-view-pg-major-branch`

**Concern:** The "property" is entirely about the workload driver's own
Python code (`checkpoint_count()` in `sk-recovery-race-chaos/driver.py`),
already fixed in this branch's own recent history (`f0c818c1`, confirmed
present). There is no OrioleDB C-level behavior under test at all; the
"Antithesis Angle" section itself says so ("driver-side regression guard
rather than something SUT-side fault injection interacts with directly").
This is a legitimate test-quality finding but doesn't belong alongside the
other 37 as an implementable SUT property — it's a "run this workload against
PG16/17/18 in CI" recommendation.

**Evidence:** `checkpoint-stats-view-pg-major-branch.md`'s own framing;
confirmed `f0c818c1` is the actual most-recent-but-one commit on this branch
and is present in the working tree's `driver.py`.

**Suggested action:** Keep as a documented process note for whoever
maintains the harness's own CI, not as a catalog property competing for
Antithesis run budget.

### Finding 9 (property-specific, low-medium severity) — `wal-recovery-rejects-future-version` and `wal-decode-rejects-future-version` are `AlwaysOrUnreachable` in a way likely to sit permanently unreached

**Properties affected:** `wal-recovery-rejects-future-version`,
`wal-decode-rejects-future-version`

**Concern:** Both are verified-correct-by-static-analysis contracts whose
only path to organic reachability is either a two-binary-version harness
(not planned) or a deliberate bit-flip of the version tag byte in an
in-flight WAL record. Antithesis's *generic* disk/memory fault injection
could in principle flip that exact byte, but the probability of landing
precisely on a 2-byte version field (as opposed to anywhere else in a much
larger WAL stream) during any given run is low, and there is no workload
lever to bias toward it (no coverage signal exists for "did a corruption
land on the version tag" specifically — this is a sub-case of Finding 2's
instrumentation gap, though a lower-priority one since these are
regression-guard-on-already-correct-code properties, not suspected bugs).
Practically these will report `Unreachable` for a long time unless someone
deliberately constructs the scenario.

**Evidence:** Both evidence files' own Open Questions ("no config in
`test/antithesis/` runs two different OrioleDB binary versions... likely to
sit at 'never reached' for a long time").

**Suggested action:** Lower priority than most of the catalog; acceptable to
leave as `AlwaysOrUnreachable` and revisit only if a version bump or a
dedicated fault-injection primitive targeting specific byte offsets becomes
available.

---

## Passes

Things checked directly and found sound (implementability-wise):

- **`sk-fixup-undo-recycling-drop`, `recovery-sk-rebuild-desync`,
  `recovery-worker-redispatch-consistency`, `non-modify-wal-record-replay-safety`,
  `pk-update-chain-race-consistency`, `serializable-table-lock-untested`,
  `disk-leaf-header-read-before-validation`, `tbl-check-oracle-transient-false-negative`,
  `jepsen-verdict-not-sdk-visible`, `chaos-driver-skips-check-on-fault-landing`**:
  all fully implementable on the **existing** single-node topology with
  **no** new SUT-side C instrumentation required — each reuses either the
  existing `orioledb_tbl_check()`/PK-SK-count oracle, an existing SQL
  function, or is a pure workload/driver-code change. These are the
  catalog's strongest implementability candidates and should be prioritized
  first, in parallel with standby-topology construction.
- **Assertions are compiled into the harness's build** (`--enable-cassert`
  confirmed in the Dockerfile): this resolves the catalog-wide open question
  favorably for `replica-xmin-monotonicity`, `multi-insert-undo-capacity-invariant`,
  and `replica-undo-reclaimed-too-early` — their `Assert()`-backed invariants
  are not silently compiled out; a violation crashes the process and
  Antithesis's generic crash detection will catch it even before any new
  SDK assertion is added (though a labeled `always()` at the same site would
  still give much better triage payload, per those properties' own
  suggestions).
- **`recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-leader`,
  `recovery-worker-redispatch-consistency`, `non-modify-wal-record-replay-safety`**:
  confirmed `orioledb.recovery_pool_size`/`orioledb.recovery_idx_pool_size`
  both default to 3, so parallel recovery/index-build workers are active
  during **ordinary single-node crash recovery** with zero config change —
  these do **not** require the standby topology, contrary to what a
  surface reading of "recovery worker" properties might suggest.
- **`checksums enabled by default and never overridden`**: confirmed
  `orioledb_checksums_enabled` defaults `true` and no harness config
  disables it, so `disk-leaf-header-read-before-validation` is reachable
  today without any config change.
- **`checkpointer-heavyweight-lock-deadlock`**: the core safety claim (no
  permanent hang) is externally observable via Postgres's standard
  `deadlock detected` log line and overall checkpoint/DDL completion —
  doesn't strictly need new SUT-side instrumentation for a first-pass
  implementation, contrary to how heavily its own evidence file emphasizes
  the missing `reachable()` call (useful for precision, not required for a
  baseline signal).
- **`serializable-error-mode-truncate-gap`**: correctly self-assessed by its
  own evidence file as low-value/low-priority; agree with its recommendation
  not to invest workload effort here given the `PGC_USERSET` framing.
- **`orioledb-requires-preload-clean-failure`**: implementable as a one-off
  config-mutation smoke test; correctly triaged as low priority by its own
  evidence file.

## Uncertainties

- **Whether Antithesis's fault-injection primitives can target a specific
  process among several cooperating ones** (e.g., killing/freezing
  specifically the index-build leader among recovery workers for
  `recovery-worker-idxbuild-stall`, or specifically the sentinel-holding
  backend for `sk-fixup-sentinel-spin-livelock`) — several properties assume
  this granularity is available ("Antithesis's process-kill fault primitive
  specifically, since it needs to selectively target one process among
  several cooperating ones"). I could not verify Antithesis's actual
  fault-injection targeting granularity from this repo alone; this is a
  platform-capability question outside the SUT/harness source, not
  something `grep`/code-reading can resolve.
- **Whether undo churn sufficient to recycle a checkpoint-time
  `pendingLoc` within a realistic Antithesis run duration is achievable** —
  `sk-fixup-undo-recycling-drop`'s own Open Questions flag this as
  unmeasured (would require reading the undo retention/horizon-advancement
  arithmetic in `src/transam/undo.c` in more depth than this pass covered).
  I could not resolve the numeric feasibility question either; this affects
  whether the property is practically reachable within Antithesis's
  timeline limits, distinct from whether the topology/instrumentation
  supports it in principle.
- **Whether a real double-wrap of the undo circular buffer
  (`undo-wraparound-retry-cap`) is combinatorially possible or only
  vanishingly rare** given the `max_procs * 2 * O_MAX_UNDO_RECORD_SIZE`
  sizing floor — this requires a probabilistic/combinatorial argument this
  pass (and the underlying evidence file) did not construct. Implementable
  as a workload (small buffer + low `max_procs` + concurrent writers), but
  whether it will ever actually fire is unresolved.
- **Whether `verify_orioledb()` shares `orioledb_tbl_check()`'s transient
  false-negative window** — I confirmed `verify_orioledb` exists in
  `src/tableam/func.c` at this commit (the deprecation stub is only on an
  unmerged branch), but did not read `check_btree()`'s full body to confirm
  whether the underlying transient-state condition is identical between the
  two entry points, as `tbl-check-oracle-transient-false-negative.md` itself
  flags as unresolved. This matters for how future-proof that property's
  implementation is if the harness migrates off the deprecated function.
- **Antithesis's disk-corruption fault-injection precision** — several
  findings (Finding 9, `disk-leaf-header-read-before-validation`) depend on
  whether Antithesis's disk-fault injection can be biased toward, or at
  least sometimes lands on, small structurally-significant byte ranges
  (a 2-byte WAL version tag, a page checksum field) versus being uniform
  over an entire block/file. This is a platform capability, not something
  visible from the SUT/harness source.
