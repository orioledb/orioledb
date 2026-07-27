---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: in-repo documentation site, consulted as background if needed
---

# Coverage Balance Evaluation: OrioleDB Property Catalog

## Method

Read `property-catalog.md` (38 properties / 10 categories) and
`sut-analysis.md` section by section (excluding rewind/S3/patched-Postgres,
which are out of scope per the catalog's own scope note), cross-referencing
every named risk area, bug-history lead, doc claim, and product-context item
against the catalog's 38 properties and their evidence files in
`properties/`. Also read `deployment-topology.md`, `existing-assertions.md`,
and `property-relationships.md` for topology/assertion-type context. Used
`grep` across `properties/*.md` to verify whether specific named risks (e.g.
`bgwriter`, `pgbackrest`, `ReindexConcurrentlySkipHook`, "committed oxid
reverts to in-progress") are actually referenced anywhere, rather than
relying on category titles alone.

---

## Findings

### Catalog-wide

**1. Resource-boundary / infrastructure-fault properties are thin relative to how sut-analysis frames them as Antithesis's unique value.**
Only 2 of 38 properties ("Undo Log and MVCC Resource Boundaries") directly
target resource-boundary risk, and both are scoped to the undo circular
buffer specifically (`multi-insert-undo-capacity-invariant`,
`undo-wraparound-retry-cap`). No property targets disk-space exhaustion
(ENOSPC during WAL write / checkpoint-control-file write /
`.map` free-extent persistence), backend/shared-memory OOM as a first-class
scenario, or connection/process-count exhaustion. OOM appears only as one of
several generic "any crash counts" fault triggers inside
`recovery-sk-rebuild-desync` and `malformed-wal-container-double-finish`
("Assert/OOM/PANIC/SIGKILL... all of which the doc cross-references"), not
as its own resource-boundary property. `sut-analysis.md` §9 explicitly names
"Infrastructure-level faults (OOM-killer, disk-slow-not-dead, CPU
starvation, network partition, clock skew)... this is squarely Antithesis's
unique value" as an existing-harness gap, and "Resource Boundaries" was one
of the 11 named discovery-agent focuses in the catalog's own Methodology
section — yet the synthesized catalog carries almost none of that focus's
output forward as resource-exhaustion-shaped properties (as distinct from
undo-capacity, which is really a concurrency/sizing-assumption property that
happens to live under a "Resource Boundaries" heading).
Scope: catalog-wide (portfolio gap).
Suggested action: add at least one property exercising OrioleDB's
control-file/WAL-write/checkpoint behavior under disk-full and one under
memory-pressure conditions, or explicitly document why these were judged
low-value (e.g., if Postgres's own crash-restart handles them generically
and no orioledb-specific state is at risk).

**2. Backup/restore under fault injection has zero properties, despite being explicitly named as a gap and a medium-high-impact product workflow.**
`sut-analysis.md` §9 lists "Backup/restore under fault injection... no
Antithesis counterpart" as a distinct existing-coverage gap, and §10 ranks
"Backup/restore and disaster recovery" as priority #3 by product impact,
specifically flagging that `pg_rewind` is documented as *not* incrementally
supported for OrioleDB (copies tables wholesale) and that
`orioledb.replay_until_lsn` "intentionally induces a split-brain state." No
property in the catalog addresses `pg_rewind`, `pgbackrest`, `wal-g`, or
`replay_until_lsn` under any kind of fault injection (confirmed via grep:
zero hits for "pgbackrest"/"wal-g"/"backup" across `properties/*.md`). This
is a different feature from the excluded orioledb SQL "rewind" function
(`src/rewind/`), so it is not covered by the scope restriction.
Scope: catalog-wide gap (no properties in this area at all).
Suggested action: at minimum, a property around `orioledb.replay_until_lsn`'s
self-described split-brain state (does the system behave exactly as
documented, or can the "last-resort" mechanism produce worse-than-documented
corruption under concurrent faults?) — it's explicitly named in `doc/` and
requires no new topology beyond what exists today.

**3. No property exercises a real, functioning logical-replication consumer under operational faults — only wire-format/version correctness of bytes reaching the decoder.**
`wal-decode-rejects-future-version`, `wal-decode-malformed-container-fails-safe`,
and `wal-older-version-seamless-conversion` all test whether logical decoding
handles version/corruption at the byte level, but none exercise an actual
subscriber/replication-slot lifecycle (crash-restart of the decoding backend,
slot invalidation, reconnect-and-resume) — several of these properties'
own "Antithesis Angle" sections admit "requires adding a logical-replication
consumer to the harness, which doesn't exist today." `sut-analysis.md` §9
separately lists "logical replication/decoding under faults" as an
uncovered gap, distinct from the format-correctness question.
Scope: catalog-wide gap, adjacent to but not fully closing sut-analysis §9 item 4.
Suggested action: track as dependent on adding a logical-replication
consumer to the harness (same dependency shape as the standby topology);
low priority until that infrastructure exists.

**4. Two SUT-analysis-named risks structurally similar to risks the catalog *did* cover got no property at all — an inconsistency in what got carried through discovery.**
- **bgwriter single-point-of-responsibility for replica undo cleanup**
  (`sut-analysis.md` §5: "`BGWriterNum == 0`... if that specific worker
  stalls, crashes, or is disabled (`orioledb.debug_disable_bgwriter`), the
  claimed replica-cleanup liveness may not hold"). The catalog built a
  symmetric *pair* of properties for the structurally analogous
  "one process's stall silently degrades a shared liveness guarantee"
  pattern in recovery workers (`recovery-worker-idxbuild-stall`,
  `recovery-worker-stall-blocks-leader` — explicitly framed in the catalog
  as "together these suggest the leader/worker protocol generally lacks
  wedged-peer detection in either direction"), but the same pattern for
  bgwriter — named just as explicitly in the SUT analysis — has no property.
  The only mention of bgwriter anywhere in `properties/` is a passing aside
  in `multi-insert-undo-capacity-invariant.md` about whether it's counted in
  `max_procs`, not a dedicated liveness property.
- **The cross-worker replay-visibility ordering claim**
  (`doc/architecture/overview.mdx:153`, cited in `sut-analysis.md` §4: "a
  transaction is visible to readers only once *all* recovery workers have
  completed all associated work"). The catalog systematically converts
  adjacent doc claims into properties elsewhere (e.g.
  `pk-update-chain-race-consistency` for the row-level-concurrency doc,
  the `wal-*-rejects-future-version` pair for the WAL-version doc claims),
  but this specific claim — which is exactly the shape of bug a
  multi-worker parallel-recovery architecture would produce (a reader
  seeing partially-replayed state) — has zero corresponding property or
  even an Open Question footnote anywhere in `properties/`.
- **`elog(PANIC, "failed to re-find shared proclock object")` (`oxid.c:1262`)**,
  one of several §11 "worth keeping enabled... so races surface as
  attributable failures" trip-wires. The other trip-wires in that same
  sentence all got carried forward in some form (the `MOVE DATABASE` race
  got a footnote in `non-modify-wal-record-replay-safety`'s Open Questions;
  the undo-wraparound trip-wire and the `CHECK_PAGE_STRUCT` broken-page
  trip-wire both have dedicated or closely-adjacent properties). This one
  PANIC site has zero references anywhere in `properties/` or the catalog.
Scope: 3 distinct, individually-named risks from sut-analysis §4/§5/§11.
Suggested action: add a bgwriter-stall liveness property (cheapest, matches
an existing template); at minimum footnote the visibility-ordering claim and
the proclock PANIC as Open Questions on the nearest existing property
(recovery-worker or checkpoint clusters) if not promoted to full properties.

**5. The "committed oxid reverts to in-progress" bug-doc lead named in `sut-analysis.md`'s own Assumptions section was not clearly picked up.**
`sut-analysis.md`'s Assumptions/Open-Questions section explicitly flags
`ISSUE_recovery_committed_oxid_reverts_to_inprogress.md` (on an unmerged
branch) as unread, "suggest[ing] additional undiscussed bugs." The catalog's
replication category addresses an oxid *stuck* in-progress forever
(`recovery-finish-abort-livelock`) and `globalXmin` *regressing*
(`replica-xmin-monotonicity`), but neither is the same failure direction as
a **committed** (already-resolved, already-visible) oxid reverting back to
appearing in-progress — which would be a stronger, visibility-breaking
regression (a previously-visible row's effects disappearing) rather than a
liveness stall. No property or evidence file references this specific
symptom or doc filename. This is exactly the kind of "quiet-but-complex,
didn't map cleanly to a single discovery focus" area the evaluation brief
asks to cross-check.
Scope: one named, unread lead from sut-analysis §8/Assumptions.
Suggested action: read `ISSUE_recovery_committed_oxid_reverts_to_inprogress.md`
directly (one `git show` away, same branch as the docs that produced
`checkpoint-abort-snapshot-standby-panic` and `recovery-finish-abort-livelock`)
before assuming it's subsumed by the two related properties above.

**6. "WAL Format and Version Compatibility" is the second-largest category (8/38, 21%) but has unusually low near-term actionability — a volume/payoff mismatch worth sequencing around.**
Of its 8 properties, 5 explicitly self-describe as not organically
reachable via Antithesis fault injection today: `wal-recovery-rejects-future-version`
and `wal-decode-rejects-future-version` need "a deliberately constructed
two-version-in-one-run harness config" that doesn't exist;
`wal-older-version-seamless-conversion` needs a non-`IS_DEV` build variant
that doesn't exist ("Recommend flagging to `antithesis-workload` as a
build-matrix gap"); `checkpoint-control-version-gate-fails-safe` is "best
falsified by a deliberate compatibility-break test... rather than organic
fault injection"; `page-version-mismatch-fails-safe` is explicitly "Not
organically testable today (single version in existence)... a forward-looking
process note." Only `wal-decode-malformed-container-fails-safe`,
`malformed-wal-container-double-finish`, and
`disk-leaf-header-read-before-validation` are actionable with today's
harness plus real fault injection (and the first two still need a
standby/logical-decoding consumer that doesn't exist yet). Compare to the
much smaller "Undo Log/MVCC Resource Boundaries" category (2 properties),
both fully actionable today with no additional infrastructure. This isn't
necessarily wrong — regression-guard and forward-looking value is real, and
`ORIOLEDB_BINARY_VERSION` correctness genuinely matters per `CLAUDE.md`'s
own binary-compatibility guidance — but 21% of the catalog's property count
sitting mostly in "verified-correct-by-reading, not yet reachable" status is
worth the team weighing against the zero-property areas above when
sequencing `antithesis-workload` implementation.
Scope: Category 6 (8 properties) vs. Category 5 (2 properties) and the
zero-property gaps in findings 1-4.
Suggested action: none required — flagging for prioritization awareness,
not a defect in the catalog itself.

**7. Roughly a third of the catalog cannot be exercised at all until the standby topology in `deployment-topology.md` is built — a single dependency the catalog is transparent about, but still a portfolio concentration risk.**
The entire "Streaming Replication / GlobalXmin Coordination" category (5),
plus `checkpoint-abort-snapshot-standby-panic`,
`checkpointer-startup-lock-drain-progress`, the standby-scoped half of
`checkpoint-recovery-lsn-sync-gap`, and `malformed-wal-container-double-finish`
— roughly 9-13 properties depending on how partial-dependencies are counted
— require a second Postgres node that does not exist in the harness today.
The catalog's own "Open Questions (catalog-wide)" section already flags this
("affects roughly a third of this catalog") and `deployment-topology.md`
correctly proposes it as the top-priority infrastructure addition. This is
listed as a finding for portfolio-balance completeness (a third of the
catalog's *value* is currently latent, not realized), not because the
catalog mishandled it — it is the single most self-aware section of the
whole document.
Scope: ~9-13 properties across 3 categories.
Suggested action: none beyond what `deployment-topology.md` already
recommends; noted here so the coverage-balance lens explicitly confirms the
dependency's size and that it's correctly prioritized rather than buried.

### Property-specific

No property-specific coverage-balance findings beyond what's captured above
— this lens evaluates portfolio shape, not individual property soundness,
and no single property showed a balance-relevant defect (e.g., silently
duplicating another property's coverage, or being miscategorized in a way
that hides a gap) beyond the category-level observations in findings 1-6.

---

## Passes

- **Checkpoint/recovery/replication concentration (~18-20 of 38 properties,
  roughly half the catalog) is proportionate to bug-density evidence, not
  over-investment.** `sut-analysis.md` §8 names `recovery.c` (159 commits),
  `checkpoint.c` (117), `undo.c` (112), `oxid.c` (67) as the hottest files
  and describes "a striking revert cluster" concentrated in exactly this
  area. Weighting the catalog this heavily toward it is the correct call,
  not a discovery-agent pile-on.
- **Compression/bridge-index corruption vector (`sut-analysis.md` §10 item
  6) is substantively covered, contrary to first appearance.**
  `sk-extent-leak-after-crash.md` explicitly merged with an independently-
  discovered `compressed-extent-leak-after-crash.md` finding and states it
  "directly corroborates `sut-analysis.md` §10's flagged 'plausible
  corruption vector' for compressed-tree free-space management, with much
  stronger... evidence than the original static-analysis lead." Initially
  looked like a possible gap by category title alone; reading the evidence
  file confirms it isn't.
- **Assertion-type balance is reasonable, not all-`Always`.** Counted across
  the 38 properties: `Always` (or `Always`-shaped) dominates as expected for
  a safety-heavy codebase, but `Sometimes` appears paired with `Always` in
  roughly 10 properties specifically to guard against vacuous passes (e.g.
  `recovery-finish-abort-livelock`, `checkpointer-heavyweight-lock-deadlock`,
  `recovery-worker-idxbuild-stall`), `Reachable` appears standalone in at
  least 2-3 properties as pure exploration guidance
  (`recovery-worker-stall-blocks-leader`, `core-postgres-hook-coverage-blind-spot`),
  and `Unreachable`/`AlwaysOrUnreachable` appear in ~6 properties for
  contracts expected to hold with no organic counterexample
  (`replica-undo-reclaimed-too-early`, `malformed-wal-container-double-finish`,
  the two `wal-*-rejects-future-version` properties,
  `pk-update-chain-race-consistency`). 5 properties are explicitly typed
  "Liveness." This is a genuinely mixed portfolio, not a safety-only catalog.
- **The "Test Oracle and Harness Integrity" category is a valuable, unusual
  addition that a shallower discovery pass would likely miss.**
  `tbl-check-oracle-transient-false-negative` directly targets
  `sut-analysis.md` §8's finding that `orioledb_tbl_check()` itself "has had
  correctness problems... twice-reverted fix history," and
  `jepsen-verdict-not-sdk-visible` / `chaos-driver-skips-check-on-fault-landing`
  catch that the two most important existing checks (jepsen's verdict, the
  chaos driver's post-burst assertion) can silently produce no signal on
  exactly the runs where a fault landed hardest. Without this category, every
  other property in the catalog would silently inherit these oracle-trust
  risks.
- **`verify_orioledb()`/`orioledb_tbl_check()`'s "doesn't block concurrent
  traffic nor false-fail during a concurrent checkpoint" product claim**
  (`sut-analysis.md` §10 item 7) **is directly covered**, not missed —
  `tbl-check-oracle-transient-false-negative` is exactly this test.
- **The isolation/serializability category correctly identifies the single
  largest completely-untested correctness mechanism.**
  `serializable-table-lock-untested` confirms via direct config grep that
  `orioledb.serializable=table_lock` (the default substitute for SSI) has
  literally never been exercised by any test or Antithesis config — a
  well-targeted, well-evidenced Reachability/Meta finding.
- **Evidence-file bookkeeping is accurate.** All 38 catalog properties have
  a corresponding file in `properties/` (verified by directory listing: 38
  files present, one-to-one with the catalog's slugs).

---

## Uncertainties

- **Whether `pg_upgrade` (major PostgreSQL version upgrade) is even a
  supported/applicable operational scenario for OrioleDB today.** No
  reference to `pg_upgrade` was found anywhere in `doc/`, so its absence
  from the catalog could reflect either a real gap (an operational
  lifecycle transition the catalog should cover) or a non-applicable
  scenario for a public-beta extension that may not support major-version
  upgrades at all. Could not resolve without asking the team or finding
  authoritative project guidance beyond `doc/`.
- **Whether the "committed oxid reverts to in-progress" symptom (finding 5)
  is actually distinct from `recovery-finish-abort-livelock` /
  `replica-xmin-monotonicity`, or effectively the same bug described from a
  different angle.** The underlying doc
  (`ISSUE_recovery_committed_oxid_reverts_to_inprogress.md`) was not read by
  this evaluation (it lives on an unmerged branch, and this pass — per its
  own scope — worked from the catalog and sut-analysis text rather than
  re-deriving root causes from unmerged-branch docs). `sut-analysis.md`
  itself only names the filename, without summarizing contents.
- **Severity/reachability of the resource-boundary gaps (finding 1).**
  Whether OrioleDB has any orioledb-specific error handling on ENOSPC/OOM
  paths worth targeting (as opposed to falling through to generic Postgres
  crash-restart behavior, which may already be adequately covered by
  existing crash-fault properties) would require source reading beyond this
  evaluation's coverage-balance lens. I can confirm the gap exists in the
  catalog and is named as a priority in `sut-analysis.md`, but not its true
  value/cost ratio.
- **`ReindexConcurrentlySkipHook`'s correctness risk.** Named once, in
  `core-postgres-hook-coverage-blind-spot.md`, as one of several hooks in a
  list — never traced to determine whether OrioleDB's opt-out from
  Postgres's concurrent-reindex protocol is actually safe or just untested.
  Could not judge severity without deeper code reading than this lens
  covers.
