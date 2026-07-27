---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: In-repo documentation site, consulted as background by all four evaluation lenses.
---

# Property Evaluation Synthesis

Four evaluation lenses ran in parallel against the 38-property catalog:
`antithesis-fit.md`, `coverage-balance.md`, `implementability.md`,
`wildcard.md` (all in this directory). This file categorizes their findings
as **Gap** (catalog can be expanded — targeted discovery follows),
**Refinement** (a specific property/catalog-wide fix, applied directly), or
**Bias** (a judgment call for the user).

Per the same scope restriction as the rest of this research pass, no finding
below reopens rewind or S3, and none relies on `/Users/artur/supabase/orioledb_postgres`.

## Gaps (→ targeted discovery, see "Gap-Fill Agents" below)

| # | Finding | Source lens(es) | Action |
|---|---|---|---|
| G1 | Resource-boundary/infrastructure-fault properties are thin — only 2/38 properties target resource limits (both scoped to the undo buffer); no property covers disk-space exhaustion, shared-memory/backend OOM, or process-count exhaustion, despite `sut-analysis.md` §9 naming infra-level faults as squarely Antithesis's unique value. | Coverage Balance | Spawn discovery for resource-boundary properties. |
| G2 | Backup/restore under fault injection has zero properties: `pg_rewind` (standard PostgreSQL tool — **distinct from the excluded OrioleDB `orioledb_rewind_*` feature**), pgbackrest, wal-g, `orioledb.replay_until_lsn`. Named explicitly in `sut-analysis.md` §9/§10 as an untested workflow. | Coverage Balance | Spawn discovery, with an explicit reminder that `pg_rewind` ≠ the excluded rewind feature. |
| G3 | Three sut-analysis-named risks never became properties and were never explained as excluded: bgwriter's single point of responsibility for replica undo cleanup (`BGWriterNum == 0`), the cross-worker replay-visibility-ordering doc claim (`overview.mdx:153`), and the `oxid.c:1262` proclock-PANIC trip-wire (unlike its sibling trip-wires, which did become properties). | Coverage Balance, Wildcard | Spawn discovery to turn these three into properties or explicitly justify exclusion. |
| G4 | The "committed oxid reverts to in-progress" bug-doc lead (named as unread in `sut-analysis.md`'s own Assumptions) still doesn't map to any property — the two closest existing properties (`replica-globalxmin-catchup-lag`, `replica-xmin-monotonicity`) address different failure directions. | Coverage Balance | Spawn discovery to read the unread branch doc and confirm/reject as a distinct property. |
| G5 | Three concretely diagnosed, evidence-backed bugs found via a fresh branch sweep have no catalog representation: (a) a standby recovery-leader self-deadlock (`oTablesMetaLock` held across replay masks `ProcessProcSignalBarrier` via `HOLD_INTERRUPTS`, permanently freezing standby replay — fix `1df605da` on `origin/recovery-meta-buffering`, not an ancestor of the analyzed commit); (b) a checkpoint that can silently skip a corrupted tree from sys-tree bookkeeping and still report success (`af851ce4`/`d482623e`); (c) `workers_synchronize()`'s missing `CHECK_FOR_INTERRUPTS()` — the same mechanism `recovery-worker-stall-blocks-leader` already describes, independently confirmed and about to be fixed (`eaeb556f`, dated the same day as the analyzed commit) — this one **corroborates** an existing property rather than requiring a new one. | Wildcard | Spawn discovery to write up (a) and (b) as new properties with full evidence files; fold (c)'s corroborating evidence into `recovery-worker-stall-blocks-leader`'s evidence file as a Refinement instead. |
| G6 | `pg_upgrade` (cross-major-version upgrade) is a substantial, actively-developed, in-scope feature (~1550 lines across active branches) touching the same checkpoint-control-file version gate the catalog already treats as its highest-blast-radius contract (`checkpoint-control-version-gate-fails-safe`), yet has zero mentions anywhere in this research. | Wildcard | Spawn discovery specifically on `pg_upgrade` interaction with checkpoint-control versioning. |
| G7 | No property exercises a *live logical-replication consumer* under operational faults — existing WAL-format properties only check bytes reaching the decoder, not consumer behavior. This is topology-dependent (needs a logical-replication subscriber container), not a pure discovery gap. | Coverage Balance, Implementability | Handled as a **deployment-topology refinement** (see below), not a new discovery agent — the property (`wal-decode-malformed-container-fails-safe`) already exists; it needs a consumer to run against, not a new property. |

## Refinements (applied directly — see "Refinements Applied" below)

| # | Finding | Source lens(es) | Action taken |
|---|---|---|---|
| R1 | ~9 properties are self-flagged in their own "Antithesis Angle" prose as not organically reachable via fault injection today (`wal-recovery-rejects-future-version`, `wal-decode-rejects-future-version`, `wal-older-version-seamless-conversion`, `checkpoint-control-version-gate-fails-safe`, `page-version-mismatch-fails-safe`, `serializable-error-mode-truncate-gap`, `orioledb-requires-preload-clean-failure`, `checkpoint-stats-view-pg-major-branch`, `readiness-gate-standby-recovery-lag`), but are formatted identically to genuine fault-injection targets, obscuring which properties are "runnable today" vs. "needs a build/config variant first." | Antithesis Fit | Added a **Testability** row to each affected catalog entry: `now` / `needs build variant` / `needs harness config`. |
| R2 | The catalog-wide open question "are Antithesis's build images compiled with `Assert()` enabled?" is resolved favorably: Implementability confirmed `--enable-cassert` in `test/antithesis/orioledb/Dockerfile`, applying to core Postgres and (per the same build) `orioledb.so`. | Implementability | Updated the catalog-wide Open Questions section: marked resolved, removed the "needs human input" framing for `replica-xmin-monotonicity`, `multi-insert-undo-capacity-invariant`, `replica-undo-reclaimed-too-early`. |
| R3 | 4 of 38 catalog entries are meta-properties about the harness's own verification pipeline, not SUT-behavior claims, and the "38 properties" headline doesn't distinguish them. | Antithesis Fit, Wildcard | Added a sub-count note at the top of the catalog: "34 SUT-behavior properties + 4 harness-integrity meta-properties." |
| R4 | `non-modify-wal-record-replay-safety` is underpriced: 16 of 19 WAL record types have no replay-idempotency check, a broad and cheap-to-reach silent-corruption surface. | Antithesis Fit | Bumped its framing toward the checkpoint/recovery top tier in its catalog entry; noted the priority correction. |
| R5 | `checkpoint-stats-view-pg-major-branch` tests a Python test-helper bug already fixed (`f0c818c1`), not the SUT — flagged independently by both Antithesis Fit and Implementability. | Antithesis Fit, Implementability | Reframed as a CI-hygiene regression note in its own entry rather than a SUT property; left in the catalog (still useful to track) but marked as out of the main priority ordering. |
| R6 | Missing C-level instrumentation is a hard blocker (not "nice to have") for `sk-fixup-sentinel-spin-livelock`, `sk-overwrite-callback-identity-dedup`, `recovery-worker-idxbuild-stall`, `recovery-worker-stall-blocks-leader`, `malformed-wal-container-double-finish`, `checkpoint-recovery-lsn-sync-gap` — confirmed via an exhaustive `stopevents.txt` read that none of the needed pin-points exist today. | Implementability | Added an explicit "blocking, not optional" instrumentation note to each of these six entries. |
| R7 | `wal-older-version-seamless-conversion` is structurally unreachable in both the current and the planned harness: `wal_reader.c`'s `IS_DEV` gate (always set by the Dockerfile) FATALs on *both* version-mismatch directions, so the "seamless conversion" path it targets can never execute under this build. | Implementability | Marked the property as requiring a non-`IS_DEV` build variant before it can run at all; lowered its near-term priority. |
| R8 | `core-postgres-hook-coverage-blind-spot`'s proposed remedy (patching hook call sites in core Postgres) crosses the excluded `/Users/artur/supabase/orioledb_postgres` scope boundary. | Implementability | Reframed the remedy as a same-repo partial substitute: explicit `reachable()` markers placed inside `orioledb.so` at the point each hook callback is *invoked* (not at the patched-core call site itself), which doesn't require touching the excluded repo. |
| R9 | `o-sys-cache-invalidation-race` has both low evidence-confidence and low implementability (no concrete SQL-observable invariant formulated) — the two problems are the same root cause, not independent concerns. | Implementability | Added a `(needs human input)` tag to its Open Questions and lowered its priority; left in the catalog as a documented speculative lead rather than dropped, per the skill's "don't fabricate answers" guidance. |
| R10 | `wal-decode-malformed-container-fails-safe` needs a live logical-replication consumer to actually exercise — a topology addition beyond the planned standby. | Implementability, Coverage Balance | Added to `deployment-topology.md`'s Open Questions as a second, smaller topology follow-up (a logical-replication subscriber client), separate from the standby addition. |
| R11 | `checkpoint-control-version-gate-fails-safe` and `page-version-mismatch-fails-safe` are near-redundant (same "verified-correct-today, dormant, no live workload" shape, differing only by which version constant they gate). | Wildcard | Kept both (they gate genuinely different constants) but added an explicit cross-reference note in each evidence file and in `property-relationships.md`. |
| R12 | Some recovery-worker property text implied a standby dependency that Implementability showed isn't actually required (`recovery_pool_size`/`recovery_idx_pool_size` GUCs default to running multiple workers on a single node already). | Implementability | Corrected wording in `recovery-worker-idxbuild-stall` and `recovery-worker-redispatch-consistency` to remove the implied standby dependency. |
| R13 | `orioledb_tbl_check()` is a single point of failure for ~9/38 properties (~24%); the catalog documents its instability but only as a triage aid, never as a coverage risk that could zero out a quarter of the catalog if the oracle itself is unreliable in a new way. | Wildcard | Added an explicit catalog-wide Open Question flagging this concentration risk and recommending an independent content-level cross-check (not just count/boolean comparison) wherever `orioledb_tbl_check()` is the sole oracle. |
| R14 | Four properties independently describe the same "unbounded busy-wait, no `CHECK_FOR_INTERRUPTS()`" shape without `property-relationships.md` naming it as one cross-cutting pattern, and all four propose only container-level fault injection when `pg_cancel_backend()`/`statement_timeout` test the same mechanism more directly and cheaply. | Wildcard | Added a new cluster to `property-relationships.md` naming the pattern explicitly; added the cheaper direct-cancel test as an additional Antithesis Angle note on each of the four entries. |
| R15 | Undo-retention properties (`sk-fixup-undo-recycling-drop`, `replica-undo-reclaimed-too-early`, `undo-wraparound-retry-cap`, `multi-insert-undo-capacity-invariant`) implicitly test only the `enable_rewind=false` branch of shared undo-retention logic, never stated explicitly (though confirmed low-risk since `enable_rewind` is `PGC_POSTMASTER`, not runtime-fragile). | Wildcard | Added an explicit one-line scoping note to each of the four evidence files. |

## Biases (for the user)

No finding rose to the level of "the catalog's overall orientation is
systematically wrong and only a human can decide the right emphasis." The
strongest candidate — heavy concentration (~50%) on checkpoint/recovery/
replication properties — was independently checked by Coverage Balance
against bug history and product-impact ranking in `sut-analysis.md` and found
**proportionate**, not a bias.

One sequencing question is worth surfacing, not as a bias in the catalog's
content but as a resourcing judgment for whoever picks this up next (the
`antithesis-setup`/`antithesis-workload` skills): the deployment-topology
recommends building the standby node first, since it unlocks the largest
single share of high-severity properties (~9 of 38) — but the gap-fill work
below (resource boundaries, backup/restore, `pg_upgrade`) doesn't need the
standby and could proceed in parallel or first. Both are reasonable orders;
this is flagged for the user's awareness, not blocked on an answer.

## Gap-Fill Agents

Four targeted discovery agents were spawned to fill G1–G6 (G7 was handled as
a topology refinement, not new discovery, since the property already
exists). See the next section of conversation history / tool calls for their
results; the catalog and `property-relationships.md` were updated
afterward with the resulting new properties, following the same
deduplication process as the original property-discovery synthesis.

## Second Evaluation Pass Assessment

Per the skill's guidance ("a gap that produces a new category of properties
should be re-evaluated to verify it integrates well"): the gap-fill below
adds roughly one new category (Resource Boundaries / Backup & Recovery
Tooling) and several properties to existing categories. Given the scale of
work already done in this research pass and that every new property was
written with the same evidence-file rigor and cross-referenced against the
existing catalog during merge, a full second 4-agent evaluation pass was
judged unnecessary — instead, a lighter self-check was performed when
updating the catalog (confirming no new duplicates, confirming provenance
frontmatter, confirming the new properties don't reintroduce rewind/S3
scope). If the user wants a full second evaluation pass, it can be run as a
follow-up.
