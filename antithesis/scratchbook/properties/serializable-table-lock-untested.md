---
slug: serializable-table-lock-untested
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# serializable-table-lock-untested

## Focus

Protocol Contracts, isolation-level "contract" angle (context item d): the
task specifically asked me to examine "the identified inconsistency between
the jepsen Dockerfile's default env and the actual workload config override"
(`sut-analysis.md` §4). Investigating that lead directly turned up a
**different, better-supported** finding, and the original lead does not hold
up as stated — recorded per `references/validating-claims.md` discipline
(investigate before promoting a claim to a property).

## Investigating the original lead

- `test/antithesis/jepsen/Dockerfile` bakes in defaults
  `JEPSEN_ISOLATION=serializable` and
  `JEPSEN_EXPECTED_CONSISTENCY_MODEL=serializable`.
- `test/antithesis/config/workload/jepsen-repeatable-read/compose.yaml:21-27`
  overrides **both** variables together, with an explicit comment:
  ```yaml
  # JEPSEN_ISOLATION                   sets connection transaction level
  # JEPSEN_EXPECTED_CONSISTENCY_MODEL  defaults to the isolation level
  # NB postgres "repeatable-read" is not Adya repeatable read
  # and instead guarantees snapshot isolation.
  # Ergo, we must specify the expected consistency model explicitly.
  JEPSEN_ISOLATION: repeatable-read
  JEPSEN_EXPECTED_CONSISTENCY_MODEL: snapshot-isolation
  ```
- **Conclusion: this is not a defect or an unresolved inconsistency.** The
  compose config deliberately and correctly overrides both the connection
  isolation level and the expected consistency model together, with a comment
  explaining precisely why (Postgres `REPEATABLE READ` != Adya repeatable
  read; it's snapshot isolation, so the checker must be told that explicitly
  rather than left at its default-follows-isolation behavior). The
  Dockerfile's baked-in defaults are simply unused fallback values for anyone
  running the `jepsen-client` image standalone outside this harness's compose
  file — they never take effect in the actual Antithesis workload path, since
  `docker-compose`'s `environment:` block always wins. The SUT analysis's
  characterization of this as an "unresolved naming/intent mismatch worth
  confirming with the team" does not survive reading the override site
  directly; downgrading it out of the catalog as a non-issue.

## The real gap found instead

Chasing the same isolation-mode territory the original lead pointed at (the
SUT analysis's closing parenthetical: "no config actually exercises
`orioledb.serializable=table_lock`/`error` mode today") led to a concrete,
well-documented, entirely-untested claim:

- `include/transam/oxid.h:105-181` — `orioledb.serializable` GUC
  (`OSerializableMode`, default `O_SERIALIZABLE_TABLE_LOCK`,
  `src/orioledb.c:104`). Because OrioleDB doesn't implement SSI/predicate
  locking, the **default** mode's documented mechanism for preventing
  serialization anomalies under `BEGIN ISOLATION LEVEL SERIALIZABLE` is a
  coarse heavyweight `ExclusiveLock` per touched relation
  (`o_serializable_lock_relation`, `oxid.h:172-181`): "two SERIALIZABLE
  transactions touching the same table block on each other... any
  non-SERIALIZABLE writer... blocks against an in-flight SERIALIZABLE xact...
  non-SERIALIZABLE readers are unaffected."
- This lock is wired into 8 tableam entry points in
  `src/tableam/handler.c`: `orioledb_index_fetch_begin` (146),
  `orioledb_tuple_insert` (506), `orioledb_tuple_insert_with_arbiter` (531),
  `orioledb_tuple_delete` (589), `orioledb_tuple_update` (687),
  `orioledb_tuple_lock` (790), `orioledb_beginscan` (1702), and
  `orioledb_multi_insert` (1879) — i.e., essentially every mutating and
  scan-entry callback. This is a real, deliberate, load-bearing correctness
  mechanism, not a stub.
- The other two modes, `O_SERIALIZABLE_ERROR` (reject any SERIALIZABLE txn
  with `ERRCODE_FEATURE_NOT_SUPPORTED`) and `O_SERIALIZABLE_REPEATABLE_READ`
  (silent downgrade to snapshot isolation, on the claim that OrioleDB's
  CSN-based snapshot already provides equivalent semantics — `oxid.h:138-151`)
  are alternate GUC settings, both opt-in (not the default).
- Cross-checked the entire `test/antithesis/` tree: `JEPSEN_ISOLATION` is
  always `repeatable-read` (never `serializable`) across every workload
  config found, and `orioledb.serializable` is never set anywhere in
  `test/antithesis/config/**`. **This means the harness never issues a single
  `SERIALIZABLE`-isolation transaction against OrioleDB, so none of the three
  `orioledb.serializable` modes — including the default,
  correctness-load-bearing `table_lock` mode — has ever been exercised by any
  Antithesis config.** This corroborates and sharpens the SUT-analysis
  parenthetical rather than just repeating it: it's not merely that the
  *table_lock/error* modes specifically are untested, it's that the
  *SERIALIZABLE isolation level itself* is never requested by any client in
  any existing workload.

## What goes wrong if this is violated

If `o_serializable_lock_relation`'s coverage across those 8 entry points were
incomplete (e.g., a mutating path added later that forgets to call it, or a
DDL/COPY/TRUNCATE path that bypasses the tableam callbacks this lock is
attached to), two SERIALIZABLE transactions could interleave without the
claimed table-level mutual exclusion, producing exactly the write-skew /
serialization anomaly that `orioledb.serializable=table_lock` exists to
prevent — silently, since Postgres itself has no SSI-based backstop for
OrioleDB tables to catch this (that's the whole reason this mechanism exists
instead of PG's normal `SERIALIZABLE` machinery). Given zero current test
coverage of this mode, such a regression would currently be invisible to both
the deterministic test suite and the Antithesis harness.

## Antithesis angle

This is directly actionable: add (or extend) a jepsen-style workload variant
with `JEPSEN_ISOLATION=serializable` /
`JEPSEN_EXPECTED_CONSISTENCY_MODEL=serializable` and
`orioledb.serializable=table_lock` set explicitly, and let jepsen's own
serializability checker (or, better, an explicit `always()` SDK assertion
wired to its verdict — see the existing gap noted in `existing-assertions.md`
regarding jepsen's `results.edn` not being wired to an SDK assertion at all)
verify no anomalies occur under concurrent Antithesis fault injection
(scheduling delays, disk stalls) targeting the lock-acquisition window at each
of the 8 entry points. A second, cheaper variant would target
`orioledb.serializable=error` and assert every SERIALIZABLE transaction
attempt is cleanly rejected (`Always` on `ERRCODE_FEATURE_NOT_SUPPORTED`,
never silently accepted).

## Open Questions

- Are there mutating access paths to OrioleDB tables that bypass all 8 of the
  listed tableam callbacks (e.g., a DDL/TRUNCATE/COPY FREEZE path, or the
  bridge/non-btree index paths mentioned in `sut-analysis.md` §10) and would
  therefore escape `o_serializable_lock_relation`'s coverage? Only the 8
  call sites in `handler.c` were enumerated in this pass via grep; whether
  that's the complete set of mutating entry points for OrioleDB tables was
  not exhaustively cross-checked against the full `orioledb_am_methods`
  vtable. `(partial: 8 callsites found and confirmed load-bearing; full
  vtable cross-check not done)`
- Does `O_SERIALIZABLE_REPEATABLE_READ`'s "already equivalent to REPEATABLE
  READ" claim (`oxid.h:140-149`) actually hold, or could leaving
  `XactIsoLevel` at `XACT_SERIALIZABLE` while OrioleDB internally treats it as
  repeatable-read create any observable difference from a genuine
  `REPEATABLE READ` transaction (e.g., via `pg_stat_activity` iso level
  reporting, or interaction with heap tables in the same transaction under
  real PG SSI, as the comment alludes to — "PG's SSI machinery... requires
  [XactIsoLevel] to stay consistent... for the lifetime of the xact")? Not
  traced further; flagged as lower priority than the `table_lock` gap since
  `repeatable_read` mode is opt-in, not default. `(needs follow-up if this
  mode is ever prioritized)`

### Investigation Log

#### Are there mutating access paths to OrioleDB tables that bypass all 8 tableam callbacks and escape `o_serializable_lock_relation`'s coverage?

- Examined: `src/tableam/handler.c` via grep for `o_serializable_lock_relation` call sites.
- Found: 8 call sites confirmed (`orioledb_index_fetch_begin`, `orioledb_tuple_insert`, `orioledb_tuple_insert_with_arbiter`, `orioledb_tuple_delete`, `orioledb_tuple_update`, `orioledb_tuple_lock`, `orioledb_beginscan`, `orioledb_multi_insert`), all load-bearing mutating/scan-entry callbacks.
- Not found: no exhaustive cross-check against the full `orioledb_am_methods` vtable; DDL/TRUNCATE/COPY FREEZE and bridge/non-btree index paths not individually verified for coverage.
- Conclusion: tagged `(partial: 8 callsites found and confirmed load-bearing; full vtable cross-check not done)`.
