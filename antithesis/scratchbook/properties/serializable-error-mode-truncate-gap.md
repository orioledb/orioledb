# serializable-error-mode-truncate-gap

## Attention focus

Security Boundaries, specifically the assigned question: "is the `orioledb.serializable`/`error`
mode's `ERRCODE_FEATURE_NOT_SUPPORTED` rejection path enforced consistently, or could a client
construct a sequence of operations that bypasses the configured isolation-mode restriction?"

## Important framing caveat, stated up front

`orioledb.serializable` is `PGC_USERSET` (`src/orioledb.c:507-519`) — **any session can set it for
itself at any time**, including mid-transaction (`SET orioledb.serializable = 'table_lock'`).
Because of this, the GUC is not a privilege/authorization boundary in the usual sense: it isn't
something a DBA imposes on an untrusted role that the role cannot itself override. Any role that
could exploit an enforcement gap in `error` mode could instead simply `SET orioledb.serializable =
'table_lock'` for itself and get the exact same effective behavior legitimately. This substantially
lowers the security relevance of any inconsistency found here relative to the other two properties
in this batch (which involve functions with no such self-service opt-out). This property is
recorded because the assigned focus explicitly asked the question and a genuine code-level
inconsistency was found during investigation — not because it constitutes a privilege escalation.

## What was found

`o_check_isolation_level()` (`include/transam/oxid.h:120-152`) is the function that raises
`ERRCODE_FEATURE_NOT_SUPPORTED` when `orioledb.serializable = error` and `XactIsoLevel ==
XACT_SERIALIZABLE`. It is invoked, directly or via the `O_LOAD_SNAPSHOT`/`O_LOAD_SNAPSHOT_CSN`
macros or `fill_current_oxid_osnapshot()`, from every DML entry point checked: sequential scan
begin (`src/tableam/scan.c:612,666`), row-version fetch and index-scan tuple fetch
(`src/tableam/handler.c:247,321`, `src/indexam/handler.c:1953`), `INSERT`/`INSERT...ON CONFLICT`
(`src/tableam/handler.c:511`, `src/tableam/operations.c:1102`), `UPDATE`/`DELETE`
(`src/tableam/handler.c:595,692`), batched multi-insert (`src/tableam/handler.c:1882`), table
creation (`src/catalog/ddl.c:2149`), and ordinary `TRUNCATE` of a pre-existing table — which in
Postgres goes through the `relation_set_new_filenode` tableAM callback
(`src/tableam/handler.c:872-...`, checked call at line 952).

One path does **not** call it: `orioledb_relation_nontransactional_truncate()`
(`src/tableam/handler.c:1030-1050`) — the tableAM callback Postgres core uses for `TRUNCATE` when
the target table's storage was created in the *current* transaction/subtransaction (confirmed in
`/Users/artur/supabase/orioledb_postgres/src/backend/commands/tablecmds.c:2141-2145`:
`rel->rd_createSubid == mySubid || rel->rd_newRelfilelocatorSubid == mySubid`). This function calls
`o_truncate_table()` and `drop_indices_for_rel()` directly with no `o_serializable_lock_relation()`
or isolation-level check anywhere in it.

## Why this is likely not independently exploitable (validated, not assumed)

For `orioledb_relation_nontransactional_truncate()` to be reached, the table must have been created
(or last given a new relfilenode) earlier in the *same* transaction. But `CREATE TABLE ... USING
orioledb` itself calls `fill_current_oxid_osnapshot()` — the checked variant — at
`src/catalog/ddl.c:2149`, during table registration. So under `orioledb.serializable = error`, a
`SERIALIZABLE` transaction doing `CREATE TABLE t (...) USING orioledb; TRUNCATE t;` would already
raise `ERRCODE_FEATURE_NOT_SUPPORTED` at the `CREATE TABLE` step, before ever reaching the
unchecked `TRUNCATE` path — so this specific sequence doesn't survive to demonstrate a bypass.

The one sequence that *does* survive: because the GUC is `PGC_USERSET` and takes effect immediately
within a transaction, a session could `CREATE TABLE` while `orioledb.serializable = table_lock`
(no error), then issue `SET orioledb.serializable = 'error'` in the same transaction, then
`TRUNCATE` the just-created table — the `TRUNCATE` would silently succeed with no error, even
though the session's *current* setting at the time of the `TRUNCATE` is `error`. This is a genuine,
reachable inconsistency in the letter of the "reject SERIALIZABLE" contract, but per the framing
caveat above, it grants the session nothing it couldn't already get by leaving the GUC at
`table_lock`/`repeatable_read` the whole time.

## Why not already covered

`sut-analysis.md` §4 records the `orioledb.serializable` modes as a claimed guarantee to test but
does not examine enforcement-path completeness across all tableAM callbacks. No existing test or
Antithesis config exercises `orioledb.serializable = error` at all (§4: "no config actually
exercises `orioledb.serializable=table_lock`/`error` mode today").

## Antithesis angle

Low priority relative to the other two properties in this batch. If a future workload does exercise
`orioledb.serializable = error`, a `Reachable`/consistency check on
"`TRUNCATE` of a same-transaction-created OrioleDB table under `error` mode never raises
`ERRCODE_FEATURE_NOT_SUPPORTED`, in contrast to every other DML path" would confirm the gap
empirically rather than by static reasoning alone. Not recommending workload investment here given
the framing caveat.

## Open Questions

- Is there any other, more consequential way to reach `orioledb_relation_nontransactional_truncate()`
  without the GUC already having been `table_lock`/`repeatable_read` earlier in the same
  transaction (e.g. via a stored procedure, prepared transaction, or extension boundary that changes
  `XactIsoLevel` after table creation)? Not investigated further — the scenario found already
  requires deliberate self-inconsistency by the same session, which doesn't change privilege, so
  further investigation was judged low-value given the `PGC_USERSET` caveat. `(needs human input)`
  if the team wants this pursued further as a strict-mode correctness bug independent of security
  framing.
- Should `orioledb.serializable` be reclassified as `PGC_SUSET` if it's ever intended to function as
  an actual cluster-wide policy rather than a per-session preference? Docs (`doc/architecture/...`)
  frame it purely as a behavior selector, not a security control, so current behavior may be
  working as designed. `(needs human input)`

## Investigation Log

#### Is the `error`-mode rejection enforced on every OrioleDB tableAM/indexAM entry point reachable under SERIALIZABLE?

- Examined: all call sites of `O_LOAD_SNAPSHOT`, `O_LOAD_SNAPSHOT_CSN`, `o_check_isolation_level`,
  `fill_current_oxid_osnapshot` (checked variant) vs. `fill_current_oxid_osnapshot_no_check` across
  `src/tableam/handler.c`, `src/tableam/operations.c`, `src/indexam/handler.c`,
  `src/catalog/ddl.c`, `src/tuple/toast.c`.
- Found: consistent coverage across scan/insert/update/delete/multi-insert/table-creation paths.
  One gap: `orioledb_relation_nontransactional_truncate()` has no check at all.
- Found (reachability): the gap requires the target table to have been created/given new storage
  earlier in the same transaction; `CREATE TABLE` itself is checked, so the gap is only reachable if
  the GUC value changes between `CREATE TABLE` and `TRUNCATE` within one transaction (`PGC_USERSET`
  makes this possible for any session, for itself).
- Not found: any evidence this GUC is meant to function as an administrator-imposed restriction on
  other roles rather than a session's own preference.
- Conclusion: real code-level inconsistency exists, but its security relevance is low because the
  GUC is fully self-service (`PGC_USERSET`) — documented above as a framing caveat rather than
  suppressed, per the discipline of not hiding uncertainty in the prose.
