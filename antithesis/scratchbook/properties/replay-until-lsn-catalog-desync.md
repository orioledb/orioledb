# replay-until-lsn-catalog-desync

## Focus

Backup/restore under fault injection — the `orioledb.replay_until_lsn` angle
named explicitly in the evaluation gap ("Backup/restore under fault
injection has zero properties: `pg_rewind`, pgbackrest, wal-g,
`orioledb.replay_until_lsn`"). Per the task instructions, I read the actual
code (`src/recovery/recovery.c`, `src/orioledb.c`'s GUC registration) rather
than trusting `sut-analysis.md` §10's summary ("a last-resort disaster
recovery mechanism that intentionally induces a split-brain state") as
given, and traced precisely *what* diverges from *what*, not just that "a
split-brain" occurs.

## What led to this, and what I validated

`doc/usage/configuration.mdx:355-370` documents the GUC:

> Specifies the cutoff LSN at or after which OrioleDB will permanently stop
> applying its WAL records during recovery... This is a last-resort disaster
> recovery mechanism strictly comparable to `pg_resetwal`. It intentionally
> induces a split-brain state: PostgreSQL core catalogs and standard heap
> tables will continue replaying WAL, while OrioleDB tables remain at the
> specified LSN.

**Validated mechanism (not just the doc's summary), traced directly in code:**

1. **The GUC is `PGC_POSTMASTER`** (`src/orioledb.c:1132-1141`,
   `DefineCustomStringVariable(... PGC_POSTMASTER ...)`) — settable only via
   config file before server start, requiring a restart to take effect; not
   a runtime-mutable knob. Its check/assign hooks
   (`orioledb_replay_until_lsn_check_hook`/`_assign_hook`,
   `src/orioledb.c:442-466`) only parse an LSN string into the global
   `replay_until_lsn` (`include/orioledb.h:490`); no other side effect at
   startup time.

2. **The cutoff logic lives entirely inside `orioledb_redo()`**
   (`src/recovery/recovery.c:1157-1220`), which is OrioleDB's registered
   `rm_redo` callback for its own resource manager (`ORIOLEDB_RMGR_ID`,
   registered `src/orioledb.c:396-409` per `sut-analysis.md` §1). The logic
   is a simple state machine scoped to the Startup process's lifetime
   (`static bool needs_init`, `is_stop_lsn_active`, `skip_all_future_records`):
   on the first `ORIOLEDB_XLOG_CONTAINER` record read at or after
   `replay_until_lsn`, it emits a `WARNING` ("OrioleDB recovery has reached
   LSN... All future OrioleDB transactions will not be replayed"), sets
   `skip_all_future_records = true`, and `return`s — every subsequent call to
   `orioledb_redo()` for the remainder of the Startup process's life becomes
   a no-op fast path (`recovery.c:1174-1181`) that does not even inspect the
   record.
3. **This is scoped to OrioleDB's own resource manager only — it is not a
   general recovery-target/replay-stop mechanism.** `orioledb_redo()` is the
   redo callback core Postgres's rmgr dispatch table invokes *specifically*
   for `ORIOLEDB_XLOG_CONTAINER` records; it has no effect on `StartupXLOG`'s
   main replay loop, does not set `recoveryTargetLSN`/`recoveryTarget`, and
   does not touch any other rmgr's dispatch. Every other resource manager
   (`heap`, `xact`, `btree` for any non-OrioleDB index, `standby`, etc.)
   keeps calling its own unmodified redo function for every WAL record up to
   the real end of WAL (or an actual `recovery_target_*`, if separately
   configured) — confirmed by the absence of any interaction between this
   code path and `xlog.c`/recovery-target machinery anywhere in this repo.
   **This precisely confirms the doc's claim at the mechanism level**: the
   "split" is not a metaphor for "the database looks weird afterward" — it
   is a literal per-resource-manager fork in what gets replayed, with
   PostgreSQL's own catalog/heap redo continuing unmodified past the point
   where OrioleDB's own redo stops.

## The sharper consequence the doc's wording doesn't spell out

OrioleDB's own DDL/table-metadata bookkeeping (`o_tables`/`o_indices`, the
"non-transactional catalog cache" `sut-analysis.md` §1 describes,
`src/catalog/o_tables.c`) is itself **internal OrioleDB state, persisted and
replayed via the same `ORIOLEDB_XLOG_CONTAINER` WAL records as ordinary row
data** — not stored via `pg_class`/`pg_attribute` heap rows directly (those
are core Postgres's own catalog view of the relation, kept in sync with, but
structurally separate from, OrioleDB's own table descriptor). This means the
divergence `replay_until_lsn` induces is not simply "heap tables advance,
OrioleDB row data freezes" — it is a **two-tier split**: any DDL committed
between the cutoff LSN and the actual end of WAL updates `pg_class`/
`pg_attribute` (heap, keeps advancing) while leaving OrioleDB's own
`o_tables`/`o_indices` descriptor for that relation, and the relation's
on-disk B-tree structure itself, frozen at the old (pre-DDL) shape. A
concrete example: an `ALTER TABLE ... ADD COLUMN` committed after the
cutoff would be visible in `pg_attribute` (correct new column) while the
underlying OrioleDB table descriptor and B-tree layout would still reflect
the pre-`ALTER` shape.

## What goes wrong if the property is violated (or rather: what is currently unknown)

Whether this two-tier divergence **fails safe** (OrioleDB detects the
pg_class-vs-o_tables mismatch on next table access and cleanly `ERROR`s) or
**fails silently** (a stale/mismatched tuple descriptor is used against the
frozen B-tree, corrupting reads/writes or crashing) is not resolved by this
pass — `src/catalog/o_tables.c`'s `o_tables_get()` and the table-open path in
`src/tableam/handler.c` were only spot-checked, not fully traced for a
version/generation cross-check against `pg_class`. This is exactly the
uncertainty this property should resolve, and it matters a great deal: a
clean `ERROR` on the affected table (with everything else in the database
usable) is a tolerable outcome for a documented last-resort mechanism; a
crash or silent wrong-schema read is not, and the doc's `:::warning[Data
consistency]::: ` block does not distinguish between these outcomes.

## Why this is a real gap, not a duplicate of anything in the existing catalog

Grepped `test/t/`, `test/sql/`, `test/specs/`, `test/antithesis/`, and
`test/integration/` for `replay_until_lsn`: **zero hits anywhere.** This
GUC has no test coverage at all, matching `sut-analysis.md` §12's note that
it is "unused anywhere in `test/antithesis/`" — confirmed here to be unused
anywhere in the *entire* test suite, not just the Antithesis harness. No
property in the existing 38-entry catalog touches it (confirmed via a
full read of `property-catalog.md`).

## The property

| | |
|---|---|
| **Type** | Safety |
| **Property** | After `orioledb.replay_until_lsn` triggers (recovery reaches or passes the configured LSN and permanently stops applying OrioleDB WAL records for the remainder of that Startup process's life), any subsequent access to an OrioleDB table whose DDL (`pg_class`/`pg_attribute`) was modified by WAL between the cutoff LSN and the actual end of replayed WAL either (a) cleanly `ERROR`s with a diagnosable message identifying the OrioleDB/catalog descriptor mismatch, or (b) is fully and correctly usable if no such DDL occurred — it never silently uses a mismatched tuple descriptor against the frozen on-disk B-tree, and it never crashes the backend/instance. |
| **Invariant** | `AlwaysOrUnreachable(post_cutoff_ddl_table_access_fails_safe_or_is_absent)`: construct a scenario where DDL (`ALTER TABLE ADD/DROP COLUMN`, `CREATE INDEX`) on an OrioleDB table is committed strictly after the intended `replay_until_lsn` cutoff, then start the instance with that GUC set to the pre-DDL LSN and query the affected table — assert no crash/PANIC and, if an error is raised, that it is a clean, attributable `ERROR` rather than a generic assertion failure or wrong-result read. Pair with `Reachable(replay_until_lsn_cutoff_warning_logged)` to confirm the cutoff path itself was actually exercised (it's a `WARNING`-only side effect, easy to silently miss). |
| **Antithesis Angle** | Fully constructible in the *existing* single-node topology — no replication/standby needed, unlike most of this catalog's highest-priority gaps. Requires: sustained DML + DDL against OrioleDB tables, an unclean shutdown or `pg_ctl stop -m immediate` at a known WAL position, then a restart with `orioledb.replay_until_lsn` set to an earlier LSN (deliberately chosen to land before some committed DDL) — a config-mutation-style property (per `orioledb-requires-preload-clean-failure`'s similar shape) rather than a live fault-injection target, since the GUC is `PGC_POSTMASTER`-only. A secondary, more organic Antithesis angle: since the mechanism is entirely LSN-comparison-based and takes any config-supplied LSN, deliberately setting a cutoff computed from a live run's actual WAL position (rather than a value picked by the test author) plus randomized DDL timing lets Antithesis explore *which* DDL a given cutoff does or doesn't include, without needing new topology. |
| **Why It Matters** | This is a **self-disclosed, admittedly dangerous, last-resort mechanism** (the doc's own `:::warning[Data consistency]:::` block) with zero test coverage anywhere in the repository — the same "docs pre-confess a risk but nothing tests exactly what happens" pattern the catalog's `rewind` scope-exclusion already flagged for the (out-of-scope) `orioledb_rewind_*` feature, but `replay_until_lsn` is explicitly **in scope** per this task and named directly in the evaluation gap. Per `sut-analysis.md` §10, this sits squarely in the "medium-high impact, concentrated at incident-response time" backup/restore-and-disaster-recovery workflow category — exactly the moment an operator invoking a "last-resort" GUC can least afford an undiagnosed second failure mode layered on top of whatever incident prompted its use. |

**Open Questions:**

- Does `o_tables_get()`/the table-open path in `src/tableam/handler.c` cross-
  validate OrioleDB's own table descriptor against `pg_class`/`pg_attribute`
  (e.g. a version/generation number, a relfilenode/oid consistency check)
  before using it, such that a post-cutoff DDL mismatch would be caught
  cleanly — or is there no such check, making a silent tupdesc mismatch the
  likely outcome? `(needs further code reading in src/catalog/o_tables.c and
  src/tableam/handler.c — only spot-checked this pass, not fully traced)`
- Does the mechanism apply per-database or cluster-wide — i.e., if the
  disaster prompting use of `replay_until_lsn` only affected one database's
  OrioleDB tables, does the cutoff still freeze *every* database's OrioleDB
  state cluster-wide (since the GUC and the Startup process are both
  cluster-level, not per-database)? Not traced — the redo function's state
  machine (`needs_init`/`is_stop_lsn_active`/`skip_all_future_records`) has
  no visible per-database scoping in the code read this pass.
- After the cutoff triggers and the instance opens for read-write (once
  recovery otherwise completes), can new writes to the *same* OrioleDB table
  whose earlier post-cutoff DDL was skipped succeed at all, or would every
  subsequent write to that table hit whatever failure mode the first open
  question above resolves? Not investigated — depends on the answer above.
- Is there any operator-facing tooling (a query, a log summary) that lists
  *which* tables/objects were affected by a given cutoff, so an operator
  invoking this "last resort" mechanism has any way to know what to check
  afterward — or is the single `WARNING` at first-record-skipped the entire
  operator-visible signal? Not found in `doc/` beyond the configuration
  reference already cited.

### Investigation Log

#### Does replay_until_lsn stop all of WAL recovery, or only OrioleDB's own resource manager's redo?

- Examined: `src/recovery/recovery.c:1157-1220` (`orioledb_redo`, the sole
  function containing any `replay_until_lsn` logic); `src/orioledb.c:396-409`
  (rmgr registration) and `:1132-1141` (GUC registration); grepped the whole
  repo for `replay_until_lsn` (7 call sites total, all confined to these two
  files) and for any interaction with `recoveryTargetLSN`/`recovery_target`
  (none found).
- Found: the cutoff state machine lives entirely inside the OrioleDB-specific
  `rm_redo` callback and only ever causes that one callback to short-circuit
  on records it would have processed; nothing here touches core Postgres's
  main replay loop, its own recovery-target machinery, or any other resource
  manager's redo dispatch.
- Conclusion: the doc's "PostgreSQL core catalogs and standard heap tables
  will continue replaying WAL" claim is confirmed precisely at the mechanism
  level — validated as a genuine per-resource-manager fork, not merely
  asserted from the doc text.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** `existing-assertions.md` confirms zero assertions exist outside
`sk-recovery-race[-chaos]`; nothing touches `replay_until_lsn` or the
Startup process's redo dispatch. Two candidate additions:

- A `reachable()`/log-parseable marker at the point `orioledb_redo()` first
  sets `skip_all_future_records = true` (`recovery.c:1216`) would let
  Antithesis confirm the cutoff path was actually exercised in a given run —
  today the only signal is a `WARNING` in the server log, easy for a
  workload driver to miss if it isn't specifically tailing logs.
- Given the open question above about `o_tables`/`pg_class` cross-validation
  is unresolved by static reading, a stopevent or `always()` at whatever
  code path opens an OrioleDB relation (in `src/tableam/handler.c`) that
  records whether the descriptor came from `o_tables` matching or mismatching
  the `pg_class` generation at hand would directly resolve it without
  needing a full manual trace — the fastest path to answering this
  property's central open question.
