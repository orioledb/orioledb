# pg-rewind-orioledb-fullcopy-correctness

## Focus

Backup/restore under fault injection — specifically the `pg_rewind` angle
named in the evaluation gap. Per the task's scope note: `pg_rewind` is the
standard, unrelated PostgreSQL tool (rebuilds a diverged replica from a new
primary by copying changed blocks) and is explicitly **in scope**; it is not
the excluded `orioledb_rewind_*` point-in-time-undo feature.

## What led to this, and what I validated (not just repeated from docs)

`doc/usage/getting-started.mdx:288` states, under "Current limitations":

> `pg_rewind` copies OrioleDB tables completely. Shortly OrioleDB will
> implement incremental copying of OrioleDB tables using `pg_rewind`.

The task explicitly asked me not to just repeat this claim but to validate
the mechanism against the actual code. I traced it as follows.

**1. `pg_rewind`'s incremental-copy mechanism, as (publicly, well-known)
implemented, depends on generic WAL block-reference metadata.** `pg_rewind`
determines which blocks changed since the point of divergence by scanning
WAL and reading each record's registered block references
(`XLogRecGetBlockTagCount()`/`XLogRecGetBlockTag()` in upstream Postgres),
which are populated by any WAL-inserting code that calls
`XLogRegisterBuffer()` before `XLogInsert()`. This mechanism is generic
across resource managers — it works for heap, btree, gin, gist, etc. without
`pg_rewind` needing rmgr-specific decode logic, *provided* the rmgr actually
registers block references at insert time.

**2. OrioleDB's WAL-insertion code never calls `XLogRegisterBuffer()` —
confirmed by direct reading of the only function that inserts OrioleDB WAL,
`log_logical_wal_container()` (`src/recovery/wal.c:741-786`):**

```c
XLogBeginInsert();
XLogRegisterData((char *) (&wal_version), sizeof(wal_version));
...
XLogRegisterData((char *) &rec, sizeof(rec));      /* xact info, optional */
...
XLogRegisterData((char *) &origin, sizeof(origin)); /* origin info, optional */
...
XLogRegisterData(ptr, length);                      /* the actual payload */
return XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_CONTAINER);
```

Every single field, including the opaque payload, goes through
`XLogRegisterData()`. A repo-wide grep for `XLogRegisterBuffer` in `src/` and
`include/` returns zero hits (the only `XLogRegister*`/`XLogInsert*` call
sites in the whole extension are the ones shown above, in this one function,
called from `flush_local_wal()`/`flush_local_wal_if_needed()`). **This means
every OrioleDB WAL record is, from any generic WAL-consuming tool's point of
view, a single opaque data blob with zero block references** — not merely
"a different format `pg_rewind` doesn't decode," but structurally invisible
to the generic block-reference mechanism `pg_rewind`'s incremental-copy path
relies on, regardless of whether `pg_rewind` ever added OrioleDB-specific
decode logic. This is the actual mechanism behind the doc's "copies OrioleDB
tables completely" statement, not just its assertion.

**3. OrioleDB's on-disk files live outside the paths generic fallback tools
special-case for incremental diffing — confirmed by the project's *own*
test for a different tool with the identical root cause.**
`include/orioledb.h:107-108` defines `ORIOLEDB_DATA_DIR "orioledb_data"` and
`ORIOLEDB_UNDO_DIR "orioledb_undo"` — top-level directories directly under
`PGDATA`, siblings of `base/`, `pg_wal/`, etc., not nested under
`base/<dboid>/` in the standard per-relation file-naming convention.
`test/integration/walg_test.py::test_delta_backup_ships_whole_orioledb_file`
(read in full) already documents and *tests* the identical root cause for
wal-g: its own comment states "wal-g's page-level delta diffing... only
applies to files whose path contains `base/` or `pg_tblspc/`. OrioleDB's
files live under `orioledb_data/` instead, so they never qualify" — and pins
down that a changed OrioleDB file is re-shipped in full. This is the same
mechanism (a physical-layout assumption baked into a generic backup/rebuild
tool, violated by OrioleDB's separate top-level directories), independently
confirmed for a third tool.

**4. Whether `pg_rewind`'s fallback is a *safe* full copy (as documented) or
could, for some file, be an *unsafe* skip is not fully resolved from this
repo alone** — see Open Questions. One piece of positive evidence found in
this repo: `src/s3/checkpoint.c:100` (in code adapted from core Postgres's
own base-backup exclusion-list logic, comment reads *"this list should be
kept in sync with the filter lists in pg_rewind's filemap.c"*) lists the
directories `pg_rewind`'s exclude-list is known to special-case
(`pg_stat_tmp`, `pg_replslot`, `pg_dynshmem`, etc. — standard transient
Postgres-internal directories). `orioledb_data`/`orioledb_undo` do not match
any of these documented exclusions, which is evidence (not proof, since
`pg_rewind`'s actual `filemap.c` lives in the excluded `orioledb_postgres`
source) that these directories fall into `pg_rewind`'s generic "not a known
special-cased path — copy whole file if content differs" bucket, consistent
with (and explaining the safety of) the documented "copies completely"
behavior rather than a silent-skip risk.

## Why this is worth a property despite the mechanism being well-explained

The mechanism *why* `pg_rewind` can't do incremental copy is now confirmed,
but that only establishes the *documented* behavior is plausible — it does
not establish that the resulting full-copy fallback actually produces a
**correct** rebuild in every case, especially combined with fault injection
during the rewind itself (the target's crash-then-diverge scenario `pg_rewind`
exists to handle is, definitionally, always preceded by some kind of fault —
this tool's entire purpose is disaster recovery after a diverged replica).
Zero test coverage exists for `pg_rewind` anywhere in this repo (confirmed:
grep for `pg_rewind` across `test/` returns no hits) — a real, total gap, not
an efficiency-only concern like the already-tested wal-g case.

Concretely unresolved: does the full-copy fallback correctly capture **every**
OrioleDB-introduced file type — not just user-table data files under
`orioledb_data/`, but also the checkpoint control file
(`include/checkpoint/control.h`), `.map` free-extent files, undo files under
`orioledb_undo/`, and sys-tree files (`o_tables`/`o_indices` metadata,
themselves stored in OrioleDB's own B-tree format, not `pg_catalog` heap
tables) — given all of these are subject to the same "opaque WAL, no block
references" property as ordinary table data? A partial or inconsistent
full-copy across these different file categories (e.g. copying the data
files but missing a concurrently-rotated `.map`/temp file, or copying the
checkpoint control file at a moment that doesn't correspond to the same
logical instant as the data files copied alongside it) is exactly the kind
of correctness question the documented "copies completely" framing doesn't
address — it only speaks to the top-level table data, not to internal
consistency across OrioleDB's several independently-versioned persistence
structures (`sut-analysis.md` §2's "five independent version knobs... two
independently-versioned control structures must stay consistent across
crash" finding applies with equal force to a `pg_rewind`-rebuilt data
directory as to a crash-recovered one).

## What goes wrong if the property is violated

A `pg_rewind`-rebuilt standby that looks superficially fine (starts up,
serves queries) but has an internally inconsistent OrioleDB state — e.g. a
checkpoint control file whose recorded LSN/undo boundaries don't correspond
to the actually-copied B-tree/undo file contents — is precisely the silent,
hard-to-detect corruption class `sut-analysis.md` §10 ranks as worst-case for
a database engine, and specifically for a tool whose entire purpose is
disaster recovery: a `pg_rewind` that "succeeds" but leaves a subtly-broken
node is worse than one that fails loudly, because the failure surfaces
later, away from the incident that prompted the rewind.

## The property

| | |
|---|---|
| **Type** | Safety |
| **Property** | A physical replica rebuilt via `pg_rewind` against a diverged primary is, after the rebuild completes and the node is started, indistinguishable in OrioleDB state from a replica built via a fresh `pg_basebackup` against the same primary at the same point — same row content, and `verify_orioledb()`/`pg_amcheck` report no structural issues on any OrioleDB relation. |
| **Invariant** | `Always(rewound_node_matches_fresh_basebackup)`: after `pg_rewind` completes and the rewound node starts, compare (a) content fingerprints of OrioleDB tables against the source primary, and (b) `verify_orioledb()` results (see `backup-restore-lacks-structural-oracle`) — both must match a fresh-basebackup baseline taken at the same logical point. `Reachable(pg_rewind_completed_against_diverged_orioledb_data)` as a companion, since this scenario (a real timeline divergence involving OrioleDB tables, not just heap/catalog changes) doesn't exist anywhere in the current test suite and needs to be confirmed as actually constructed before the `Always` check carries weight. |
| **Antithesis Angle** | Needs a scenario the harness doesn't have today: two nodes that diverge (e.g. a promoted former-standby vs. its old primary, both having taken independent writes to OrioleDB tables on different timelines) followed by a `pg_rewind` of the loser against the winner. This is a natural extension of the primary/standby topology `deployment-topology.md` already recommends adding for the replication-focused properties — the same topology, with an additional failover-then-rewind step. Combine with fault injection (kill `pg_rewind` mid-copy, kill the target node mid-post-rewind-recovery) for the fuller version of this property, mirroring `backup-window-crash-untested`'s angle applied to `pg_rewind` instead of pgbackrest/wal-g. |
| **Why It Matters** | `pg_rewind` is the one backup/restore-adjacent tool in the evaluation gap that has literally zero test coverage in this repo (pgbackrest and wal-g at least have substantial, if fault-injection-free, integration suites) — despite being explicitly named in `sut-analysis.md` §10 as part of the documented disaster-recovery workflow, and despite the mechanism validated above showing OrioleDB's WAL format is structurally opaque to it in a way that's unique among the tools in this gap (pgbackrest/wal-g at least see the files' *bytes*; `pg_rewind`'s entire incremental-diff mechanism is bypassed, making its full-copy fallback path the *only* path OrioleDB tables ever take through this tool). |

**Open Questions:**

- Does `pg_rewind`'s actual `filemap.c` file-classification logic (source
  lives in `/Users/artur/supabase/orioledb_postgres`, explicitly excluded
  from further consultation per this task's scope restriction) definitely
  place `orioledb_data/`/`orioledb_undo/` into the generic "copy whole file
  if changed" bucket, as the indirect evidence in `src/s3/checkpoint.c:100`
  suggests, or could some specific file within those directories (e.g. the
  checkpoint control file, which unlike ordinary data files is a small,
  frequently-rewritten, CRC-protected single file more similar in shape to
  `pg_control` — which `pg_rewind` **does** special-case) be misclassified?
  `(needs human input or a black-box pg_rewind run — cannot be resolved by
  further code reading, since the deciding code is in the excluded repo)`
- Is a genuine OrioleDB-relevant divergence-then-rewind scenario (as opposed
  to a rewind that only needs to reconcile heap/catalog changes) even
  reachable in the current harness, which per `deployment-topology.md` has no
  second Postgres node at all today? `(depends entirely on whether the
  recommended primary/standby topology addition is built — not resolvable
  until then)`
- Does `pg_rewind`'s full-copy fallback correctly handle a file that exists
  on the diverged target but has been **removed** on the source (e.g. a
  `.map`/temp file cleaned up by `orioledb.remove_old_checkpoint_files`, or
  an undo file recycled past a checkpoint) — i.e., is file *deletion*
  reconciliation, not just copying, also verified correct for OrioleDB's
  non-standard file layout? Not investigated — a distinct sub-question from
  "are changed files copied."

### Investigation Log

#### Does OrioleDB's WAL-insertion path ever register block references (XLogRegisterBuffer), which pg_rewind's incremental-diff mechanism depends on?

- Examined: `src/recovery/wal.c` (`log_logical_wal_container`, the sole
  function calling `XLogInsert(ORIOLEDB_RMGR_ID, ...)`, lines 741-786);
  repo-wide grep for `XLogRegisterBuffer` and `XLogRegisterData` across
  `src/` and `include/`.
- Found: zero calls to `XLogRegisterBuffer` anywhere in the extension; every
  field of every OrioleDB WAL record (version, flags, xact info, origin
  info, and the opaque payload) is registered via `XLogRegisterData`.
- Conclusion: OrioleDB WAL records carry no generic block-reference metadata
  at all — this is a structural fact about the WAL format, independently
  confirming (at the mechanism level, not just citing the doc) why a
  block-reference-based tool like `pg_rewind` cannot perform incremental
  diffing against OrioleDB files.

#### Does pg_rewind's exclude-list (filemap.c, in the excluded orioledb_postgres repo) accidentally skip orioledb_data/orioledb_undo rather than fully copying them?

- Examined: `src/s3/checkpoint.c:83-120` (this repo's own base-backup-style
  exclusion list, whose comment explicitly cross-references "the filter
  lists in pg_rewind's filemap.c" for consistency).
- Found: the documented exclusion set (`pg_stat_tmp`, `pg_replslot`,
  `pg_dynshmem`, and similar standard transient directories) contains no
  entry resembling `orioledb_data`/`orioledb_undo`.
- Not found: the actual `pg_rewind` `filemap.c` source itself, which lives in
  `/Users/artur/supabase/orioledb_postgres` and is out of scope per this
  task's explicit scope restriction.
- Conclusion: indirect evidence (from a file in *this* repo, not the
  excluded one) supports the docs' framing that OrioleDB files are fully
  copied, not silently skipped — but this is inference from an adjacent,
  intentionally-kept-in-sync list, not direct confirmation of `pg_rewind`'s
  own logic. Tagged as `(needs human input or a black-box pg_rewind run)`
  above rather than treated as settled.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing**, consistent with `existing-assertions.md`'s finding that zero
assertions exist outside `sk-recovery-race[-chaos]`. No stopevent or
Antithesis SDK call exists anywhere near a `pg_rewind`-relevant code path
(there being no `pg_rewind`-specific code in this extension at all — the
interaction is entirely at the file/WAL-format level, external to any
in-process hook). The concrete addition, once a divergence-then-rewind
scenario exists in the harness: a workload-side `always()` comparing
`verify_orioledb()` output and content fingerprints between the rewound node
and a fresh-basebackup baseline, per the Invariant above — no new C-level
instrumentation is needed since the check is entirely SQL-visible.
