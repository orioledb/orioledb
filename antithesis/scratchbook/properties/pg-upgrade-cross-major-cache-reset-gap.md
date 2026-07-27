---
slug: pg-upgrade-cross-major-cache-reset-gap
attention_focus: Version Compatibility (gap-filling follow-up pass)
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
external_references:
  - path: (none — orioledb_postgres explicitly out of scope per task instructions)
---

# pg-upgrade-cross-major-cache-reset-gap

## Status: feature not present on `main`/`a975c702` — this is a forward-looking property about active, unmerged branch work

**This is the single most important caveat for this property.** `git grep -i
pg_upgrade` across `src/`, `include/`, `doc/`, `sql/`, `test/` on `a975c702`
returns **zero hits** — cross-major `pg_upgrade` support does not exist on
`main` at all today. It exists only on two unmerged remote branches:

- `origin/pg_upgrade` (tip `ccc63653`, based off `9ba4d1bd`, 11 commits)
- `origin/nickb/pg_upgrade_test` (tip `87b31108`/`3e8f4f5a`, 2 commits on top
  of `origin/pg_upgrade`)

`git merge-base --is-ancestor origin/pg_upgrade a975c702` → **NO**. Neither
branch is an ancestor of the analyzed commit. Confirmed with
`git branch -a --contains 63e7fdc1` → only `origin/pg_upgrade`.

Despite not being merged, this is exactly the kind of "substantial, actively-
developed, in-scope feature" the task description flagged: 11+2 real commits,
dated as recently as 2026-07-24 (three days before the analyzed commit's own
2026-07-27 timestamp), by two different authors, with CI wiring
(`ci/pg_upgrade.sh`), a doc page (`doc/usage/pg-upgrade.mdx`), and — most
importantly for this catalog — **three rounds of genuine crash bugs found and
iteratively fixed during development**, one of which was still being actively
reproduced and patched on the second branch as of its last commit. This
property is written up now so it is not rediscovered from scratch whenever
this branch merges, and because the underlying architectural gap it documents
(the OSysCache trees' PG-major-dependent on-disk layout) is real today even
though the pg_upgrade feature that exposes it is not yet merged.

## What led to identifying this property

Investigating "is there active pg_upgrade support/work for OrioleDB" (per the
task's Task B) found the two branches above via
`git log --all --oneline -i --grep="pg_upgrade"` and `git branch -a | grep -i
upgrade` (a third branch, `origin/upgrade_actions`, is unrelated — it's a
GitHub Actions version bump, not `pg_upgrade` support; ruled out by reading
its one commit).

The core commit, `63e7fdc1` ("Support cross-major pg_upgrade of OrioleDB
clusters"), directly touches `include/checkpoint/control.h` and
`src/checkpoint/control.c` — the exact file/function
(`check_checkpoint_control()`) the existing property
`checkpoint-control-version-gate-fails-safe.md` already treats as "the
highest-blast-radius binary-format contract in the codebase." It bumps
`ORIOLEDB_CHECKPOINT_CONTROL_VERSION` from 1 to 2, adds a `pgVersion` field
(the writing server's `PG_VERSION_NUM`), and adds a v1→v2 on-the-fly CRC/layout
conversion path in `check_checkpoint_control()` — i.e., it is a live instance
of exactly the "finer-grained version gate" scenario that existing property
was written to think about, confirming the task's framing that this feature
"touch[es] the same checkpoint-control-file version gate." (This specific
v1→v2 conversion path looks correct by direct reading — it validates the v1
CRC over the v1-sized layout and always synthesizes `pgVersion = 0` "unknown,"
which forces a full cache reset, described in its own comment as "always
safe." It is not, itself, the bug this property is about — see below for the
actual defect class.)

## The actual mechanism — three real crash bugs across three iterations

OrioleDB persists several PostgreSQL-catalog-derived structures in its own
system trees (`SYS_TREES_*`, see `include/catalog/sys_trees.h`) specifically
so that **catalog-free contexts** — crash recovery and the checkpointer,
neither of which has a live transaction or catalog access — can still resolve
type/operator/function/collation/tuple-descriptor information needed to
replay WAL or write out dirty pages. There are 14 such trees:
`OPCLASS_CACHE`, `ENUM_CACHE`, `ENUMOID_CACHE`, `RANGE_CACHE`, `CLASS_CACHE`,
`PROC_CACHE`, `TYPE_CACHE`, `AGG_CACHE`, `OPER_CACHE`, `AMOP_CACHE`,
`AMPROC_CACHE`, `COLLATION_CACHE`, `DATABASE_CACHE`, `AMOP_STRAT_CACHE`,
`MULTIRANGE_CACHE`.

A cross-major `pg_upgrade` carries over the old cluster's `orioledb_data/`
directory wholesale (per the documented manual `cp -R` step — see the sibling
property `pg-upgrade-manual-data-copy-not-atomic.md`), including these system
trees, as-is. Some of these trees' on-disk entries embed **PostgreSQL-major-
version-dependent binary layout**, and the fix work found (empirically, via
crash reproduction, not just by static reasoning) at least two distinct ways
this breaks:

**(a) Raw C-struct layout mismatch — the class cache.** `o_class_cache`
serializes tuple descriptors as raw `FormData_pg_attribute` arrays. This
struct's size/layout is not guaranteed stable across PG majors. Reading a
carried-over entry on the new major trips a length-mismatch assertion in
`o_class_cache_deserialize_entry` (`elog(FATAL, ...)` at
`src/catalog/o_class_cache.c:169/197` on current `main`, confirmed present —
the assert site itself is unrelated to pg_upgrade and exists on `main` today,
it's just never fed a foreign-major entry there). Root-cause commit
`87039a9b`'s own log quotes the exact backtrace hit in the team's pg_upgrade
CI: `o_collect_funcexpr_refresh -> o_collect_function ->
o_class_cache_add_if_needed -> o_sys_cache_get_from_toast_tree ->
o_class_cache_deserialize_entry: "(ptr - data) + len == length"`.

**(b) Serialized-node-tree incompatibility — index expressions and, distinctly,
proc-cache SQL-function bodies.** Index expressions/predicates and SQL-
language function bodies are both stored as `nodeToString()`-serialized
parse trees, whose format is also not stable across majors. `o_deserialize_node`
tolerates this by silently dropping an unparseable tree (leaving `NULL`), which
is safe **only where the caller checks for it**:
  - Index descriptors have an explicit guard: `o_index_fill_descr()`
    (`src/catalog/o_indices.c:1265-1271` on `origin/pg_upgrade`) raises a clean,
    documented `ERROR` ("has expressions stored by another PostgreSQL major
    version... Run orioledb_upgrade_refresh()") if `oIndex->refresh_exprs` is
    set. This path was engineered correctly the first time.
  - The **proc cache** has no equivalent guard. `origin/nickb/pg_upgrade_test`'s
    two commits (`3e8f4f5a`, `87b31108`) found this the hard way: a
    `SECURITY DEFINER` SQL function used inside an index expression forces
    catalog-free evaluation through OrioleDB's own function-call manager
    (`o_fmgr_sql`), which reads the function's parse trees
    (`jf_targetList`/`qtlists`) straight from the proc cache with no
    `refresh_exprs`-style check — dereferencing the `NULL` trees left by a
    cross-major carry-over crashes recovery. The commit titled "Claude's fix"
    patches this the same way class/database cache were fixed: add
    `SYS_TREES_PROC_CACHE` to `sys_tree_reset_on_major_upgrade()` so the whole
    tree is wiped and rebuilt from the catalog on first (foreground,
    transactional) use, rather than adding a read-time guard analogous to the
    index-expression one.

**The fix mechanism, both times, is the same: `sys_tree_reset_on_major_upgrade()`
(`src/catalog/sys_trees.c`) enumerates which trees get unconditionally wiped
(`cleanup_btree_files`) at startup when the checkpoint control file's recorded
`pgVersion` differs from the running server's major (`checkpoint.c`'s
`resetSysCaches` flag, set once at `checkpoint_shmem_init` time). As of the
last commit read (`87b31108` on `origin/nickb/pg_upgrade_test`), exactly
three of the fourteen trees are enumerated: `DATABASE_CACHE`, `CLASS_CACHE`,
`PROC_CACHE`.**

## Why this is likely an incomplete fix, not a closed issue

`87039a9b`'s own commit message states this explicitly, unprompted: *"Proper
cross-major handling of the version-dependent OSysCache trees (class, proc,
type, ...) — whose persisted entries are not layout-compatible across majors
— is a broader issue than this change and is left as follow-up."* `e50dc63a`
repeats the same framing: *"the version-dependent OSysCache trees
(class/proc/type/...) keep per-object entries whose on-disk layout is not
compatible across PG majors, yet only the database cache is reset on a
cross-major restart. Any path that deserializes such an entry after
pg_upgrade asserts."*

This is a self-acknowledged, unresolved gap from the implementers themselves,
not a static-analysis inference. The eleven remaining trees
(`OPCLASS_CACHE`, `ENUM_CACHE`, `ENUMOID_CACHE`, `RANGE_CACHE`, `TYPE_CACHE`,
`AGG_CACHE`, `OPER_CACHE`, `AMOP_CACHE`, `AMPROC_CACHE`, `COLLATION_CACHE`,
`AMOP_STRAT_CACHE`, `MULTIRANGE_CACHE`) have not, as far as this pass's git
history reading found, been individually audited for whether their on-disk
entries are actually PG-major-layout-dependent. A quick differential grep
(`grep -l FormData_pg_ src/catalog/o_*cache*.c`) found `FormData_pg_` literals
**only** in `o_class_cache.c` — suggesting the other caches may serialize
individual scalar fields (Oid, int4, etc., generally stable-width types)
rather than blitting whole C structs, which would make the raw-struct-layout
failure mode (a) specific to class cache. But (b) — the serialized-node-tree
failure mode — is not confined to expressions/predicates/SQL-function bodies;
any other OSysCache tree that persists a `nodeToString()`-format blob (e.g.
default expressions, check constraints referenced by a cache, or anything
routed through `o_serialize_node`/`o_deserialize_node`) would have the same
"silently-NULL, callsite-must-guard" risk pattern that already bit the proc
cache once and was fixed reactively rather than by a systematic audit.

## What goes wrong if this is violated (once the feature merges)

A crash (or checkpoint) on the new-major cluster, before the affected object
is refreshed, that needs to catalog-freely deserialize a stale-format entry
from one of the un-audited trees would crash the recovery process or the
checkpointer with an `elog(FATAL)` or a NULL-pointer dereference — exactly
the failure class the class-cache and proc-cache bugs already produced twice.
Since recovery/checkpointer crashing is itself the trigger for *more*
crash-recovery (the server "recovers from the recovery crash," as literally
described in the doc's own §10 for a related bug: "orioledb recovery after
fatal error started. Unable to make multiprocess recovery."), a crash inside
this specific window could plausibly degrade into a boot-loop rather than a
single clean crash, though this was not independently confirmed for the
pg_upgrade case specifically.

## Antithesis angle

Not implementable today (feature doesn't exist on `main`). Once/if this
branch merges, the natural Antithesis scenario is exactly the one the team's
own CI step 9b already manually constructs, generalized and fuzzed:

1. Build a two-major-version harness variant (old-major primary produces a
   data directory with `orioledb`-backed tables including at least one
   expression index, one partial index, and one index referencing a
   `SECURITY DEFINER` SQL function — mirroring `3e8f4f5a`'s test fixture).
2. Run `pg_upgrade`, do the manual `orioledb_data`/`orioledb_undo` copy, start
   the new-major cluster.
3. Inject a crash (`SIGKILL`/`-m immediate`) or force a checkpoint at
   varying points **relative to** whether `orioledb_upgrade_refresh()` /
   `maybe_auto_upgrade_refresh()` has run yet in any session — including the
   currently-untested ordering where the **first-ever checkpoint** is the
   automatic background one (driven by `checkpoint_timeout`), occurring
   before any foreground session has issued a utility statement at all. This
   ordering is not exercised by the existing CI script, which always
   triggers the refresh (via `DISCARD ALL` or an explicit call) earlier in
   the same script, before its own CHECKPOINT/crash probe (step 9b) runs.
4. Vary which catalog objects are exercised catalog-free (expression index
   with a plain expression vs. one calling a SQL function vs. one using a
   custom collation/operator class/aggregate/enum/range type) to probe the
   un-audited trees, not just class+proc cache which are already fixed.
5. Assert: the new cluster comes up clean after the crash (no `FATAL`, no
   core dump), and a read-after-recovery of the affected object returns
   correct data — mirroring the existing CI step's own row-count/value
   assertions, generalized across more object types and crash-timing offsets.

## SUT-side instrumentation candidates

Per `existing-assertions.md` (0 SDK assertions exist anywhere in `src/`/
`include/` today), this whole feature has no Antithesis-visible signal at
all. If/when this branch is adopted:
- A `reachable()`/`always()` at `sys_tree_reset_on_major_upgrade()`'s call
  site (`sys_trees.c` around the `cleanup_btree_files` call) recording which
  tree numbers were actually reset on a given cross-major startup, so a
  fuzzed run can confirm which trees were exercised catalog-free afterward.
- An `unreachable()` wrapping the exact assertion pattern in
  `o_class_cache_deserialize_entry` (and the analogous check in any other
  `*_cache_deserialize_entry` function, once audited) — so a violation is
  captured as a scored property outcome rather than only as a process crash
  the harness happens to notice.
- A `reachable()` at `maybe_auto_upgrade_refresh()`'s entry and at each of its
  early-return guards (`IsBinaryUpgrade`, `RecoveryInProgress()`,
  `!IsNormalProcessingMode()`), to make the currently-invisible "has the
  refresh actually run yet in this cluster's lifetime" state externally
  observable to a fuzzed fault schedule — directly enabling the "crash before
  first refresh" scenario in step 3 above to be constructed deliberately
  rather than by chance.

## Open Questions

- Which of the eleven not-yet-enumerated `SYS_TREES_*` caches
  (`OPCLASS`/`ENUM`/`ENUMOID`/`RANGE`/`TYPE`/`AGG`/`OPER`/`AMOP`/`AMPROC`/
  `COLLATION`/`AMOP_STRAT`/`MULTIRANGE`) actually persist PG-major-dependent
  binary layout (raw struct blits per the class-cache pattern, or
  serialized node trees per the proc-cache pattern), and which are safely
  version-independent (plain scalar fields)? `(needs further investigation —
  the team's own commit messages treat this as open; a systematic audit of
  each cache's `*_deserialize_entry` function against each object type's
  cross-major field stability was not performed here, and is arguably the
  team's job once they resume this branch, not this catalog's)`
- Does a crash landing *before* the very first `maybe_auto_upgrade_refresh()`
  call in a cluster's lifetime (e.g., the automatic background checkpointer's
  first checkpoint, with zero foreground sessions having connected yet) still
  hit the documented "clean guard error" behavior, or does it reach one of
  the un-guarded catalog-free paths (like the proc-cache bug before its fix)?
  The existing CI script (`ci/pg_upgrade.sh` step 9b) always triggers the
  refresh earlier in the same script before its crash probe, so this specific
  ordering has not been exercised even in the team's own manual testing.
  `(needs further investigation once the branch is resumed — this is the
  single highest-value residual question for Antithesis to probe once this
  feature exists in the harness)`
- Is this branch (`origin/pg_upgrade` + `origin/nickb/pg_upgrade_test`)
  actively planned to merge to `main`, or is it exploratory/parked work? Not
  determinable from git history alone. `(needs human input)`
- Does `origin/nickb/pg_upgrade_test`'s "Claude's fix" (adding
  `SYS_TREES_PROC_CACHE` to the reset list) fully close the SQL-function
  proc-cache crash, or only the one specific repro (`oriole_suffix`, a
  `SECURITY DEFINER` scalar function) constructed in `3e8f4f5a`? No commit
  after `87b31108` re-runs or extends that specific CI probe to confirm.
  `(needs further investigation)`

### Investigation Log

#### Is this branch (`origin/pg_upgrade` + `origin/nickb/pg_upgrade_test`) actively planned to merge to `main`, or is it exploratory/parked work?

- Examined: `git log --all --oneline -i --grep="pg_upgrade"`, `git branch -a | grep -i upgrade`, `git merge-base --is-ancestor` checks for both branches against `a975c702`.
- Found: two branches with real, recent (2026-07-24), multi-author commits, CI wiring (`ci/pg_upgrade.sh`), and a doc page — but neither is an ancestor of the analyzed commit.
- Not found: no roadmap, issue-tracker link, or PR discussion indicating merge intent; git history alone shows active development, not intent.
- Conclusion: tagged `(needs human input)` — merge intent isn't answerable from git history alone.
