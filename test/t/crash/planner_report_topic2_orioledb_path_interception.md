# Topic 2: How OrioleDB Intercepts and Patches the PostgreSQL Query Plan

This is part 2 of a three-part investigation into how OrioleDB participates in
PostgreSQL's query planning. Topic 1 covers the vanilla planner baseline (how
`make_one_rel` / `set_plain_rel_pathlist` / `cost_seqscan` build the initial
`rel->pathlist`). Topic 3 covers OrioleDB's own cost estimator
(`orioledb_amcostestimate`, `OPath` cost shape). This topic sits in between:
it documents the **interception layer** — the hook plumbing, the custom-scan
registration, and the surgical pathlist rewrite that swaps PG-built `Path`,
`IndexPath`, and `BitmapHeapPath` nodes for OrioleDB's own `CustomPath` while
leaving secondary-index paths untouched.

## Hook installation in `_PG_init`

OrioleDB plugs into the planner through PG's function-pointer hooks
(`extern PGDLLIMPORT` slots), installed in `_PG_init` once per backend when
the extension loads via `shared_preload_libraries = 'orioledb.so'`. The
pathlist hook install is at `src/orioledb.c:1250-1254`:

```c
old_set_rel_pathlist_hook = set_rel_pathlist_hook;
...
set_rel_pathlist_hook = orioledb_set_rel_pathlist_hook;
set_plain_rel_pathlist_hook = orioledb_set_plain_rel_pathlist_hook;
```

Two planner hooks matter for Topic 2:

1. **`set_rel_pathlist_hook`** (declared at
   `/home/user/work/postgres/src/include/optimizer/paths.h:30-34`) — fires
   *after* the core planner has finished populating `rel->pathlist` for a
   baserel. This is the surgical-rewrite entry point and the focus of this
   report.

2. **`set_plain_rel_pathlist_hook`** — fires *inside*
   `set_plain_rel_pathlist`, *before* the core planner adds a seqscan path.
   Its return value controls whether the core seqscan path is added at all.
   OrioleDB uses it to mutate `IndexOptInfo` entries (appending PK columns to
   SK indextlists so that an IndexOnlyScan becomes feasible on the SK) — see
   `src/tableam/scan.c:225-293`. This hook is part of the patched-PG
   extensibility patch (`patches17_6`), not stock PG.

OrioleDB respects the cooperative-chaining idiom:
`orioledb_set_rel_pathlist_hook` chains to `old_set_rel_pathlist_hook` on
the way out (`src/tableam/scan.c:409-410`).

The `_PG_init` block installs many other hooks (`shmem_request_hook`,
`CheckPoint_hook`, xact callbacks, `get_relation_info_hook`,
`IndexAMRoutineHook`, etc. — `src/orioledb.c:1245-1289`); only
`set_rel_pathlist_hook` and `set_plain_rel_pathlist_hook` are part of the
path-rewrite story.

## Custom scan registration: `o_path_methods`, `o_scan_methods`, `o_scan_exec_methods`

PG's custom-scan API is a three-table affair, declared in
`/home/user/work/postgres/src/include/nodes/extensible.h`:

- `CustomPathMethods` (extensible.h:92-106) — has one mandatory entry,
  `PlanCustomPath`, which converts a `CustomPath` into a `Plan`. This is the
  Path -> Plan boundary.
- `CustomScanMethods` (extensible.h:112-118) — has one mandatory entry,
  `CreateCustomScanState`, which creates the executor-time `CustomScanState`
  from the `CustomScan` plan node. This is the Plan -> State boundary, and
  it is what gets stored in a registry so the plan-deserialiser can rehydrate
  a `CustomScan` after a serialise/parse cycle.
- `CustomExecMethods` (extensible.h:124-158) — the executor-side vtable with
  `BeginCustomScan` / `ExecCustomScan` / `EndCustomScan` / `ReScanCustomScan`
  plus optional mark/restore, parallel-DSM and `ExplainCustomScan` hooks.

OrioleDB defines instances of all three at `src/tableam/scan.c:92-119`:

```c
static CustomPathMethods o_path_methods =
{
    .CustomName = "o_path",
    .PlanCustomPath = o_plan_custom_path
};

CustomScanMethods o_scan_methods =
{
    "o_scan",
    o_create_custom_scan_state
};

static CustomExecMethods o_scan_exec_methods =
{
    "o_exec_scan",
    o_begin_custom_scan,
    o_exec_custom_scan,
    o_end_custom_scan,
    o_rescan_custom_scan,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    o_explain_custom_scan
};
```

Note that `o_scan_methods` is *not* static — it has external linkage because
other translation units want to compare a `CustomScan`'s `methods` pointer
against it to identify "is this OrioleDB's CustomScan?" (the helpers
`is_o_custom_scan` and `is_o_custom_scan_state` at
`src/tableam/scan.c:121-131` use exactly this pointer-equality test).

Only `o_scan_methods` is *registered* with the executor:

```c
/* src/orioledb.c:1234 */
RegisterCustomScanMethods(&o_scan_methods);
```

`RegisterCustomScanMethods` keeps a hash table keyed by `CustomName` so that
the plan-deserialiser can find the right method pointer when reading a
serialised plan back in (this matters for parallel workers and for cached
plans). `o_path_methods` and `o_scan_exec_methods` are *not* registered —
they are reached directly via the `methods` pointer carried inside
`CustomPath` and `CustomScanState` respectively.

The three vtables correspond one-to-one to the three lifecycle stages of an
OrioleDB scan:

| Stage           | Node type                              | Methods vtable        | Defining file:line          |
|-----------------|----------------------------------------|-----------------------|-----------------------------|
| Path            | `CustomPath`                           | `o_path_methods`      | `src/tableam/scan.c:92-96`  |
| Plan            | `CustomScan`                           | `o_scan_methods`      | `src/tableam/scan.c:98-102` |
| Executor state  | `CustomScanState` (`OCustomScanState`) | `o_scan_exec_methods` | `src/tableam/scan.c:104-119`|

These three nodes are built in sequence by the planner pipeline:
`transform_path` (path) -> `o_plan_custom_path` (plan node) ->
`o_create_custom_scan_state` (executor state). Each section below explains
one of those transitions.

## The PG hook entry point: where `set_rel_pathlist_hook` actually fires

The hook fires once per base relation, at the end of `set_rel_pathlist` in
the core planner. The call site is
`/home/user/work/postgres/src/backend/optimizer/path/allpaths.c:539-540`:

```c
if (set_rel_pathlist_hook)
    (*set_rel_pathlist_hook) (root, rel, rti, rte);
```

By the time the hook is invoked, the core planner has already finished the
work that Topic 1 describes:

- `set_plain_rel_pathlist` (allpaths.c:765) has added a seqscan path via
  `create_seqscan_path` (allpaths.c:779).
- `create_index_paths` (allpaths.c:786) has added one `IndexPath` per
  applicable index (with `pathkeys` already populated for ordered scans), and
  has additionally added `BitmapHeapPath`s wrapping bitmap-OR/AND trees over
  `BitmapIndexPath` children.
- Partial paths (parallel seqscan, parallel bitmap) have been added to
  `rel->partial_pathlist`.

The comment above the hook call site is explicit about the contract
(allpaths.c:533-538):

> Allow a plugin to editorialize on the set of Paths for this base relation.
> It could add new paths (such as CustomPaths) by calling `add_path()`, or
> `add_partial_path()` if parallel aware. It could also delete or modify paths
> added by the core code.

OrioleDB takes the "delete or modify" branch. It does not call `add_path()`
(which would have to recompute the path's place in the sorted pathlist);
instead it walks `rel->pathlist` in place, calling `list_delete_nth_cell`
and `list_insert_nth` to swap entries at the same index. This is a deliberate
choice: by preserving the index in the list it avoids `add_path()`'s
dominance-based pruning, which would otherwise compare the new `CustomPath`'s
cost against existing entries and might drop it.

Note that this hook runs *after* `set_rel_size` has finished and *before*
`generate_useful_gather_paths` (allpaths.c:558) — so partial-path edits made
inside the hook still feed into Gather construction, which is why
`orioledb_set_rel_pathlist_hook` also walks `rel->partial_pathlist`.

## `orioledb_set_rel_pathlist_hook`: the surgical pathlist rewrite

The entry point is at `src/tableam/scan.c:321-411`. The function's job is to
replace every PG-generated scan path that touches an OrioleDB table with an
equivalent OrioleDB CustomPath, while leaving paths on non-OrioleDB tables
alone.

**Step 1: filter to OrioleDB relations only** (`src/tableam/scan.c:324-330`):

```c
if (rte->rtekind == RTE_RELATION &&
    (rte->relkind == RELKIND_RELATION || rte->relkind == RELKIND_MATVIEW))
{
    Relation relation = table_open(rte->relid, NoLock);

    if (is_orioledb_rel(relation))
    {
        ...
```

`is_orioledb_rel` is defined at `src/tableam/handler.c:2549` and is a simple
pointer equality test:

```c
return (rel->rd_tableam == (TableAmRoutine *) &orioledb_am_methods);
```

`orioledb_am_methods` is the `TableAmRoutine` struct registered as the
table access method (`src/tableam/handler.c:2480`). Any relation whose
`rd_tableam` points at it is an OrioleDB table.

**Step 2: get the OrioleDB table descriptor** (`scan.c:335-336`). The
`OTableDescr` is OrioleDB's per-table runtime descriptor; it carries
`descr->indices[]` (one `OIndexDescr` per OrioleDB index, with the PK at
`PrimaryIndexNumber == 0`). Needed to identify which `IndexPath` corresponds
to the PK.

**Step 3: iterate `rel->pathlist`** (`scan.c:341-383`):

```c
while (i < list_length(rel->pathlist)) {
    Path *path = list_nth(rel->pathlist, i);
    if (IsA(path, Path) || IsA(path, IndexPath) || IsA(path, BitmapHeapPath)) {
        bool replace = !IsA(path, Path);
        ...
```

Three path types are accepted. Anything else (`TidPath`,
`SubqueryScanPath`, etc.) is left alone — which is why `SELECT ... WHERE
ctid = ...` on an OrioleDB table produces a TidScan plan that then fails at
executor init. The `replace` flag starts `true` for non-`Path` nodes,
`false` for plain `Path` nodes (this is the load-bearing initialiser
discussed in "What stays vs what gets swapped" below).

**Step 4: special-case TABLESAMPLE** (`scan.c:352-362`). A `SampleScan` is a
plain `Path` with `pathtype == T_SampleScan`. OrioleDB
`ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, "orioledb table \"%s\" does
not support TABLESAMPLE", ...)` rather than silently swap it for an
`o_scan`, because TABLESAMPLE's row-level random sampling isn't implemented.

**Step 5: PK gating for IndexPath** (`scan.c:364-370`):

```c
if (IsA(path, IndexPath)) {
    IndexPath *ix_path = (IndexPath *) path;
    OIndexDescr *primary = GET_PRIMARY(descr);
    replace = primary->oids.reloid == ix_path->indexinfo->indexoid;
}
```

The load-bearing predicate. The IndexPath is replaced **only if** its index
OID matches the PK's index OID. SK IndexPaths set `replace = false` and
survive into the planner stage as vanilla PG `IndexPath` nodes. This is the
"SK escape hatch" discussed below.

**Step 6: perform the swap** (`scan.c:372-380`):

```c
if (replace) {
    Path *custom_path = transform_path(path, descr);
    rel->pathlist = list_delete_nth_cell(rel->pathlist, i);
    rel->pathlist = list_insert_nth(rel->pathlist, i, custom_path);
}
i++;
```

The `list_delete` + `list_insert` at the same index preserves total list
length and ordering. The loop's `i++` is unconditional — replaced and
kept-as-is paths both advance.

**Step 7: chain** (`scan.c:409-410`): if `old_set_rel_pathlist_hook` was
non-NULL when OrioleDB installed itself, call it now on the rewritten
pathlist (cooperative-hook etiquette).

## `transform_path`: building the `CustomPath` from a PG path

Defined at `src/tableam/scan.c:133-193`. Allocates a fresh `CustomPath`,
copies every cost-relevant field verbatim from the source path
(`scan.c:141-154`):

```c
result = makeNode(CustomPath);
result->path.pathtype     = T_CustomScan;
result->path.parent       = src_path->parent;
result->path.pathtarget   = src_path->pathtarget;
result->path.param_info   = src_path->param_info;
result->path.rows         = src_path->rows;
result->path.startup_cost = src_path->startup_cost;
result->path.total_cost   = src_path->total_cost;
result->path.pathkeys     = src_path->pathkeys;
result->path.parallel_aware   = src_path->parallel_aware;
result->path.parallel_safe    = src_path->parallel_safe;
result->path.parallel_workers = src_path->parallel_workers;
result->methods      = &o_path_methods;
result->custom_paths = list_make1(src_path);
```

Three things to call out:

1. **`pathtype = T_CustomScan`** — *not* `T_SeqScan` and *not* `T_IndexScan`.
   This drives the `enable_seqscan = off` behaviour discussed below.
2. **Cost is copied verbatim.** `transform_path` does *not* recost — Topic 1
   (`cost_seqscan` / `cost_index`) and Topic 3 (`orioledb_amcostestimate`)
   have already produced the final number by the time we get here.
3. **The original path is preserved in `custom_paths`**. PG's
   `create_customscan_plan` later calls `create_plan_recurse` on each entry
   of `custom_paths` and passes the resulting plans to `PlanCustomPath` as
   `custom_plans` — this is how `o_plan_custom_path` later gets hold of the
   original IndexScan/IndexOnlyScan/BitmapHeapScan plan child.

Then `transform_path` branches on the source-path type to build the
discriminating OPath stuffed into `result->custom_private`
(`scan.c:156-191`):

- **`IsA(src_path, Path)`** (the seqscan case, scan.c:156-164) — builds
  `OIndexPath { O_IndexPath, ix_num=PrimaryIndexNumber,
  scandir=ForwardScanDirection }`. Semantically: a seqscan over an OrioleDB
  table *is* a forward PK scan, because the PK B-tree is the table.
- **`IsA(src_path, IndexPath)`** (the PK case, scan.c:166-184) — finds the
  OrioleDB-internal `ix_num` by matching the PG IndexPath's
  `indexinfo->indexoid` against `descr->indices[]`, then builds `OIndexPath
  { O_IndexPath, ix_num, scandir=ix_path->indexscandir }`. The `Assert(ix_num
  < descr->nIndices)` documents the invariant that *if* an `IndexPath`
  reaches `transform_path`, then its OID must correspond to one of OrioleDB's
  indices on this table (in practice the PK, since the caller's PK gate has
  already filtered out SK paths). Preserving `indexscandir` keeps
  `ORDER BY pk DESC` (`BackwardScanDirection`) working through the
  transform.
- **`IsA(src_path, BitmapHeapPath)`** (scan.c:185-191) — builds the empty
  tag `OBitmapHeapPath { O_BitmapHeapPath }`. Everything needed at
  plan-time is read from the preserved child plan.

Returns `&result->path` (scan.c:192), aliasing the start of the
`CustomPath`. PG's list-manipulation routines see only the `Path` header per
the standard idiom (pathnodes.h:1896-1901).

## What stays vs what gets swapped — the SK escape hatch

This is the most important table in the document. For a query `SELECT col
FROM t WHERE ...` where `t` is an OrioleDB table with a primary key `pk` and
secondary indexes `sk1`, `sk2`, ...:

| Input path (from core planner) | After `orioledb_set_rel_pathlist_hook` | Why |
|---|---|---|
| `Path` (T_SeqScan)                       | **Unchanged** — vanilla PG `Path` (see observation below) | `replace = !IsA(path, Path)` starts `false` and is never reset to `true` for plain Path nodes. Table-AM callbacks then handle execution. |
| `IndexPath` on PK (IndexScan)            | `CustomPath` (o_scan, PK scan)            | `replace = true` from the `!IsA(path, Path)` init; PK gate at `scan.c:369` keeps `replace = true`. |
| `IndexPath` on PK (IndexOnlyScan)        | `CustomPath` (o_scan, PK scan)            | Same as above. The PK index *is* the table, so "index only" is always satisfiable. |
| `IndexPath` on SK (IndexScan)            | **Unchanged** — vanilla PG `IndexPath`    | PK gate at `scan.c:369` sets `replace = false`. |
| `IndexPath` on SK (IndexOnlyScan)        | **Unchanged** — vanilla PG `IndexPath`    | Same. |
| `BitmapHeapPath` (over any index)        | `CustomPath` (o_scan, bitmap heap)        | `replace = true` from init, no override. |
| `TidPath`                                | **Unchanged** (executor-time error)       | Not matched by the `IsA` filter at `scan.c:346-348`. |
| `Path` with `pathtype == T_SampleScan`   | `ereport(ERROR)` — TABLESAMPLE unsupported | Explicit early-error at `scan.c:352-362`. |

**Observation on plain `Path` (seqscan) handling.** At `scan.c:350` the
initialiser `bool replace = !IsA(path, Path);` starts `replace` as `false`
for plain `Path` nodes (which is what `create_seqscan_path` produces — see
`/home/user/work/postgres/src/backend/optimizer/util/pathnode.c:930-932`).
Nothing in the subsequent code resets `replace` back to `true` for the
plain-Path case (the TABLESAMPLE branch unconditionally `ereport(ERROR)`s,
and the PK-gate at `scan.c:364-370` only runs under `IsA(path, IndexPath)`).
So the `if (replace)` block at scan.c:372 is skipped for plain SeqScan
paths.

This means **the plain SeqScan path is left in `rel->pathlist` unchanged**,
despite `transform_path`'s `IsA(src_path, Path)` branch at scan.c:156-164
existing to handle exactly that case. The transform_path branch is reachable
only if a future change flips the `replace` initialiser. In practice, the
table-AM `scan_begin` / `scan_getnextslot` callbacks (`src/tableam/handler.c`)
handle PG's vanilla `SeqScan` plan node directly — walking the PK B-tree —
so a non-swapped SeqScan still produces correct results on OrioleDB tables;
it just doesn't go through `o_exec_custom_scan`. Worth verifying with
EXPLAIN: if `SELECT * FROM oriole_tbl` shows `Seq Scan` the table-AM path is
what's running; if it shows `Custom Scan (o_scan)` then I missed something
in this review.

**Summary**: the swap is asymmetric. PK `IndexPath` paths are always
swapped. SK `IndexPath` paths are never swapped (the SK escape hatch).
`BitmapHeapPath` paths are always swapped. Plain `Path` (SeqScan) paths
appear not to be swapped under the current code at scan.c:350.

## `partial_pathlist` pruning (parallel bitmap heap TODO)

After the main pathlist walk, the hook makes a second pass over
`rel->partial_pathlist` (`src/tableam/scan.c:385-398`):

```c
i = 0;
while (i < list_length(rel->partial_pathlist))
{
    Path *path = list_nth(rel->partial_pathlist, i);

    /*
     * TODO: Remove when parallel bitmap heap scan will be
     * implemented
     */
    if (!IsA(path, Path))
        rel->partial_pathlist = list_delete_nth_cell(rel->partial_pathlist, i);
    else
        i++;
}
```

The semantics: anything in `partial_pathlist` that is *not* a plain `Path`
(i.e. anything that is a `BitmapHeapPath` or `IndexPath`) is silently
**dropped**. Plain `Path` nodes (parallel seqscan) survive. The explicit
TODO comment at scan.c:391-393 says the long-term plan is to implement
parallel bitmap heap scan in OrioleDB and lift this restriction.

Note that parallel seqscan paths are not rewritten into `o_scan` partial
paths here — they survive as PG-side `Path` nodes. The presumption is that
the table-AM `scan_begin_parallel` / `scan_getnextslot` callbacks will
handle the parallel seqscan over OrioleDB's PK B-tree directly. Given the
seqscan-replacement gap noted in the previous section, this is internally
consistent: PG's plain SeqScan plan node is left intact whether parallel
or not, and the table-AM layer is what makes it actually walk the PK.

The pruning happens *after* the main `rel->pathlist` walk and *before* the
core planner's `generate_useful_gather_paths` call (allpaths.c:558), so
the Gather node built downstream sees only the seqscan partial paths
that survived.

## The `enable_seqscan = off` interaction

`enable_seqscan = off` makes `cost_seqscan` add `disable_cost = 1.0e10`
(`costsize.c:130`) to a seqscan path's startup cost (`costsize.c:304-305`):

```c
if (!enable_seqscan)
    startup_cost += disable_cost;
```

The penalty is keyed on the planner being *inside `cost_seqscan`*, not on
the path's `pathtype` later. So:

- The PK `o_scan` CustomPath is `T_CustomScan` and was synthesised from a
  PK *IndexOnlyScan* — `cost_seqscan` was never called for it, no
  `disable_cost` was ever added. `enable_seqscan = off` does *not* penalise
  it.
- A vanilla SK `IndexOnlyScan` (the SK escape hatch) is also not penalised
  by `enable_seqscan = off`.
- The plain `Path` SeqScan that survives the rewrite (per the observation
  above) *would* carry the penalty, but it almost never wins on cost
  against the PK `o_scan` anyway.

**Bug-investigation consequence.** When an SK is present and a user tries
to force "use the SK" by setting `enable_seqscan = off`, the disable_cost
penalty does *not* apply to the PK `o_scan` — so the planner can still
pick PK-`o_scan` over the vanilla SK `IndexOnlyScan` on raw cost. The
detailed cost-comparison arithmetic is Topic 3's territory; the structural
reason `disable_cost` is bypassed is documented here.

## `OPath`, `OCustomScanState`, and the fields carried through

OrioleDB defines a small tagged-union family stuffed into `CustomPath`'s
opaque `custom_private` list so that the plan and executor stages can tell
which PG path the CustomPath was derived from
(`src/tableam/scan.c:54-75`):

```c
typedef enum OPathTag { O_IndexPath, O_BitmapHeapPath } OPathTag;
typedef struct OPath          { OPathTag type; } OPath;
typedef struct OIndexPath     { OPath o_path; ScanDirection scandir; OIndexNumber ix_num; } OIndexPath;
typedef struct OBitmapHeapPath{ OPath o_path; } OBitmapHeapPath;
```

`OIndexPath` carries the OrioleDB-internal index number
(`PrimaryIndexNumber == 0` for the PK) and the scan direction;
`OBitmapHeapPath` is a tagged empty struct (the `BitmapHeapScan` plan
child carries everything needed). The OPath gets attached to the CustomPath
at `scan.c:163 / 183 / 190` via `list_make1(new_path)`.

**Note on cost.** The OPath structs do *not* carry a cost field. The cost
lives on `CustomPath.path.startup_cost` / `total_cost` (`scan.c:147-148`),
populated by copy-from-source-path in `transform_path` and untouched
thereafter. Topic 3's `orioledb_amcostestimate` runs *before* this hook,
during `create_index_paths` -> `cost_index` -> the index-AM
`amcostestimate` callback. By the time `transform_path` sees the path the
cost is already final.

`OCustomScanState` (`include/tableam/scan.h:36-41`) is the executor-time
state struct:

```c
typedef struct OCustomScanState
{
    CustomScanState css;
    OEACallsCounters eaCounters;
    OPlanState *o_plan_state;
} OCustomScanState;
```

- `css` — the PG-required prefix; PG core code only sees this.
- `eaCounters` — per-index EXPLAIN ANALYZE counters.
- `o_plan_state` — pointer to either an `OIndexPlanState`
  (`include/tableam/index_scan.h:43-65`) or an `OBitmapHeapPlanState`
  (`include/tableam/bitmap_scan.h:26-40`), allocated by
  `o_create_custom_scan_state` (`scan.c:520-587`) based on the OPlanTag.

`OIndexPlanState` carries the `OScanState` (iterator, key range, snapshot),
the stripped indexquals for EXPLAIN, the runtime-key bookkeeping, and a
preserved `indexRelation` handle (with the comment at
`include/tableam/index_scan.h:57-63` explaining the relcache-invalidation
hazard that motivates keeping the relation open).

## `CustomPath` -> `CustomScan` plan: `o_plan_custom_path`

The Path-to-Plan transition lives at `src/tableam/scan.c:416-506`. PG's
`create_customscan_plan` calls it via the
`o_path_methods.PlanCustomPath` slot (`scan.c:95`) once the planner has
finalised path selection. Key moves:

- **Extract the OPath tag** from `best_path->custom_private`
  (`scan.c:421`).
- **Recover the underlying plan child** — PG has already built a child plan
  from `best_path->custom_paths` (the original `IndexPath` /
  `BitmapHeapPath`) and passes it in as `custom_plans`. `o_plan_custom_path`
  unwraps any `Result` wrapper (`scan.c:452-455`).
- **Branch on OPath type** (`scan.c:457-501`):
  - For `O_IndexPath` with an `IndexScan` child: copy `targetlist`,
    `custom_scan_tlist = NIL`, qual from `ix_scan->scan.plan.qual`. Stuff
    `custom_private = list_make4(O_IndexPlan, ix_num, scandir, onlyCurIx)`.
  - For `O_IndexPath` with an `IndexOnlyScan` child: copy `targetlist`,
    `custom_scan_tlist = ixo_scan->indextlist`, qual from
    `ixo_scan->scan.plan.qual`.
  - For `O_BitmapHeapPath`: copy targetlist/qual from the bh_scan, stuff
    `custom_private = list_make2(O_BitmapHeapPlan,
    primary->fields[0].inputtype)`.
- **Set scanrelid and methods** (`scan.c:445-450`):
  ```c
  custom_scan->scan.scanrelid = rel->relid;
  custom_scan->methods = &o_scan_methods;
  custom_scan->custom_plans = custom_plans;
  ```
  The `methods = &o_scan_methods` line is the critical one — it ties the
  plan node back to the registered `CustomScanMethods` so that
  `ExecInitCustomScan` (`/home/user/work/postgres/src/backend/executor/nodeCustom.c:42`)
  can later call `cscan->methods->CreateCustomScanState(cscan)`.
- **Cost is left blank** — the comment at `scan.c:443` says "plan costs will
  be filled by create_customscan_plan". `o_plan_custom_path` does not touch
  `plan->plan_rows` / `plan->total_cost` directly.

## `CustomScan` -> `CustomScanState` executor node: `o_create_custom_scan_state`

The Plan-to-State transition runs at `src/tableam/scan.c:515-588`. PG's
`ExecInitCustomScan` calls it through the registered
`CustomScanMethods.CreateCustomScanState` slot. Key moves:

- **Allocate** an `OCustomScanState` (size `sizeof(OCustomScanState)`, not
  `sizeof(CustomScanState)`) — the "larger struct embedding CustomScanState"
  idiom from `nodeCustom.c:34-39`. `scan.c:518-519`.
- **Tag and wire** (`scan.c:523-525`): set `type = T_CustomScanState`,
  `css.methods = &o_scan_exec_methods`, `css.slotOps = &TTSOpsOrioleDB`.
  Hooking `o_scan_exec_methods` here is what makes `o_begin_custom_scan`,
  `o_exec_custom_scan`, `o_end_custom_scan`, `o_rescan_custom_scan` and
  `o_explain_custom_scan` fire downstream.
- **Discriminate on OPlanTag** (`scan.c:520-585`): allocate either an
  `OIndexPlanState` or an `OBitmapHeapPlanState`, copy in the right fields
  from `cscan->custom_private` and from the child plan node (the IndexScan,
  IndexOnlyScan, or BitmapHeapScan that was the source).

Once returned, `ExecInitCustomScan` proceeds to call
`methods->BeginCustomScan` (`nodeCustom.c:108`), which lands in
`o_begin_custom_scan` (`scan.c:594`). That function `index_open`s the
OrioleDB index with `AccessShareLock` (`scan.c:620`), initialises
scan/runtime-key arrays via `init_index_scan_state`, and stashes the open
`Relation` handle in `ix_plan_state->indexRelation` (`scan.c:636`) per the
relcache-invalidation comment at `scan.c:629-635`. From there
`ExecCustomScan` (`nodeCustom.c:113-122`) dispatches each tuple fetch to
`o_exec_custom_scan` (`scan.c:677`).

## Other planner-adjacent hooks installed at `_PG_init`

For completeness, two other hooks touch planner behaviour:

- **`get_relation_info_hook`** (`src/orioledb.c:1270`) ->
  `orioledb_get_relation_info_hook`. Runs much earlier than
  `set_rel_pathlist_hook` — during `get_relation_info` in the planner,
  where `RelOptInfo->indexlist` is populated. OrioleDB uses it to override
  the `IndexOptInfo` entries with OrioleDB-aware metadata (canreturn flags
  for the columns the PK-and-SK fusion can return, etc.).

- **`set_plain_rel_pathlist_hook`** (`src/orioledb.c:1254`) — already
  described under "Hook installation". Runs *before* the core planner adds
  the seqscan path; OrioleDB uses it to augment SK `indexlist` entries
  with PK columns so that an IndexOnlyScan on the SK becomes feasible
  (`src/tableam/scan.c:225-293`). This is *crucial* for the
  "kept-as-is SK IndexOnlyScan" survival path described above.

No `planner_hook`, no `create_upper_paths_hook`, no
`set_join_pathlist_hook`. OrioleDB does not intervene at the join level or
in the upper-rel paths (aggregation, sort, etc.); it only rewrites baserel
scan paths.

## Before/after diagram of the path list

Picture an `OrioleDB` table `t` with PK `(pk_col)` and an SK on
`(sk_col)`. Run `SELECT pk_col, sk_col FROM t WHERE sk_col = 42`. After
`set_plain_rel_pathlist` -> `create_index_paths`, the PG core planner has
populated `rel->pathlist` as:

```
rel->pathlist (before orioledb_set_rel_pathlist_hook):

  [0] Path           pathtype=T_SeqScan       cost=0.00..290.00  rows=10000
  [1] IndexPath      pathtype=T_IndexScan     cost=0.15..8.32    indexoid=t_pkey
  [2] IndexPath      pathtype=T_IndexOnlyScan cost=0.15..4.32    indexoid=t_pkey
  [3] IndexPath      pathtype=T_IndexScan     cost=0.15..2.85    indexoid=t_sk_idx
  [4] IndexPath      pathtype=T_IndexOnlyScan cost=0.15..1.85    indexoid=t_sk_idx  <-- cheapest
  [5] BitmapHeapPath bitmapqual=BitmapIndexPath(t_sk_idx) cost=0.50..6.10
```

After `orioledb_set_rel_pathlist_hook` runs, the same list looks like:

```
rel->pathlist (after orioledb_set_rel_pathlist_hook):

  [0] Path        (UNCHANGED — `replace` stays false at scan.c:350)
                  pathtype=T_SeqScan  cost=0.00..290.00

  [1] CustomPath  <-- WAS IndexPath(PK,IndexScan)
                  pathtype=T_CustomScan  methods=&o_path_methods
                  custom_paths   = [ original IndexPath(PK,IndexScan) ]
                  custom_private = [ OIndexPath { O_IndexPath,
                                                  PrimaryIndexNumber,
                                                  ForwardScanDirection } ]
                  cost=0.15..8.32 (copied verbatim)

  [2] CustomPath  <-- WAS IndexPath(PK,IndexOnlyScan)
                  pathtype=T_CustomScan  methods=&o_path_methods
                  custom_paths   = [ original IndexPath(PK,IndexOnlyScan) ]
                  custom_private = [ OIndexPath { ...PrimaryIndexNumber... } ]
                  cost=0.15..4.32

  [3] IndexPath   (UNCHANGED — SK escape hatch)
                  pathtype=T_IndexScan      indexoid=t_sk_idx  cost=0.15..2.85

  [4] IndexPath   (UNCHANGED — SK escape hatch)
                  pathtype=T_IndexOnlyScan  indexoid=t_sk_idx  cost=0.15..1.85
                                                              <-- still wins

  [5] CustomPath  <-- WAS BitmapHeapPath
                  pathtype=T_CustomScan  methods=&o_path_methods
                  custom_paths   = [ original BitmapHeapPath ]
                  custom_private = [ OBitmapHeapPath { O_BitmapHeapPath } ]
                  cost=0.50..6.10
```

Visual summary of what got swapped vs kept:

```
                BEFORE                          AFTER
              -----------                     ----------

  [0] SeqScan Path  ------- KEPT AS-IS ------> SeqScan Path
                            (replace stays false)  (table-AM handles exec)

  [1] PK IndexPath  ---------- swapped ------> CustomPath (o_scan, PK)

  [2] PK IndexOnlyPath ------- swapped ------> CustomPath (o_scan, PK)

  [3] SK IndexPath  ------- KEPT AS-IS ------> SK IndexPath
                            (PK gate fails)       (vanilla PG IndexScan)

  [4] SK IndexOnlyPath  --- KEPT AS-IS ------> SK IndexOnlyPath
                            (PK gate fails)       (vanilla PG IndexOnlyScan)

  [5] BitmapHeapPath  ------- swapped ------> CustomPath (o_scan, bitmap)
```

Result: the optimiser then picks the cheapest path. In the worked example
that's the SK `IndexOnlyPath` at `total_cost = 1.85` — a vanilla PG path,
not OrioleDB's o_scan. The resulting plan is a regular `IndexOnlyScan` node
that talks to OrioleDB through the index-AM glue, *not* through
o_scan/CustomScan.

For a query that has to scan the whole table (`SELECT * FROM t`), the SK
paths are not generated in the first place, the seqscan is comparatively
expensive, and the PK CustomPaths win — resulting in a `Custom Scan
(o_scan)` plan that walks the PK B-tree.

## Bug-investigation implications

The path-interception layer has four observable consequences that are
relevant to the active SK-leak investigation under `test/t/crash/`:

1. **PK vs SK queries traverse different code paths in the executor.**
   - A `count(DISTINCT col)` over an SK that can be answered via
     `IndexOnlyScan` on the SK goes through PG's `nodeIndexonlyscan.c` and
     the OrioleDB index-AM glue (`src/indexam/`) — *not* through
     `o_exec_custom_scan`. SK leaks therefore have to be detected at the
     index-AM tuple-emission level, not at the o_scan level.
   - A `SELECT * FROM t` goes through `o_exec_custom_scan` ->
     `o_index_scan_getnext` over the PK. PK-side bugs surface here.

   The crash-test harness in `test/t/crash/rr_stress_test.py` uses both
   query shapes deliberately to diff "what the SK believes" vs "what the
   PK believes". The SK side reading goes through vanilla PG plan nodes;
   the PK side reading goes through `o_scan`.

2. **`enable_seqscan = off` cannot be used to force SK-only diagnostic
   queries on OrioleDB tables.** The PK `o_scan` is `T_CustomScan` and
   sidesteps the `disable_cost` penalty entirely (see "The `enable_seqscan
   = off` interaction" section). Diagnostics that need a strictly SK
   reading need to either hint at the index name or hide the table behind
   a CTE that the planner cannot pushdown.

3. **`get_relation_info_hook` runs *before* `set_rel_pathlist_hook`.** Any
   index metadata mutation OrioleDB performs (e.g. adding PK columns to SK
   index target lists) is in place by the time `create_index_paths` runs.
   This is *why* the SK IndexOnlyScan paths are even feasible — without
   the PK columns appended, the SK alone could not return PK values for an
   `IndexOnly` plan, and only `BitmapHeapPath` over the SK would be
   generated. The hook chain (`get_relation_info_hook` then
   `set_plain_rel_pathlist_hook` then `set_rel_pathlist_hook`) is therefore
   load-bearing for the SK escape hatch.

4. **The seqscan-swap gap deserves direct verification.** Reading
   `scan.c:350` in isolation, plain `Path` (SeqScan) nodes should not be
   replaced. Worth confirming with `EXPLAIN (VERBOSE) SELECT * FROM
   oriole_tbl` whether the executor shows `Seq Scan` (table-AM handles it)
   or `Custom Scan (o_scan)` (some logic was missed in this review).

These observations frame the path-rewrite layer as the boundary between
"vanilla PG planning that OrioleDB lets through" and "OrioleDB-specific
custom-scan execution". The SK escape hatch (paths kept as PG-native
IndexPaths) is what enables certain SK-vs-PK diagnostic queries; the PK
swap (paths converted to CustomPaths) is what wraps the rest in OrioleDB's
own snapshot/iterator machinery.
