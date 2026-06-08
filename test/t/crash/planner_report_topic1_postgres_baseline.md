# Topic 1 — Vanilla PostgreSQL: How the Planner Picks a Scan Node for a Single Relation

This is part 1 of a 3-part investigation. It documents what *upstream* PostgreSQL
(PG 17, branch reference: `/home/user/work/postgres/` at orioledb's `patches17_6`,
but the planner code is untouched by orioledb) does between parse-analysis and
plan generation, focusing on the single-relation scan-path decision: Seq Scan vs
Index Scan vs Index Only Scan vs Bitmap Heap Scan. Topic 2 covers
`set_rel_pathlist_hook` and how orioledb intercepts this pipeline; Topic 3
covers orioledb's cost callbacks. Here we stay in `src/backend/optimizer/`.

## 1. The Path-Generation Pipeline for a Base Relation

After parse-analysis and rewrite, the executor entry point is
`standard_planner` at `/home/user/work/postgres/src/backend/optimizer/plan/planner.c:288`,
which calls `subquery_planner` (`planner.c:629`), which in turn calls
`grouping_planner` (`planner.c:1335`). `grouping_planner` calls
`query_planner` at
`/home/user/work/postgres/src/backend/optimizer/plan/planmain.c:54`.
`query_planner` sets up RelOptInfos for every base relation
(`add_base_rels_to_query`, `planmain.c:170`), builds equivalence classes,
distributes restriction clauses, then calls
`make_one_rel` at `planmain.c:280`.

`make_one_rel` is defined in
`/home/user/work/postgres/src/backend/optimizer/path/allpaths.c:172`. It does
three things in order:

1. `set_base_rel_consider_startup(root)` (`allpaths.c:179`) — flag rels on
   the inner side of SEMI/ANTI joins as needing fast-start paths.
2. `set_base_rel_sizes(root)` (`allpaths.c:184` → loop body at
   `allpaths.c:291`) — for each base rel, compute row/width estimates and
   the `consider_parallel` flag, then call `set_rel_size` (`allpaths.c:361`)
   which for plain relations calls `set_plain_rel_size`
   (`allpaths.c:573`) → `set_baserel_size_estimates`.
3. `set_base_rel_pathlists(root)` (`allpaths.c:334`) — iterate base rels
   and dispatch to `set_rel_pathlist` (`allpaths.c:470`).

`set_rel_pathlist` (`allpaths.c:470`) is a switch on `rel->rtekind`. For a
plain (non-foreign, non-sampled) table it calls
`set_plain_rel_pathlist(root, rel, rte)` at `allpaths.c:500`.

After dispatching, `set_rel_pathlist` calls the
`set_rel_pathlist_hook` (`allpaths.c:539`); this is the seam Topic 2
covers. It then calls `generate_useful_gather_paths` (`allpaths.c:558`)
to wrap any partial paths into Gather/GatherMerge, and finally
`set_cheapest(rel)` (`allpaths.c:561`).

ASCII diagram of the single-relation pipeline:

```
standard_planner [planner.c:288]
        |
        v
grouping_planner [planner.c:1335]
        |
        v
query_planner [planmain.c:54]
        |
        v
make_one_rel [allpaths.c:172]
        |
        +-- set_base_rel_consider_startup
        +-- set_base_rel_sizes -------------- per rel: set_rel_size,
        |                                     set_baserel_size_estimates,
        |                                     check_index_predicates
        +-- set_base_rel_pathlists
                  |
                  v
              set_rel_pathlist [allpaths.c:470]
                  |
                  +-- set_plain_rel_pathlist [allpaths.c:765]
                  |       +-- add_path(seqscan)        [allpaths.c:779]
                  |       +-- create_plain_partial_paths (parallel seq scan)
                  |       +-- create_index_paths        [indxpath.c:230]
                  |       |     +-- get_index_paths
                  |       |     |     +-- build_index_paths
                  |       |     |     +-- add_path(IndexPath)  per index
                  |       |     +-- generate_bitmap_or_paths
                  |       |     +-- choose_bitmap_and
                  |       |     +-- add_path(BitmapHeapPath)
                  |       +-- create_tidscan_paths
                  |
                  +-- (*set_rel_pathlist_hook)()  <-- Topic 2 (orioledb)
                  +-- generate_useful_gather_paths
                  +-- set_cheapest [pathnode.c:242]
```

Notes:

- For partitioned/inheritance parents (`rte->inh`) the path is
  `set_append_rel_pathlist` at `allpaths.c:480`; we do not cover that here.
- Foreign tables get `set_foreign_pathlist` (`allpaths.c:490`); FDWs install
  their own scan paths.
- Sampled relations go to `set_tablesample_rel_pathlist` (`allpaths.c:495`).

## 2. `set_plain_rel_pathlist` — Adding Candidate Paths

`set_plain_rel_pathlist` is at
`/home/user/work/postgres/src/backend/optimizer/path/allpaths.c:765`. The body
(`allpaths.c:765-790`) is short and order-revealing — it is the canonical
"list of candidate scan kinds" for a single relation:

```c
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    Relids      required_outer;

    required_outer = rel->lateral_relids;

    if (!set_plain_rel_pathlist_hook ||
        set_plain_rel_pathlist_hook(root, rel, rte))
        /* Consider sequential scan */
        add_path(rel, create_seqscan_path(root, rel, required_outer, 0));

    /* If appropriate, consider parallel sequential scan */
    if (rel->consider_parallel && required_outer == NULL)
        create_plain_partial_paths(root, rel);

    /* Consider index scans */
    create_index_paths(root, rel);

    /* Consider TID scans */
    create_tidscan_paths(root, rel);
}
```

Order of candidate generation:

1. **Seq Scan** is always considered first (`allpaths.c:779`). Its path is
   built by `create_seqscan_path` in
   `/home/user/work/postgres/src/backend/optimizer/util/pathnode.c`, which
   calls `cost_seqscan`. A plain table without any indexes will end the
   pipeline with the seqscan path as the only candidate.
2. **Parallel Seq Scan**, when `rel->consider_parallel` is set and the
   rel has no lateral-required outer rels, via
   `create_plain_partial_paths` (`allpaths.c:796`). This calls
   `compute_parallel_worker` and adds a partial path with
   `add_partial_path` (`allpaths.c:809`); it is *partial*, not yet
   wrapped in Gather — that happens later in
   `generate_useful_gather_paths`.
3. **Index Scan / Index Only Scan / Bitmap Index Scan** via
   `create_index_paths(root, rel)`
   (`/home/user/work/postgres/src/backend/optimizer/path/indxpath.c:230`).
4. **TID Scan** via `create_tidscan_paths` (`tidpath.c`); only adds a
   path when a WHERE clause references `ctid = …`.

There are two hooks visible in this function:

- `set_plain_rel_pathlist_hook` — declared at
  `allpaths.c:86`. If installed, it can suppress the default seqscan path
  by returning false. It runs *before* index/TID consideration.
- `set_rel_pathlist_hook` — invoked by the caller `set_rel_pathlist`
  at `allpaths.c:539`, *after* every default path has been added.
  Plugins (orioledb, citus, etc.) typically use this seam to add
  custom paths or replace the seqscan with their own. Topic 2 covers
  orioledb's use of this hook.

Inside `create_index_paths` (indxpath.c:230) the per-index loop is at
`indxpath.c:249`:

```
for each index in rel->indexlist:
    if index->indpred != NIL && !index->predOK: skip partial index
    match_restriction_clauses_to_index → rclauseset
    get_index_paths(root, rel, index, &rclauseset, &bitindexpaths)
        |  +-- build_index_paths (indxpath.c:799)
        |        considers forward + (optionally) backward scans
        |        decides index_only_scan via check_index_only
        |        builds zero or more IndexPaths
        |  +-- for each: if amhasgettuple -> add_path
        |  +-- for each: if amhasgetbitmap -> append to bitindexpaths
    match_join_clauses_to_index   (for parameterized paths)
    match_eclass_clauses_to_index
    consider_index_join_clauses   (parameterized variants)

generate_bitmap_or_paths(rel->baserestrictinfo)
bitindexpaths += those OR results

if bitindexpaths != NIL:
    bitmapqual = choose_bitmap_and(rel, bitindexpaths)   [indxpath.c:1711]
    bpath = create_bitmap_heap_path(root, rel, bitmapqual, …)
    add_path(rel, bpath)                                 [indxpath.c:339]
    if rel->consider_parallel: create_partial_bitmap_paths
```

So at the end of `set_plain_rel_pathlist`, `rel->pathlist` may contain
(some subset of):

- 1 unparameterized Seq Scan path
- 0..1 partial Seq Scan paths (in `rel->partial_pathlist`)
- 0..K IndexPaths, one per usable (index, scan-direction) pair, possibly
  flagged as Index Only Scan (`IndexPath.path.pathtype == T_IndexOnlyScan`)
- 0..1 BitmapHeapPath from restriction clauses
- 0..M parameterized IndexPaths and 0..M parameterized BitmapHeapPaths
  for join clauses
- 0..1 TIDPath

## 3. The `pathlist` and the Path Node

Each candidate plan is a `Path` (subclasses: `IndexPath`, `BitmapHeapPath`,
`BitmapAndPath`, `BitmapOrPath`, `TidPath`, `AppendPath`, …) declared in
`/home/user/work/postgres/src/include/nodes/pathnodes.h`. The key fields the
optimizer compares paths on are:

- `pathtype` — the executor node tag (`T_SeqScan`, `T_IndexScan`,
  `T_IndexOnlyScan`, `T_BitmapHeapScan`, etc.).
- `startup_cost` — cost to produce the first output row.
- `total_cost` — cost to read every row.
- `rows` — estimated number of output rows (after baserestrictinfo).
- `pathkeys` — list of `PathKey` describing the sort order the path
  produces. A Seq Scan has `pathkeys == NIL`; an Index Scan over a btree
  with no order-by `DESC` has the index's natural order.
- `parallel_aware` / `parallel_safe` / `parallel_workers`.
- `param_info` (NULL for unparameterized paths). When non-NULL the path
  is "inner of nestloop with these outer relids", carrying its own
  `ppi_rows` (post-join-clauses row estimate) and the param-conditional
  clauses.

`rel->pathlist` is a list of unparameterized + parameterized paths kept
sorted by `total_cost` ascending. The list is *never* the full Cartesian
product of all possible paths — `add_path` enforces Pareto dominance, so
only the candidates that win on *some* (cost, sort order, parallel-safety,
rows, parameterization) axis survive.

The `RelOptInfo` also has:

- `partial_pathlist` — partial paths (parallel-worker output, not yet
  Gathered). Added by `add_partial_path`
  (`/home/user/work/postgres/src/backend/optimizer/util/pathnode.c:747`).
- `cheapest_startup_path`, `cheapest_total_path`,
  `cheapest_unique_path`, `cheapest_parameterized_paths` — filled in
  by `set_cheapest` (see §5).

## 4. `add_path` — Pareto Dominance and Fuzzy Comparison

`add_path` is at
`/home/user/work/postgres/src/backend/optimizer/util/pathnode.c:420`. The
contract is:

> A path is worthy if it has a better sort order (better pathkeys) or
> cheaper cost (on either dimension), or generates fewer rows, than any
> existing path that has the same or superset parameterization rels.
> We also consider parallel-safe paths more worthy than others.

`add_path` is dominance pruning, not just insertion. Each new candidate
is compared pairwise against every surviving old path
(`pathnode.c:441` `foreach(p1, parent_rel->pathlist)`):

1. **Fuzzy cost compare** via `compare_path_costs_fuzzily`
   (`pathnode.c:164`) using `STD_FUZZ_FACTOR` (`pathnode.c:43`, set to
   1.01). The comparison returns one of:
   - `COSTS_EQUAL` — both startup and total within 1% of each other.
   - `COSTS_BETTER1` — new path dominates on both cost axes (within fuzz).
   - `COSTS_BETTER2` — old path dominates on both cost axes.
   - `COSTS_DIFFERENT` — paths disagree on which dimension wins (e.g. new
     has lower startup, old has lower total). Keep both.

2. **Pathkeys compare** via `compare_pathkeys`. Returns
   `PATHKEYS_EQUAL`, `PATHKEYS_BETTER1` (new is at-least-as-strong),
   `PATHKEYS_BETTER2`, or `PATHKEYS_DIFFERENT` (incomparable, keep both).
   Parameterized paths are treated as having NIL pathkeys
   (`pathnode.c:434`) — the planner does not use parameterized paths to
   provide ordering.

3. **Parameterization** via `bms_subset_compare(PATH_REQ_OUTER(new),
   PATH_REQ_OUTER(old))`. A path with a *subset* parameterization
   dominates if it also wins on cost+rows+pathkeys+parallel-safety
   (`pathnode.c:479-543`). A path with a *superset* parameterization is
   only kept if it produces strictly fewer rows.

4. **rows** and **parallel_safe** are tiebreakers (`pathnode.c:485`,
   `pathnode.c:486`, `pathnode.c:493-494`, etc.).

For `COSTS_EQUAL` + `PATHKEYS_EQUAL` + same parameterization the
deterministic tiebreak chain is (`pathnode.c:516-532`):
parallel_safe > rows < smaller > fuzzy total_cost (1.0000000001 factor) >
arbitrarily keep old.

The list `parent_rel->pathlist` is kept ordered by `total_cost`
ascending (`pathnode.c:597-599`). Cheap-first ordering is exploited by
`add_path_precheck` (`pathnode.c:642`) so that an obviously-worse
candidate can be rejected without ever being fully costed.

Discarded `Path` nodes are immediately `pfree`'d
(`pathnode.c:591-592`), *except* `IndexPath` nodes — they may be
referenced as children of `BitmapHeapPath` and so cannot be freed
(`pathnode.c:591`). This subtlety matters for any code that subclasses
`Path` and is registered via `set_rel_pathlist_hook`: returning a
"keepable as bitmap child" path requires the IndexPath escape hatch.

## 5. `set_cheapest` — Picking the Winner(s)

`set_cheapest` is at
`/home/user/work/postgres/src/backend/optimizer/util/pathnode.c:242`. It is
called once per rel at the end of `set_rel_pathlist`
(`allpaths.c:561`) and once per join rel after join paths have been
generated. It populates four fields on the `RelOptInfo`:

- `cheapest_startup_path` — best `startup_cost` among unparameterized
  paths (`pathnode.c:327-332`). If two are tied on cost, the one with
  the *strictly stronger* pathkeys wins (`PATHKEYS_BETTER2` test at
  `pathnode.c:330`).
- `cheapest_total_path` — best `total_cost` among unparameterized
  paths (`pathnode.c:334-339`), tie-broken the same way.
- `cheapest_unique_path` — left `NULL` here, filled in lazily by
  `create_unique_path` if a SEMI/UNIQUE plan needs it
  (`pathnode.c:357`).
- `cheapest_parameterized_paths` — the cheapest path *per distinct
  minimum parameterization* plus the unparameterized cheapest-total
  (`pathnode.c:262-309`, final `lcons` at `pathnode.c:345`).

A single loop over `parent_rel->pathlist` (`pathnode.c:258-341`)
splits paths into parameterized vs unparameterized buckets. For the
parameterized bucket the comparison logic is more complex because we
must track the *minimum parameterization* using `bms_subset_compare`:

- `BMS_EQUAL` (same parameterization): keep cheaper by `TOTAL_COST`
  (`pathnode.c:289`).
- `BMS_SUBSET1`: new path is less-parameterized — replace
  (`pathnode.c:293`).
- `BMS_SUBSET2`: old path is less-parameterized — keep it
  (`pathnode.c:297`).
- `BMS_DIFFERENT`: incomparable, keep old until something better arrives
  (`pathnode.c:300`).

If there are no unparameterized paths at all (rare for a baserel,
common for an inner rel where parameterization is mandatory),
`cheapest_total_path` falls back to the best parameterized path
(`pathnode.c:351-352`); `cheapest_startup_path` stays NULL in that
case.

`set_cheapest` will `elog(ERROR, "could not devise a query plan for
the given query")` if `pathlist` is empty (`pathnode.c:252-253`).

## 6. Cost Model Constants and the GUCs That Tune Them

PostgreSQL's cost model is in *abstract cost units*. By convention 1 unit
= "the cost of reading one sequential disk page from a warm cache". The
factory defaults are at
`/home/user/work/postgres/src/include/optimizer/cost.h:24-34`:

```
DEFAULT_SEQ_PAGE_COST          1.0
DEFAULT_RANDOM_PAGE_COST       4.0
DEFAULT_CPU_TUPLE_COST         0.01
DEFAULT_CPU_INDEX_TUPLE_COST   0.005
DEFAULT_CPU_OPERATOR_COST      0.0025
DEFAULT_EFFECTIVE_CACHE_SIZE   524288   (pages = 4 GiB at 8 KiB/page)
```

The runtime GUCs that hold them are at
`/home/user/work/postgres/src/backend/optimizer/path/costsize.c:119-128`:

```
double  seq_page_cost          = DEFAULT_SEQ_PAGE_COST;       // costsize.c:119
double  random_page_cost       = DEFAULT_RANDOM_PAGE_COST;    // costsize.c:120
double  cpu_tuple_cost         = DEFAULT_CPU_TUPLE_COST;      // costsize.c:121
double  cpu_index_tuple_cost   = DEFAULT_CPU_INDEX_TUPLE_COST;// costsize.c:122
double  cpu_operator_cost      = DEFAULT_CPU_OPERATOR_COST;   // costsize.c:123
int     effective_cache_size   = DEFAULT_EFFECTIVE_CACHE_SIZE;// costsize.c:128
```

Per-tablespace overrides exist; `cost_seqscan` resolves them via
`get_tablespace_page_costs` (`costsize.c:308`), so a tablespace on SSD
can be made cheap-random without globally lowering `random_page_cost`.

The `enable_*` GUCs are declared at
`/home/user/work/postgres/src/backend/optimizer/path/costsize.c:134-137`:

```
bool  enable_seqscan        = true;   // costsize.c:134
bool  enable_indexscan      = true;   // costsize.c:135
bool  enable_indexonlyscan  = true;   // costsize.c:136
bool  enable_bitmapscan     = true;   // costsize.c:137
```

Setting one of them to `off` does **not** remove the corresponding path.
Instead, the cost function adds `disable_cost = 1.0e10`
(`costsize.c:130`) to `startup_cost`. The effect is:

- `cost_seqscan` (`costsize.c:304-305`): `if (!enable_seqscan)
  startup_cost += disable_cost;`
- `cost_index` (`costsize.c:606-607`): `if (!enable_indexscan)
  startup_cost += disable_cost;`. Index-only scans don't get an
  additional charge here; `indxpath.c` rejects them upstream via
  `check_index_only` (see §9).
- `cost_bitmap_heap_scan` (`costsize.c:1041-1042`): `if
  (!enable_bitmapscan) startup_cost += disable_cost;`.

Because the penalty is finite, the planner *will* still pick a disabled
path if every alternative is also disabled or if no alternative exists.
This is intentional — the planner promises a plan exists, not a "good"
plan. It also means `EXPLAIN` of a `SET enable_seqscan = off` query that
still picks Seq Scan will show a `total_cost` ≥ 1e10, which is the
standard sniff test that the seqscan really was the only choice.

Two consequences for bug investigations:

1. Forcing a specific scan node with `enable_*` GUCs is *biased*, not
   absolute. The planner can still cross over if the penalty doesn't
   cover the cost gap (it does in practice).
2. When comparing `EXPLAIN ANALYZE` between with-vs-without an
   `enable_*` knob, the cost numbers shift by `1e10` per disabled
   path, but the row estimates and the `cheapest_total_path` shape do
   not.

## 7. `cost_seqscan`

`cost_seqscan` is at
`/home/user/work/postgres/src/backend/optimizer/path/costsize.c:284`. The
model is dirt-simple — read every page sequentially, evaluate the
restriction quals on every tuple.

The formula, distilled from `costsize.c:284-351`:

```
disk_run_cost = spc_seq_page_cost * baserel->pages              [costsize.c:315]
cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple          [costsize.c:321]
cpu_run_cost  = cpu_per_tuple   * baserel->tuples               [costsize.c:322]

startup_cost  = qpqual_cost.startup + pathtarget->cost.startup
total_cost    = startup_cost
              + cpu_run_cost
              + disk_run_cost
              + pathtarget->cost.per_tuple * path->rows
              + (disable_cost if !enable_seqscan)
```

Key field provenance:

- `baserel->pages` and `baserel->tuples` come from `get_relation_info` →
  `estimate_rel_size` (see §11). For a never-vacuumed table both are
  clamped (`tableam.c:717-720` for pages → 10; `tuples` is then derived
  from row width).
- `qpqual_cost` is the per-tuple cost of evaluating the qual list. It
  is computed by `get_restriction_qual_cost` (`costsize.c:318`), which
  walks the restriction RestrictInfo list and sums `cost_qual_eval`.
- `path->pathtarget->cost` is the cost of expressions in the SELECT
  list / projection target (e.g. `f(a) + g(b)` on top of the scan).
  Startup cost is amortized once; per-tuple is charged per output row.
- For parallel paths, `cpu_run_cost` is divided by
  `get_parallel_divisor(path)` (`costsize.c:333`) and `path->rows` is
  scaled to per-worker output (`costsize.c:346`). `disk_run_cost` is
  *not* divided — the assumption is the OS prefetcher does the
  parallelism for free, so each worker still pays the full disk-read
  cost.

What `cost_seqscan` deliberately does *not* model:

- TOAST detoasting cost (paid implicitly via `cpu_operator_cost` in
  the qual evaluation).
- The "first page is random, subsequent are sequential" effect — Seq
  Scan charges `seq_page_cost` for every page.
- HOT-chain following or visibility-map checks on the heap.

## 8. `cost_index`, `amcostestimate`, and `btcostestimate`

`cost_index` is at
`/home/user/work/postgres/src/backend/optimizer/path/costsize.c:549`. It is
the engine of Index Scan vs Index Only Scan cost — the same function
handles both (`indexonly = (path->path.pathtype == T_IndexOnlyScan)` at
`costsize.c:554`) and only branches when accounting for heap fetches.

The function works in four phases.

### 8.1 Delegate to the AM's `amcostestimate`

`cost_index` calls the index's per-AM cost callback
(`costsize.c:617-621`):

```c
amcostestimate = (amcostestimate_function) index->amcostestimate;
amcostestimate(root, path, loop_count,
               &indexStartupCost, &indexTotalCost,
               &indexSelectivity, &indexCorrelation,
               &index_pages);
```

The callback signature is at
`/home/user/work/postgres/src/include/access/amapi.h:172-179`:

```c
typedef void (*amcostestimate_function) (struct PlannerInfo *root,
                                         struct IndexPath *path,
                                         double loop_count,
                                         Cost *indexStartupCost,
                                         Cost *indexTotalCost,
                                         Selectivity *indexSelectivity,
                                         double *indexCorrelation,
                                         double *indexPages);
```

It is wired into `IndexAmRoutine.amcostestimate` at `amapi.h:318`.
Every AM (btree, hash, gist, gin, brin, hnsw, …) supplies one.

Outputs the AM owes:

- `indexStartupCost` — cost up to producing the first index tuple
  (e.g. for btree this includes ~log2(N) comparisons to descend the
  tree).
- `indexTotalCost` — cost to produce all matching index tuples,
  *index-side only* (i.e. excludes any heap I/O cost).
- `indexSelectivity` — fraction of the table's tuples that match the
  index quals. Used by the caller to compute heap fetches.
- `indexCorrelation` — Pearson correlation between physical index
  order and physical heap order; in `[-1, +1]`. Drives the
  random-vs-sequential interpolation for heap fetches.
- `indexPages` — number of *index* pages assumed to be touched; used
  by the planner to bill against `effective_cache_size` and to pick
  parallel-worker counts for partial paths.

### 8.2 `btcostestimate` (the canonical implementation)

`btcostestimate` is at
`/home/user/work/postgres/src/backend/utils/adt/selfuncs.c:6874`.

It walks `path->indexclauses` looking for "boundary quals" — leading
`=` clauses plus the immediately next column's inequality clauses
(`selfuncs.c:6911-6992`). Only these contribute to *narrowing the index
range*; additional non-boundary quals can suppress heap visits but they
do not move the index endpoints.

Short-circuit for unique indexes (`selfuncs.c:7000-7005`):

```c
if (index->unique &&
    indexcol == index->nkeycolumns - 1 &&
    eqQualHere &&
    !found_saop &&
    !found_is_null_op)
    numIndexTuples = 1.0;
```

i.e. an equality probe on every column of a unique index always yields
1 tuple, regardless of stats. This is the planner's first-line
"primary-key lookup" recognizer.

Otherwise it calls
`clauselist_selectivity(root, selectivityQuals, index->rel->relid,
JOIN_INNER, NULL)` (`selfuncs.c:7018`) and multiplies by
`index->rel->tuples` (`selfuncs.c:7022`) to get `numIndexTuples`.

It then calls `genericcostestimate`
(`/home/user/work/postgres/src/backend/utils/adt/selfuncs.c:6630`,
invoked at `selfuncs.c:7076`) to do the bulk of the work:

- `numIndexPages = ceil(numIndexTuples * index->pages / index->tuples)`
  (`selfuncs.c:6733`). Pro-rata estimate of leaf pages.
- For a single scan: `indexTotalCost = numIndexPages *
  spc_random_page_cost` (`selfuncs.c:6789`). For repeated scans
  (loop_count > 1 or SAOP), use the Mackert-Lohman formula via
  `index_pages_fetched` to amortize cache hits.
- CPU: `indexTotalCost += numIndexTuples * num_sa_scans *
  (cpu_index_tuple_cost + qual_op_cost)` (`selfuncs.c:6813`).
- `indexCorrelation = 0.0` from `genericcostestimate`
  (`selfuncs.c:6818`).

After `genericcostestimate` returns, `btcostestimate` adds:

- **Descent cost**: `ceil(log(index->tuples) / log(2.0)) *
  cpu_operator_cost` once (`selfuncs.c:7091`). Plus
  `num_sa_scans` × the same charge added to total
  (`selfuncs.c:7093`).
- **Per-page descent CPU**: `(index->tree_height + 1) *
  DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost`
  (`selfuncs.c:7107`). This is intentionally non-zero so a bloated
  index looks slower even when all its work is cached.
- **Index correlation** from `pg_statistic` (`selfuncs.c:7119-7204`):
  fetches the `STATISTIC_KIND_CORRELATION` slot for the leading index
  column via `SearchSysCache3(STATRELATTINH, …)`. For multi-column
  indexes the correlation is multiplied by 0.75 (`selfuncs.c:7199`).

### 8.3 Heap I/O cost (back in `cost_index`)

After getting `indexSelectivity` from the AM, `cost_index` estimates
heap fetches:

```
tuples_fetched = clamp_row_est(indexSelectivity * baserel->tuples)   [costsize.c:636]
```

It then computes two bracket estimates,
`max_IO_cost` (perfectly uncorrelated) and `min_IO_cost` (perfectly
correlated), and interpolates by `csquared = indexCorrelation^2`
(`costsize.c:785-787`):

```
run_cost += max_IO_cost + csquared * (min_IO_cost - max_IO_cost);
```

- **Uncorrelated branch** (`costsize.c:720-731`):
  `pages_fetched = index_pages_fetched(tuples_fetched, baserel->pages,
  index->pages, root)` then
  `max_IO_cost = pages_fetched * spc_random_page_cost`.
  `index_pages_fetched` (`costsize.c:898`) is the Mackert-Lohman
  finite-LRU formula — given the size of the table, the size of the
  caches assumed available (`effective_cache_size` pro-rated), and the
  number of tuples to fetch, what's the expected number of distinct
  pages we'll actually touch.
- **Correlated branch** (`costsize.c:734-746`):
  `pages_fetched = ceil(indexSelectivity * baserel->pages)`, then
  `min_IO_cost = spc_random_page_cost + (pages_fetched - 1) *
  spc_seq_page_cost`. The first page is "random" because we still seek
  to it; subsequent pages are sequential because of perfect correlation.

`csquared` (correlation squared) interpolation is the planner's
crude proxy for "how clustered is this index". A freshly-CLUSTERed
table sees `csquared ≈ 1` and pays nearly `min_IO_cost`; a hash-shuffled
loaded table sees `csquared ≈ 0` and pays nearly `max_IO_cost` (often a
factor of 4 worse at default `random_page_cost`).

### 8.4 CPU + parallelism

Final accounting (`costsize.c:795-820`):

```
cost_qual_eval(&qpqual_cost, qpquals, root)
cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple
cpu_run_cost += cpu_per_tuple * tuples_fetched

if parallel_workers > 0:
    path->rows /= parallel_divisor
    cpu_run_cost /= parallel_divisor

path->startup_cost = startup_cost
path->total_cost   = startup_cost + run_cost
```

`qpquals` is the subset of restriction clauses that the index machinery
*cannot* enforce, computed by `extract_nonindex_conditions`
(`costsize.c:840`) — clauses already implied by the index clauses are
dropped, the rest become a filter on top of the scan.

## 9. `cost_index_only_scan` Specifics and `check_index_only`

There is no separate `cost_index_only_scan` symbol — Index Only Scan is
the same code path as Index Scan in `cost_index`, gated by the `indexonly`
flag at `costsize.c:554`. The only difference is the `pages_fetched`
adjustment in three spots (`costsize.c:686`, `costsize.c:710`,
`costsize.c:726`, `costsize.c:737`):

```c
if (indexonly)
    pages_fetched = ceil(pages_fetched * (1.0 - baserel->allvisfrac));
```

`baserel->allvisfrac` is the fraction of heap pages that the visibility
map flags as "all tuples visible to all current transactions". It is
populated by `estimate_rel_size` from `pg_class.relallvisible /
pg_class.relpages`
(`/home/user/work/postgres/src/backend/optimizer/util/plancat.c:1149-1160`).
For an Index Only Scan, every heap page that *is* all-visible can be
answered straight from the index — only the `1 - allvisfrac` fraction
costs `spc_random_page_cost` per page. A perfectly all-visible table
(after a recent `VACUUM`) gives `allvisfrac == 1.0` and the heap I/O
component vanishes entirely; `EXPLAIN ANALYZE` reports `Heap Fetches: 0`
in that case.

### `check_index_only` — Can the Scan Actually Be Index-Only?

`check_index_only` is at
`/home/user/work/postgres/src/backend/optimizer/path/indxpath.c:2154`. It
runs in `build_index_paths` at `indxpath.c:948` (called as
`index_only_scan = (scantype != ST_BITMAPSCAN && check_index_only(rel,
index));`). Its job is to verify two things:

1. **GUC** (`indxpath.c:2163-2164`): `if (!enable_indexonlyscan)
   return false;`. This is where the IOS-disabled knob is enforced —
   the path is never even built, no `disable_cost` fudge is needed.
2. **Attribute coverage** (`indxpath.c:2176-2218`): the union of all
   attrs needed by the query (from `rel->reltarget->exprs` + every
   restriction RestrictInfo) must be a subset of the attrs the index
   can return.

Attribute coverage is computed by `pull_varattnos`
(`indxpath.c:2176, 2193`) into a bitmapset, and compared against
`index->canreturn[i]` per column (`indxpath.c:2200-2215`). The
`canreturn` array is filled when the index is loaded into the
RelOptInfo by `get_relation_info` querying the AM's `amcanreturn`
callback. For btree this returns true for every key column; for
included columns (`CREATE INDEX … INCLUDE (…)`) it also returns true.

Final check (`indxpath.c:2218`):

```c
result = bms_is_subset(attrs_used, index_canreturn_attrs);
```

If the index covers every needed column, the IndexPath is built with
`pathtype = T_IndexOnlyScan`; otherwise it's `T_IndexScan`. Both go
into `rel->pathlist` and compete via `add_path` like everything else.

### Practical fingerprint

`EXPLAIN` output:

- `Index Scan` — heap fetch on every matching tuple.
- `Index Only Scan` with `Heap Fetches: 0` — pure index walk (after
  `VACUUM`).
- `Index Only Scan` with `Heap Fetches: N > 0` — IOS chosen by the
  planner, but at runtime some heap pages weren't all-visible and had
  to be fetched. Cost model expected `(1 - allvisfrac) * N` such
  fetches; if the value is wildly off, suspect a stale visibility map.

## 10. `cost_bitmap_heap_scan`, `generate_bitmap_or_paths`, `choose_bitmap_and`

Bitmap scans are a two-stage idea: walk one or more indexes to build a
TID bitmap, then walk the heap in *physical* order, fetching only pages
that have a TID in the bitmap. This wins when (a) there are several
indexes whose intersection prunes the heap a lot or (b) the index is
correlated badly enough that a plain Index Scan would do random heap
fetches anyway and you'd prefer to amortize them.

### `cost_bitmap_heap_scan`

Defined at
`/home/user/work/postgres/src/backend/optimizer/path/costsize.c:1013`.
The cost model (`costsize.c:1041-1106`):

```
pages_fetched = compute_bitmap_pages(...)       // Mackert-Lohman based
startup_cost += indexTotalCost                  // from the bitmapqual subtree
T = baserel->pages

// Per-page cost interpolates between random (for small fetched
// sets) and sequential (when nearly the whole table is touched):
if pages_fetched >= 2:
    cost_per_page = random - (random - seq) * sqrt(pages_fetched / T)
else:
    cost_per_page = random

run_cost += pages_fetched * cost_per_page       [costsize.c:1070]

// CPU costs assume every retrieved tuple has its quals rechecked:
cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple
cpu_run_cost  = cpu_per_tuple * tuples_fetched

if !enable_bitmapscan: startup_cost += disable_cost   [costsize.c:1041]
```

Two cost-model quirks worth highlighting:

- **Recheck assumed always**: comment at `costsize.c:1074-1080`. The
  cost charges the full qual evaluation for every tuple. In practice
  recheck is only needed when the bitmap becomes lossy (one bit per
  page rather than per tuple), but accounting for that is too
  fiddly so the planner over-charges.
- **`cost_per_page` interpolation**: at low selectivity, fetches look
  random; at high selectivity, the bitmap's page-ordered access is
  nearly sequential. The square-root interpolation is intentional —
  the cost-per-page falls off slower than the page-fetched fraction
  rises.

### `generate_bitmap_or_paths`

Defined at `indxpath.c:1556`. For each top-level `OR` clause whose
arms are each independently indexable, this builds a `BitmapOrPath`.
Without this step, an `OR` would defeat indexing entirely because
neither arm alone matches a single index.

### `choose_bitmap_and`

Defined at `indxpath.c:1711`. Given a non-empty list of bitmap inputs
(indexpaths and `BitmapOrPath` outputs), it constructs a
`BitmapAndPath` (or returns a single input if only one survives).

Comments at `indxpath.c:1727-1775` lay out the heuristic clearly:

- A full Pareto-optimal enumeration would be `O(2^N)` and is
  abandoned.
- Step 1: collapse paths that use the *exact same set* of WHERE
  clauses + predicate conditions, keeping the cheapest per group
  (`indxpath.c:1778-1797`). This kills "near-duplicate" indexes that
  cover the same columns plus irrelevant extras.
- Step 2: sort surviving inputs by `total_cost`, then greedy walk —
  start with cheapest, add each subsequent path *only if* total bitmap
  cost decreases (`O(N^2)`).
- Reject combinations where two indexes share a clause
  (`indxpath.c:1751-1762`), to avoid double-counted selectivity.
- Reject partial-index inputs whose predicate is implied by clauses
  already selected (`indxpath.c:1763-1774`).

The output goes into `add_path(rel, BitmapHeapPath)` at
`indxpath.c:339` (for restriction clauses) or `indxpath.c:406` (per
join parameterization).

## 11. Where the Statistics Come From

Two catalogs feed everything in §7-10:

- `pg_class.relpages`, `pg_class.reltuples`, `pg_class.relallvisible` —
  *per-relation* sizes, refreshed by `ANALYZE`, `VACUUM`, `VACUUM
  ANALYZE`, and `CREATE INDEX`.
- `pg_statistic` — *per-column* statistical summaries, populated by
  `ANALYZE`.

### `get_relation_info` and `estimate_rel_size`

`get_relation_info` is at
`/home/user/work/postgres/src/backend/optimizer/util/plancat.c:117`. It is
called once per base rel by `build_simple_rel`. Key responsibilities:

- Set `rel->reltablespace`, `rel->min_attr/max_attr`, `notnullattnums`
  (`plancat.c:156-194`).
- Call `estimate_rel_size(relation, …, &rel->pages, &rel->tuples,
  &rel->allvisfrac)` at `plancat.c:202`.
- For each index, build an `IndexOptInfo` (`plancat.c:280` … the loop
  body). Per-index page/tuple counts are computed at
  `plancat.c:476-484`: for a non-partial index, `info->pages =
  RelationGetNumberOfBlocks(indexRelation)` and `info->tuples =
  rel->tuples`. For a partial index, `estimate_rel_size` is called on
  the index relation and the result is clamped to ≤ `rel->tuples`.
- For btree indexes, `info->tree_height = _bt_getrootheight()`
  (`plancat.c:495`) — used in `btcostestimate` for the per-page CPU
  charge.
- Finally fire `get_relation_info_hook` (`plancat.c:576`). This is the
  *catalog-side* hook — distinct from `set_rel_pathlist_hook`. It is
  what plugins use to fake hypothetical indexes (e.g. `hypopg`) or
  override row counts for tests.

`estimate_rel_size` is at `plancat.c:1066`. For a heap (any
`RELKIND_HAS_TABLE_AM(rel)`) it delegates to the AM via
`table_relation_estimate_size`
(`/home/user/work/postgres/src/backend/access/table/tableam.c:675`).
For an index it works inline (`plancat.c:1080-1160`).

### The "never-vacuumed" clamp

`table_block_relation_estimate_size` at `tableam.c:675` contains the
famous hack (`tableam.c:717-720`):

```c
if (curpages < 10 &&
    reltuples < 0 &&
    !rel->rd_rel->relhassubclass)
    curpages = 10;
```

In words: if `pg_class.reltuples` is negative (the sentinel for "never
analyzed/vacuumed") *and* the heap is currently shorter than 10 pages,
pretend it is 10 pages. The comment (`tableam.c:695-716`) is candid:

> The idea here is to avoid assuming a newly-created table is really
> small, even if it currently is, because that may not be true once
> some data gets loaded into it. Once a vacuum or analyze cycle has
> been done on it, it's more reasonable to believe the size is
> somewhat stable.

This matters for tests because: (1) `CREATE TABLE t (…); SELECT * FROM
t WHERE pk = 1;` won't unconditionally pick Seq Scan even on a table
of one page — the planner will treat the empty table as 10 pages. (2)
After `ANALYZE` the stats become *real* and the clamp disappears.

After the clamp, if `relpages > 0` and `reltuples ≥ 0` it computes
`density = reltuples / relpages` from the cached stats; otherwise it
synthesizes a density from `get_rel_data_width` and fillfactor
(`tableam.c:736-764`).

### `pg_statistic` columns

A row in `pg_statistic` per (relation, column, inh). The columns the
planner consults are declared in
`/home/user/work/postgres/src/include/catalog/pg_statistic.h`:

- `stanullfrac` — fraction of NULLs. Used by `var_eq_const` for
  `<>` negation (`selfuncs.c:452-453`) and by every selectivity
  routine that excludes NULL.
- `stawidth` — average byte width. Used to estimate tuple width
  when `attlen` is variable.
- `stadistinct` — estimated number of distinct values; if
  positive it's a count, if negative it's a fraction of `reltuples`.
  Used by `get_variable_numdistinct` (the fallback in `var_eq_const`
  at `selfuncs.c:448`).
- `stakindN`, `stanumbersN`, `stavaluesN` slots (`N = 1..5`): typed
  arrays per `STATISTIC_KIND_*`:
  - `STATISTIC_KIND_MCV` — most-common values + their frequencies.
    Used by `var_eq_const` (`selfuncs.c:350-405`).
  - `STATISTIC_KIND_HISTOGRAM` — equi-depth histogram bucket
    boundaries. Used by `scalarineqsel`
    (`selfuncs.c:581`) for `<, ≤, >, ≥` selectivity.
  - `STATISTIC_KIND_CORRELATION` — Pearson correlation of column
    order vs physical heap order. Read by `btcostestimate`
    (`selfuncs.c:7186-7204`) into `indexCorrelation`.
  - `STATISTIC_KIND_MCELEM`, `STATISTIC_KIND_DECHIST`,
    `STATISTIC_KIND_RANGE_*` — type-specific (array, range,
    multirange). Not exercised by the simple-scan cases here.

When no `pg_statistic` row exists (column never analyzed), the
selectivity functions fall through to hard-coded defaults:

- `eqsel` defaults to `DEFAULT_EQ_SEL = 0.005` (declared in
  `selfuncs.h`).
- `scalarineqsel` defaults to `DEFAULT_INEQ_SEL = 1.0 / 3.0`.
- `var_eq_const` divides 1 by an estimated `numdistinct`, where
  `numdistinct` defaults to `clamp(0.1 * relpages, 200, …)` — see
  `get_variable_numdistinct` in `selfuncs.c`.

Together these create the well-known "freshly created table picks
Seq Scan" behavior: with default selectivity 0.005 against ten clamp
pages and ten clamp tuples, every plan has comparable cost, and Seq
Scan wins on cheap startup.

## 12. Selectivity: `clauselist_selectivity`, `var_eq_const`, `var_eq_non_const`

`clauselist_selectivity` is at
`/home/user/work/postgres/src/backend/optimizer/path/clausesel.c:100`. It
turns a list of `RestrictInfo`s into a single fraction `s ∈ [0, 1]`.
The algorithm (simplified from `clausesel.c:117-355`):

1. **Single-clause fast path** (`clausesel.c:135-138`): for one clause,
   delegate straight to `clause_selectivity_ext`.
2. **Extended statistics** (`clausesel.c:144-156`): if the clauses all
   reference a single relation and that rel has `CREATE STATISTICS`
   extended-stats objects (`rel->statlist != NIL`), try
   `statext_clauselist_selectivity` first. Clauses estimated by
   extended stats are masked off (`bms_add_member(&estimatedclauses,
   listidx)`).
3. **Per-clause loop** (`clausesel.c:165-...`): for each remaining
   clause, call `clause_selectivity_ext`, multiply into running `s1`,
   special-case range-style pairs (`x > a AND x < b`) into
   `rqlist` to avoid double-counting overlap.
4. **Range merge**: after the loop, merge each (`>`, `<`) pair on the
   same Var into a single range-selectivity estimate.

The independence assumption — multiplying selectivities — is the
single biggest source of cost-model error in OLTP workloads. Extended
stats (`CREATE STATISTICS … (dependencies, ndistinct, mcv) ON …`)
exist precisely to attack this.

`clause_selectivity_ext` (`clausesel.c:684`) dispatches by clause
node tag. For `OpExpr` (the common case `x = const`, `x < const`,
…) it consults the operator's `oprrest` from `pg_operator` — which
points at the per-operator selectivity function. For `=` on most
types that is `eqsel`, which calls `var_eq_const`
(`selfuncs.c:296`) when one side is a `Const`.

### `var_eq_const` worked example

`var_eq_const` at `selfuncs.c:296`. Flow:

```
if const is NULL: return 0.0   [selfuncs.c:309-310]
nullfrac = pg_statistic.stanullfrac

# Unique-index shortcut:
if vardata.isunique && vardata.rel.tuples >= 1:
    selec = 1 / vardata.rel.tuples            [selfuncs.c:331-334]

elif statsTuple is valid and security check passes:
    if const matches an MCV entry:
        selec = pg_statistic.stanumbersN[i]   [selfuncs.c:404]
    else:
        sumcommon = sum(MCV frequencies)
        selec    = (1 - sumcommon - nullfrac) / other_distinct
                                              [selfuncs.c:418, 429]
        # clamp to ≤ smallest MCV frequency:
        if selec > smallest_MCV: selec = smallest_MCV
                                              [selfuncs.c:435-436]
else:
    selec = 1 / get_variable_numdistinct(...)
                                              [selfuncs.c:448]

if negate (i.e. <>):
    selec = 1 - selec - nullfrac
CLAMP_PROBABILITY(selec)
return selec
```

The unique-index shortcut at `selfuncs.c:331-334` is identical in
spirit to the `numIndexTuples = 1.0` shortcut in `btcostestimate`
(§8.2). Both routines independently recognize "equality probe on a
unique key" and assume the answer is 1 row.

`var_eq_non_const` at `selfuncs.c:467` is the variant used when the
right-hand side is itself a Var (e.g. join clause). It does not get to
peek at MCV — it only consults `stanullfrac`, `stadistinct` and the
`pg_statistic` MCV frequency sum.

### Where selectivity flows back to cost

- `clauselist_selectivity` of `baserestrictinfo` is folded into
  `rel->rows` by `set_baserel_size_estimates` (called from
  `set_plain_rel_size` at `allpaths.c:582`).
- `clauselist_selectivity` over an index's bound quals is used by
  `btcostestimate` to compute `numIndexTuples`
  (`selfuncs.c:7018-7022`).
- `cost_index` multiplies `indexSelectivity * baserel->tuples` to get
  `tuples_fetched` (`costsize.c:636`).
- `cost_bitmap_heap_scan` indirectly: `compute_bitmap_pages` does the
  same multiplication for the heap-fetch component.

So the chain (`pg_statistic` → `var_eq_const` →
`clauselist_selectivity` → `cost_*`) is the one path everything else in
this report depends on.

## 13. Index Only Scan vs Index Scan — the Decision Surface

There is no "vs" comparator in the planner — the choice is *emergent*
from competitive costing. Both paths are built independently and
`add_path` decides which (or both) to keep.

Recap of who builds what:

- `build_index_paths` (`indxpath.c:799`) is called once per index.
- Inside it, `index_only_scan = (scantype != ST_BITMAPSCAN &&
  check_index_only(rel, index))` (`indxpath.c:947`). If true, the
  resulting `IndexPath` is created with `T_IndexOnlyScan` and the rest
  proceeds. If false, the path is `T_IndexScan`.
- A single index produces *one or the other*, not both: there is no
  case where IOS and Index Scan paths over the same index, same
  clauses, same direction both exist in `pathlist`. The reason is that
  `check_index_only` returns either true or false for a given (rel,
  index) pair; the planner does not weigh them.
- The IOS path's `pathtype` flows into `cost_index` via the `indexonly`
  flag (`costsize.c:554`), which only affects the heap-fetch term
  via the `1 - allvisfrac` multiplier.

Implications:

1. **`enable_indexonlyscan = off` does not fall back to an Index
   Scan with the same indexquals.** It returns false from
   `check_index_only`, the IOS-capable IndexPath becomes a regular
   IndexPath, and that path goes through `cost_index` with `indexonly
   = false`. So the cost goes up by `allvisfrac * heap_io_cost` and
   the path may lose to a different competitor.
2. **A column included only via `INCLUDE (…)` is enough.** Such
   columns set `canreturn[i] = true` and `attrs_used` is satisfied,
   but they are not part of `nkeycolumns` so `btcostestimate` won't
   consider them for selectivity. They are pure "covering index"
   columns.
3. **Expression indexes are excluded from IOS attribute coverage.**
   See `indxpath.c:2207-2209`: `if (attno == 0) continue;`. An
   expression like `CREATE INDEX … ON t ((a+b))` will not satisfy
   IOS on `SELECT a+b FROM t WHERE …`.

The IOS-vs-IS competition becomes interesting only when (a) there are
two or more indexes on the same column subset where one covers the
target and one doesn't, or (b) when an IOS path competes with an
Index Scan over a *different* index that happens to be more selective.

## 14. End-to-End ASCII Walkthrough

Consider:

```sql
CREATE TABLE t (id int PRIMARY KEY, a int, b text);
CREATE INDEX t_a_idx ON t (a);
CREATE INDEX t_a_b_idx ON t (a) INCLUDE (b);
INSERT INTO t SELECT g, g % 100, repeat('x', 50) FROM generate_series(1, 1e6) g;
ANALYZE t;

SELECT b FROM t WHERE a = 42;
```

After parse-analyze the planner sees one base rel with three indexes
(implicit PK index `t_pkey`, plus `t_a_idx`, plus `t_a_b_idx`). The
single restriction is `a = 42`.

Pipeline:

```
standard_planner
  -> grouping_planner
    -> query_planner
      -> setup_simple_rel_arrays
      -> add_base_rels_to_query                       // builds RelOptInfo for `t`
      -> build_base_rel_tlists                        // attr_needed: {a, b}
      -> deconstruct_jointree                         // baserestrictinfo: [a = 42]
      -> make_one_rel
        -> set_base_rel_consider_startup
        -> set_base_rel_sizes
          -> set_rel_size -> set_plain_rel_size
            -> check_index_predicates                  // no partial indexes
            -> set_baserel_size_estimates              // rel->rows = clauselist_selectivity * tuples
        -> set_base_rel_pathlists
          -> set_rel_pathlist
            -> set_plain_rel_pathlist
              -> add_path(seqscan)                     // cost_seqscan: ~25000
              -> create_plain_partial_paths            // partial seqscan path
              -> create_index_paths
                for index = t_pkey:
                  no clause matches PK column -> skip
                for index = t_a_idx:
                  rclauseset = {a=42}
                  build_index_paths
                    check_index_only(rel, t_a_idx):
                      attrs_used = {a, b}
                      canreturn  = {a}
                      result     = false   // b not in index
                    create_index_path(T_IndexScan, ...)
                    cost_index:
                      btcostestimate:
                        boundary qual: a = 42
                        clauselist_selectivity(a=42) = 1/100   (NDV-based)
                        numIndexTuples = 10000
                        numIndexPages  ≈ 30
                        indexTotalCost ≈ 30 * random_page_cost + CPU
                      tuples_fetched = 10000
                      pages_fetched  ≈ via Mackert-Lohman
                      run_cost      += interpolated heap I/O
                      total_cost     ≈ ~9000
                  add_path(IndexPath)                  // accepted
                  also adds to bitindexpaths
                for index = t_a_b_idx:
                  rclauseset = {a=42}
                  build_index_paths
                    check_index_only(rel, t_a_b_idx):
                      attrs_used = {a, b}
                      canreturn  = {a, b}   // b is in INCLUDE
                      result     = true
                    create_index_path(T_IndexOnlyScan, ...)
                    cost_index with indexonly=true:
                      pages_fetched scaled by (1 - allvisfrac)
                      total_cost     ≈ ~2500 if allvisfrac high
                  add_path(IndexOnlyScan)              // accepted
                  also adds to bitindexpaths
                generate_bitmap_or_paths                // no OR, no-op
                choose_bitmap_and(bitindexpaths)
                  -> after equal-clause filtering, one BitmapIndexScan
                create_bitmap_heap_path
                cost_bitmap_heap_scan
                  -> pages_fetched ≈ Mackert-Lohman
                  -> total_cost    ≈ ~3500
                add_path(BitmapHeapPath)
              -> create_tidscan_paths                  // no TID clause, no-op
            -> set_rel_pathlist_hook                   // <- Topic 2
            -> generate_useful_gather_paths            // wraps partial seqscan -> Gather
            -> set_cheapest
              -> cheapest_total_path = IndexOnlyScan(t_a_b_idx)
              -> cheapest_startup_path = same
```

`EXPLAIN` for this query would show `Index Only Scan using t_a_b_idx
on t`. If you `SET enable_indexonlyscan = off`, the IOS path is never
built (`check_index_only` returns false), Index Scan over `t_a_idx`
wins, costs go up, but Seq Scan still loses.

If you also `SET enable_indexscan = off`, the Index Scan path is built
but carries `disable_cost = 1e10`. Bitmap Heap Scan wins (its
disabled-cost trigger is `enable_bitmapscan`, not `enable_indexscan`,
even though it consumes an indexpath internally).

## 15. Summary Table

Quick reference of the moving pieces. All paths are relative to
`/home/user/work/postgres/`.

| What | Where | Notes |
|------|-------|-------|
| Planner entry | `src/backend/optimizer/plan/planner.c:288` (`standard_planner`) | Top of the chain |
| Per-rel size pass | `src/backend/optimizer/path/allpaths.c:291` (`set_base_rel_sizes`) | Populates `rel->rows`, `pages`, `tuples`, `allvisfrac` before any path is costed |
| Per-rel path pass | `src/backend/optimizer/path/allpaths.c:334` (`set_base_rel_pathlists`) | Dispatches to `set_rel_pathlist` |
| Path-list builder for plain rels | `allpaths.c:765` (`set_plain_rel_pathlist`) | Order: seqscan, partial seqscan, index paths, TID |
| Seqscan cost | `src/backend/optimizer/path/costsize.c:284` (`cost_seqscan`) | `pages * seq_page_cost + tuples * cpu_per_tuple` |
| Index cost driver | `costsize.c:549` (`cost_index`) | Calls AM callback, adds heap I/O, interpolates by correlation² |
| Bitmap heap cost | `costsize.c:1013` (`cost_bitmap_heap_scan`) | sqrt-interpolated cost-per-page between random and seq |
| Mackert-Lohman | `costsize.c:898` (`index_pages_fetched`) | Cache-aware page-fetch estimator |
| Index path builder | `src/backend/optimizer/path/indxpath.c:230` (`create_index_paths`) | One loop iteration per `IndexOptInfo` |
| Per-index path builder | `indxpath.c:799` (`build_index_paths`) | Decides forward/back, IOS-or-not, plain or bitmap |
| IOS predicate | `indxpath.c:2154` (`check_index_only`) | `enable_indexonlyscan` + attr coverage |
| Bitmap-AND chooser | `indxpath.c:1711` (`choose_bitmap_and`) | Greedy O(N²) over clause-set groups |
| Bitmap-OR builder | `indxpath.c:1556` (`generate_bitmap_or_paths`) | Salvages `OR` clauses into bitmap inputs |
| `add_path` (dominance) | `src/backend/optimizer/util/pathnode.c:420` | Pareto + 1% fuzz |
| `add_partial_path` | `pathnode.c:747` | Parallel-worker paths |
| `set_cheapest` | `pathnode.c:242` | Fills cheapest_* fields |
| Path-cost compare | `pathnode.c:69` (`compare_path_costs`), `pathnode.c:164` (`compare_path_costs_fuzzily`) | Fuzz factor 1.01 |
| AM callback signature | `src/include/access/amapi.h:172-179` (`amcostestimate_function`) | 5 outputs |
| AM callback slot in routine | `src/include/access/amapi.h:318` (`IndexAmRoutine.amcostestimate`) | Stored in `IndexOptInfo->amcostestimate` |
| btree amcostestimate | `src/backend/utils/adt/selfuncs.c:6874` (`btcostestimate`) | Reference implementation; calls `genericcostestimate` |
| Generic amcostestimate | `selfuncs.c:6630` (`genericcostestimate`) | Body of work for hash/gist/gin/etc. cost callbacks |
| Per-rel info loader | `src/backend/optimizer/util/plancat.c:117` (`get_relation_info`) | Reads `pg_class`, builds `IndexOptInfo`s, fires `get_relation_info_hook` |
| Size estimator | `plancat.c:1066` (`estimate_rel_size`) + `src/backend/access/table/tableam.c:675` (`table_block_relation_estimate_size`) | The 10-page clamp for never-vacuumed lives here |
| `clauselist_selectivity` | `src/backend/optimizer/path/clausesel.c:100` | Multiplies clause selectivities with independence assumption |
| `var_eq_const` | `selfuncs.c:296` | Equality selectivity from MCV / NDV |
| `var_eq_non_const` | `selfuncs.c:467` | Equality between two Vars |
| `set_rel_pathlist_hook` | `allpaths.c:539` (call site), `src/include/optimizer/paths.h` (declaration) | The seam Topic 2 covers |
| `set_plain_rel_pathlist_hook` | `allpaths.c:86` (definition), `allpaths.c:776` (call site) | Earlier seam, before index paths |
| `get_relation_info_hook` | `plancat.c:60` (definition), `plancat.c:576` (call site) | Catalog-side editorializing |
| Disable cost constant | `costsize.c:130` (`Cost disable_cost = 1.0e10;`) | Penalty, not removal |

### Mental model for the orioledb investigation context

When you read about orioledb intercepting the planner in Topic 2 / Topic
3, hold the following invariants from this report in mind:

- Path generation is *purely cost-comparative*. Two paths over the same
  rel only compete via `add_path`. Whoever wins is whoever has the
  lowest `(startup, total)` after Pareto pruning, with sort order and
  parallel-safety as tiebreakers.
- The cost of an Index Only Scan equals the cost of an Index Scan
  *minus* `allvisfrac` heap fetches. orioledb's PK-organized storage
  inverts this: the equivalent of "fetch heap" is actually an extra PK
  walk. Any orioledb-specific cost function has to invent the analog
  of `allvisfrac` for its own scan shape, or it will systematically
  mis-cost IOS-equivalent scans.
- `pg_class.relpages` / `reltuples` / `relallvisible` are the only
  global numbers; every selectivity and every cost recomputes from
  them on every plan. Stale stats make every cost stale equally; they
  don't bias one plan over another except via `clauselist_selectivity`.
- The "freshly created table = 10 pages" clamp at `tableam.c:717` is
  *not* used by index relations or by tables with subclasses. It is
  the single biggest source of "why is the plan different on an empty
  test fixture" surprises.
- The `set_rel_pathlist_hook` runs *after* every default path has
  been added. A plugin that wants to *replace* PG's paths must
  iterate and pfree from `rel->pathlist` itself; a plugin that just
  wants to *add* paths can call `add_path` and trust the dominance
  pruner.

This concludes Topic 1 — the unmodified PG 17 baseline. Topic 2 follows
the `set_rel_pathlist_hook` callback into orioledb's own path planner.
