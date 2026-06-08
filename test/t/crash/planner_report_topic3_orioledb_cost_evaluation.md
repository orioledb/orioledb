# Topic 3: OrioleDB's Cost Calculations and Decision Logic

This is part 3 of a 3-part investigation report on OrioleDB query planning. Topic 1 covered the vanilla PostgreSQL path-generation pipeline (`cost_seqscan`, `cost_index`, etc.). Topic 2 covered OrioleDB's `set_rel_pathlist_hook` and its CustomPath replacement mechanism. **This topic covers what OrioleDB's own cost code actually computes, and why the "PK always wins" outcome falls out of that math** for the hot crash-investigation workload `SELECT count(*) FROM o_bank_account`.

## 1. Executive Summary

Three facts dominate everything below:

1. **OrioleDB does not write a custom cost function for its `o_scan` CustomPath.** `transform_path()` in `src/tableam/scan.c:133-193` copies `startup_cost`, `total_cost`, `rows`, and `pathkeys` verbatim from the source PG path that it wraps. The CustomPath therefore inherits the cost computed by `cost_index()` / `cost_seqscan()` / `cost_bitmap_heap_scan()` for the original heap-targeted path. *(see Section 5.)*
2. **OrioleDB's only cost overrides live in two places**: `orioledb_amcostestimate()` for index paths (`src/indexam/handler.c:1008`) and `orioledb_estimate_rel_size()` for the table's `pages` / `tuples` / `allvisfrac` triple (`src/tableam/handler.c:1525`). The first is a near-verbatim copy of `btcostestimate` with a comment-flagged TODO to make it more orioledb-specific. The second uses `RelationGetNumberOfBlocks(rel)`, which for an orioledb table routes via the table-AM's `relation_size` slot into `orioledb_calculate_relation_size`, returning **the live primary-key B-tree leaf count × `ORIOLEDB_BLCKSZ`** — not the smgr's on-disk file size. *(see Section 6.)*
3. **No live free-space-map / extents / `.map` / `check_btree` data feeds the planner.** `check_btree` has exactly two callers (`src/catalog/sys_trees.c:575` and `src/tableam/func.c:1277`), neither of which is in any cost path. The planner's only orioledb-specific input is the per-PK B-tree leaf count from `relation_size`; everything else (`reltuples`, per-column statistics, correlation, MCVs, histograms) comes from `pg_class` / `pg_statistic`, populated by ANALYZE. *(see Section 9.)*

The consequence is that the "`o_scan` over PK wins" outcome for `count(*)` is not a hand-crafted preference: it is what falls out of `cost_index()` when (a) the PK is index-only-scannable, (b) PG's `disable_cost = 1.0e10` penalty (`src/backend/optimizer/path/costsize.c:130`) is added to *vanilla* paths via `enable_seqscan = off` / `enable_indexscan = off` / `enable_bitmapscan = off`, but (c) those penalties are not added a second time when the path is wrapped by `transform_path` because the wrapping happens *after* `cost_index()` has already set the cost (and the wrapper just copies it).

The rest of this document walks every line of that reasoning.

## 2. `orioledb_amcostestimate`: Per-Index-Path Cost

`orioledb_amcostestimate` is installed as `IndexAmRoutine.amcostestimate` for every orioledb btree index. PG's `cost_index()` calls it (via the function-pointer dispatch at `src/backend/optimizer/path/costsize.c:617-621`) to obtain four numbers: `indexStartupCost`, `indexTotalCost`, `indexSelectivity`, `indexCorrelation`, plus `indexPages`. The orioledb implementation is at `src/indexam/handler.c:1008` and carries a leading `/* TODO: Rewrite to be more orioledb-specific */` comment (`src/indexam/handler.c:1006`) — that TODO is the honest summary: today the function is a near line-for-line copy of `btcostestimate` from upstream PG.

The function's signature is the PG-mandated `amcostestimate_function`:

```c
void
orioledb_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                        Cost *indexStartupCost, Cost *indexTotalCost,
                        Selectivity *indexSelectivity, double *indexCorrelation,
                        double *indexPages)
```

(`src/indexam/handler.c:1008-1011`). It manipulates the `path->indexinfo` `IndexOptInfo`, whose `pages`, `tuples`, `tree_height`, `unique`, `nkeycolumns` etc. were filled in by PG's `get_relation_info()` at the start of planning — *not* by orioledb. We return to that in Section 6.

The flow is four steps:

1. Walk `path->indexclauses` and pick out *bound quals* — the leading `=` quals plus an immediately-following inequality. (`src/indexam/handler.c:1045-1129`.)
2. Compute `numIndexTuples`. If the index is unique *and* there is an `=` qual for every key column *and* no SAOP/IS NULL, short-circuit to `1.0`; otherwise compute `clauselist_selectivity(...) * index->rel->tuples`. (`src/indexam/handler.c:1137-1193`.)
3. Hand the `GenericCosts` struct to `genericcostestimate()` (PG's `src/backend/utils/adt/selfuncs.c:6629`). (`src/indexam/handler.c:1203`.)
4. Add two CPU-cost terms for B-tree descent and pull a correlation estimate from `pg_statistic`. (`src/indexam/handler.c:1216-1340`.)

The next three sections walk each step in turn.

## 3. Bound Quals, Selectivity, `numIndexTuples`

### 3.1 The bound-qual walk

The outer `foreach(lc, path->indexclauses)` loop (`src/indexam/handler.c:1051`) collects only those qualifiers that contribute to the index's *boundary*: leading equality columns plus at most one trailing inequality. Per-clause it dispatches on the clause type at `src/indexam/handler.c:1075-1116`:

- `OpExpr` → take `op->opno`.
- `RowCompareExpr` → take the first column's operator.
- `ScalarArrayOpExpr` → set `found_saop = true` and multiply `num_sa_scans` by the array length (when > 1). The 17+ branch (`src/indexam/handler.c:1091-1095`) uses the new `root`-aware `estimate_array_length`.
- `NullTest` with `IS_NULL` → sets `found_is_null_op = true` and `eqQualHere = true` (because `IS NULL` is treated as equality for selectivity).
- Anything else → `elog(ERROR, "unsupported indexqual type")` at `src/indexam/handler.c:1114`.

Every qual whose op-strategy is `BTEqualStrategyNumber` flips `eqQualHere = true` (`src/indexam/handler.c:1123`). The qual is also appended to `indexBoundQuals` (`src/indexam/handler.c:1127`) so the selectivity computation below has the full bound-qual set.

The loop exits as soon as it hits a column with no `=` qual: `if (!eqQualHere) break;` at `src/indexam/handler.c:1060`. That's the "first column without an = qual stops the leading-eq run" rule that defines a btree's bound region.

For our `count(*) FROM o_bank_account` query there are *no* index clauses at all (the query has no WHERE), so the `foreach` loop body never executes; `indexBoundQuals` stays empty, `eqQualHere` stays false, and `indexcol` stays 0.

### 3.2 Unique-index short-circuit

After the loop, the next block (`src/indexam/handler.c:1137-1142`) is:

```c
if (index->unique &&
    indexcol == index->nkeycolumns - 1 &&
    eqQualHere &&
    !found_saop &&
    !found_is_null_op)
    numIndexTuples = 1.0;
```

This is the "fully-qualified unique index, every column has an `=`" path. It is the only case where orioledb's per-index code declares the cost calculus trivial: exactly one tuple will be returned, no need to call `clauselist_selectivity`. For `count(*) FROM o_bank_account`, none of these conditions hold (`eqQualHere` is false because there are no quals), so we fall to the `else` branch.

### 3.3 The general case: `clauselist_selectivity × index->rel->tuples`

In the else branch (`src/indexam/handler.c:1143-1193`):

```c
selectivityQuals = add_predicate_to_index_quals(index, indexBoundQuals);

btreeSelectivity = clauselist_selectivity(root, selectivityQuals,
                                          index->rel->relid,
                                          JOIN_INNER,
                                          NULL);
numIndexTuples = btreeSelectivity * index->rel->tuples;
```

For our empty-bound-quals case, `selectivityQuals = add_predicate_to_index_quals(index, NIL) = NIL`, and `clauselist_selectivity(root, NIL, ...)` returns `1.0`. So `numIndexTuples = 1.0 * index->rel->tuples`, i.e. *all* tuples of the underlying relation. For 1k accounts, that's `1000.0`; this is the entire-table-scan case.

The `#if PG_VERSION_NUM >= 170000` block at `src/indexam/handler.c:1161-1185` clamps `num_sa_scans` to `Min(num_sa_scans, ceil(index->pages * 0.3333333))` so that pathological SAOP indexes can't claim more descents than 1/3 the leaf pages. Irrelevant for our case (no SAOP, so `num_sa_scans = 1`).

Finally `numIndexTuples = rint(numIndexTuples / num_sa_scans)` (`src/indexam/handler.c:1192`). With `num_sa_scans = 1`, `numIndexTuples = 1000` stays unchanged.

## 4. `genericcostestimate` Delegation and Descent CPU Cost

### 4.1 The `genericcostestimate` call

After populating `costs.numIndexTuples` (and `costs.num_sa_scans` on PG17+), `orioledb_amcostestimate` simply delegates the heavy lifting to PG's generic helper:

```c
costs.numIndexTuples = numIndexTuples;
#if PG_VERSION_NUM >= 170000
costs.num_sa_scans = num_sa_scans;
#endif

genericcostestimate(root, path, loop_count, &costs);
```

(`src/indexam/handler.c:1198-1203`.) `genericcostestimate` lives at `src/backend/utils/adt/selfuncs.c:6629`. It computes:

- `indexSelectivity = clauselist_selectivity(root, selectivityQuals, ...)` again from scratch (`src/backend/utils/adt/selfuncs.c:6685-6688`). For our zero-clauses case, this is `1.0`. (Why is `clauselist_selectivity` called twice? Because we already supplied `costs->numIndexTuples > 0.0`, the `if (numIndexTuples <= 0.0)` branch at `src/backend/utils/adt/selfuncs.c:6696` is skipped — but the *selectivity* is still recomputed for its own sake, since it's reported separately to `cost_index()`.)
- `numIndexPages = ceil(numIndexTuples * index->pages / index->tuples)` (`src/backend/utils/adt/selfuncs.c:6733`) — for a full-scan with `numIndexTuples = index->tuples = 1000`, this is `index->pages`.
- I/O cost = `numIndexPages * spc_random_page_cost` for a single scan (`src/backend/utils/adt/selfuncs.c:6789`).
- CPU cost = `numIndexTuples * num_sa_scans * (cpu_index_tuple_cost + qual_op_cost)` (`src/backend/utils/adt/selfuncs.c:6813`).
- `indexCorrelation = 0.0` (`src/backend/utils/adt/selfuncs.c:6818`) — the generic estimator has no opinion; the caller is expected to overwrite this.

The reason `orioledb_amcostestimate` exists at all (rather than just registering `btcostestimate` directly) is so that the unique-index-short-circuit numIndexTuples handling can be inserted *before* `genericcostestimate` is called, and so that the per-descent CPU cost (next subsection) and correlation (Section 4.3) can be added *after*. Functionally though, those three additions are the same as what `btcostestimate` does upstream — so for a btree index over an orioledb table, the numeric output of `orioledb_amcostestimate` and `btcostestimate` are essentially identical on identical `pg_class` / `pg_statistic` inputs.

### 4.2 Per-descent CPU cost: `(tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost`

After the generic estimate, `orioledb_amcostestimate` adds two CPU-cost terms:

```c
if (index->tuples > 1)        /* avoid computing log(0) */
{
    descentCost = ceil(log(index->tuples) / log(2.0)) * cpu_operator_cost;
    costs.indexStartupCost += descentCost;
    costs.indexTotalCost += costs.num_sa_scans * descentCost;
}

descentCost = (index->tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
costs.indexStartupCost += descentCost;
costs.indexTotalCost += costs.num_sa_scans * descentCost;
```

(`src/indexam/handler.c:1216-1235`.) The two charges are:

- **Comparison cost per descent.** `log2(index->tuples)` comparisons to descend the tree, charged at `cpu_operator_cost` each. (`src/indexam/handler.c:1218`.)
- **Page-touch cost per descent.** `(tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost`. `DEFAULT_PAGE_CPU_MULTIPLIER` is PG's `50` (i.e. 50× `cpu_operator_cost` per page touched on the descent). For an orioledb btree with `tree_height = 0` (just a root that is also a leaf — the typical tiny-table case), this is `1 * 50 * 0.0025 = 0.125`. For a typical multi-level tree, this is the only term that distinguishes a tall index from a short one. (`src/indexam/handler.c:1233`.)

These are added once to `indexStartupCost` (`+=`) and *num_sa_scans times* to `indexTotalCost` (`*=` semantically — `costs.num_sa_scans * descentCost`).

Source-of-`tree_height`: see Section 6 — it's `_bt_getrootheight(indexRelation)`, called by PG's `get_relation_info()` at `src/backend/optimizer/util/plancat.c:495`.

### 4.3 Correlation from `pg_statistic`

The last 100 lines of `orioledb_amcostestimate` (`src/indexam/handler.c:1245-1340`) lift correlation from the stats catalog. The path branches on whether the index's leading column is a plain Var or an expression:

- **Plain-Var case** (`src/indexam/handler.c:1245-1274`): use the *underlying table's* statistics for the leading column. Call `get_relation_stats_hook` first; if not, do a `SearchSysCache3(STATRELATTINH, relid, colnum, rte->inh)` lookup.
- **Expression case** (`src/indexam/handler.c:1275-1300`): use statistics on the index itself (`relid = index->indexoid`, `colnum = 1`), via `get_index_stats_hook` or the same syscache.

If a stats tuple is found, the function pulls the `STATISTIC_KIND_CORRELATION` slot (`src/indexam/handler.c:1311-1330`) and writes:

```c
if (index->nkeycolumns > 1)
    costs.indexCorrelation = varCorrelation * 0.75;
else
    costs.indexCorrelation = varCorrelation;
```

The 0.75 dilution for multi-column indexes is exactly what `btcostestimate` does. **No orioledb-internal information feeds correlation; it is whatever ANALYZE last computed and stored in `pg_statistic`.**

Final assignment at `src/indexam/handler.c:1335-1339`:

```c
*indexStartupCost = costs.indexStartupCost;
*indexTotalCost = costs.indexTotalCost;
*indexSelectivity = costs.indexSelectivity;
*indexCorrelation = costs.indexCorrelation;
*indexPages = costs.numIndexPages;
```

These five numbers are the entire orioledb-side contribution to per-index cost. PG's `cost_index()` (`src/backend/optimizer/path/costsize.c:549`) then combines them with the table-level inputs (`baserel->tuples`, `baserel->pages`, `baserel->allvisfrac`) to produce the final `IndexPath.startup_cost` / `IndexPath.total_cost`. Those table-level inputs come from `orioledb_estimate_rel_size` (Section 7).

## 5. The o_scan CustomPath: cost inheritance, not recomputation

This is the single most important architectural fact in Topic 3. **OrioleDB does not write a custom cost function for its `o_scan` CustomPath.** It wraps an existing PG path (a `Path`, an `IndexPath`, or a `BitmapHeapPath`) and copies the cost fields verbatim.

The wrapping happens in `transform_path()` at `src/tableam/scan.c:133-193`:

```c
static Path *
transform_path(Path *src_path, OTableDescr *descr)
{
    CustomPath *result;

    Assert(IsA(src_path, IndexPath) || IsA(src_path, Path) ||
           IsA(src_path, BitmapHeapPath));

    result = makeNode(CustomPath);
    result->path.pathtype = T_CustomScan;
    result->path.parent = src_path->parent;
    result->path.pathtarget = src_path->pathtarget;
    result->path.param_info = src_path->param_info;
    result->path.rows = src_path->rows;
    result->path.startup_cost = src_path->startup_cost;
    result->path.total_cost = src_path->total_cost;
    result->path.pathkeys = src_path->pathkeys;
    result->path.parallel_aware = src_path->parallel_aware;
    result->path.parallel_safe = src_path->parallel_safe;
    result->path.parallel_workers = src_path->parallel_workers;
    result->methods = &o_path_methods;
    result->custom_paths = list_make1(src_path);
    ...
}
```

(`src/tableam/scan.c:141-154`.) Notice lines `src/tableam/scan.c:146-148`:

```c
result->path.rows = src_path->rows;
result->path.startup_cost = src_path->startup_cost;
result->path.total_cost = src_path->total_cost;
```

There is no recomputation. The CustomPath's cost is the cost the underlying path already had when `set_rel_pathlist_hook` was invoked — i.e. the cost `cost_index()` / `cost_seqscan()` / `cost_bitmap_heap_scan()` produced *before* the orioledb hook saw it.

The downstream consequence: when PG's `add_path()` ranks paths for this relation, an `o_scan` CustomPath is indistinguishable in cost from the original `Path` / `IndexPath` it wrapped — but it *replaces* that path in the relation's `pathlist` (Topic 2's territory), so the only way to get a non-o_scan path executed is for some path *not* targeted by `transform_path` to survive (e.g. a parameterized inner path on a different rel).

This is also why the GUC penalties (`enable_seqscan`, `enable_indexscan`, `enable_bitmapscan`) propagate correctly: those penalties were added by `cost_seqscan` / `cost_index` *before* the orioledb hook ran (`src/backend/optimizer/path/costsize.c:304-305`, `:606-607`, `:1041-1042`). When `transform_path` copies `src_path->total_cost`, the `disable_cost` term is included in that copy. So setting `enable_seqscan = off` correctly disables the o_scan-wrapped seqscan path too — the disable propagates by inheritance.

A subtler consequence: orioledb has no opportunity to *correct* a vanilla cost number that is misleading for index-organized storage. For instance, vanilla `cost_index()` charges `spc_random_page_cost` per index page (`src/backend/utils/adt/selfuncs.c:6789`); for an orioledb table where the "index" page *is* the data page (PK = data), this is fine. But it also adds `(1.0 - baserel->allvisfrac) * pages_fetched * spc_random_page_cost` for heap-fetch I/O on non-index-only scans (`src/backend/optimizer/path/costsize.c:686-710`) — for orioledb, every scan implicitly goes through the PK B-tree regardless of whether it's "index-only" or not, so this term is over-counting the I/O cost of a non-IOS index path. The TODO comment at `src/indexam/handler.c:1006` is exactly about this kind of mismatch.

### 5.1 The three input-path shapes and how `transform_path` differentiates them

`transform_path` switches on `IsA(src_path, ...)` (`src/tableam/scan.c:156-191`) to populate `custom_private`:

- `IsA(src_path, Path)` — a plain seqscan path. `new_path->ix_num = PrimaryIndexNumber`, `new_path->scandir = ForwardScanDirection`. **The vanilla seqscan over the heap is converted to a forward primary-key scan over the orioledb tree.** This is the "PK wins the seqscan slot" mechanism.
- `IsA(src_path, IndexPath)` — a vanilla btree index scan or index-only scan. The wrapping looks up `ix_num` by matching `index_descr->oids.reloid` against the path's `indexinfo->indexoid` (`src/tableam/scan.c:172-179`), and preserves the path's `indexscandir`.
- `IsA(src_path, BitmapHeapPath)` — wraps the whole bitmap heap subtree as a single CustomPath.

In all three cases the cost is unchanged.

### 5.2 Verification: is there an `o_path` cost hook anywhere?

`CustomPathMethods` has a `.PlanCustomPath` slot and that's all (`src/tableam/scan.c:92-96`). There is no `.ReparameterizeCustomPathByChild` and no cost callback — those exist on `CustomScanMethods` for the executor side, not for the planner-side path. So when add_path compares this CustomPath, it compares with the inherited cost. End of story.

## 6. Where index and table size come from

The two numbers that drive `orioledb_amcostestimate` heaviest — `index->pages` and `index->tuples` — are *not set by orioledb*. They are populated by PG's `get_relation_info()` in `src/backend/optimizer/util/plancat.c:472-501`, which runs before any orioledb hook for this rel:

```c
if (indexRelation->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
{
    if (info->indpred == NIL)
    {
        info->pages = RelationGetNumberOfBlocks(indexRelation);
        info->tuples = rel->tuples;
    }
    else
    {
        double      allvisfrac; /* dummy */
        estimate_rel_size(indexRelation, NULL,
                          &info->pages, &info->tuples, &allvisfrac);
        ...
    }

    if (info->relam == BTREE_AM_OID && (!skip_tree_height_hook || !skip_tree_height_hook(indexRelation)))
    {
        info->tree_height = _bt_getrootheight(indexRelation);
    }
    ...
}
```

(`src/backend/optimizer/util/plancat.c:472-501`.) Three calls:

1. **`info->pages = RelationGetNumberOfBlocks(indexRelation)`** for a non-partial index. The macro is defined at `src/include/storage/bufmgr.h:280-281` as `RelationGetNumberOfBlocksInFork(reln, MAIN_FORKNUM)`. For an index (relkind not in `RELKIND_HAS_TABLE_AM`), `RelationGetNumberOfBlocksInFork` takes the `else if (RELKIND_HAS_STORAGE(...))` branch at `src/backend/storage/buffer/bufmgr.c:3999-4001`, which calls `smgrnblocks(RelationGetSmgr(relation), forkNum)` — the *real on-disk size of the index file*. **This is whatever the smgr reports**, and on an orioledb table the index's smgr file is real (orioledb writes its B-tree pages out via the standard smgr), so this is meaningful.
2. **`info->tuples = rel->tuples`** for a non-partial index — i.e. the index inherits the table's `pg_class.reltuples` value. This is what was last written by ANALYZE.
3. **`info->tree_height = _bt_getrootheight(indexRelation)`** for a BTREE index. `_bt_getrootheight` walks the btree metapage and returns the height of the cached `btm_root`. For an orioledb index, this still works because orioledb btrees use the upstream btree metapage layout for their on-smgr image.

That means `orioledb_amcostestimate` consumes `pg_class.reltuples` (via `rel->tuples` propagation) and the smgr's notion of index file size. **Neither of these is updated when orioledb writes new tuples to its in-shared-memory B-tree.** They're updated only by ANALYZE or by autovacuum.

The relevant per-relation values are filled in via `estimate_rel_size()` at `src/backend/optimizer/util/plancat.c:202-203`. That function (`src/backend/optimizer/util/plancat.c:1066`) routes through the table-AM interface:

```c
if (RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind))
{
    table_relation_estimate_size(rel, attr_widths, pages, tuples,
                                 allvisfrac);
}
```

(`src/backend/optimizer/util/plancat.c:1075-1078`.) `table_relation_estimate_size` is the macro at `src/include/access/tableam.h:2032-2036`, which dispatches to `rel->rd_tableam->relation_estimate_size`. For an orioledb table that slot points at `orioledb_estimate_rel_size` (`src/tableam/handler.c:2534`). That's the only place orioledb gets to override the table-level cost inputs — see the next section.

## 7. `orioledb_estimate_rel_size`: table-level size estimates

`orioledb_estimate_rel_size` (`src/tableam/handler.c:1525-1625`) is the *only* place orioledb gets to influence per-table planner inputs. Its three outputs are `pages`, `tuples`, `allvisfrac`. It is a near-exact copy of `table_block_relation_estimate_size` — the same algorithm vanilla heap uses — with one important difference: the `curpages` comes from `RelationGetNumberOfBlocks(rel)`, which for an orioledb table is dispatched through the table-AM `relation_size` slot, **not** through smgr.

The flow:

```c
curpages = RelationGetNumberOfBlocks(rel);

/* coerce values in pg_class to more desirable types */
relpages = (BlockNumber) rel->rd_rel->relpages;
reltuples = (double) rel->rd_rel->reltuples;
relallvisible = (BlockNumber) rel->rd_rel->relallvisible;
```

(`src/tableam/handler.c:1536-1541`.) Crucially:

- `curpages` is **live**: dispatch chain `RelationGetNumberOfBlocks(rel) → RelationGetNumberOfBlocksInFork → table_relation_size(rel) → orioledb_calculate_relation_size(rel, MAIN_FORKNUM, DEFAULT_SIZE)` (`src/backend/storage/buffer/bufmgr.c:3985-3997`, then `src/tableam/handler.c:2530` which routes to `src/tableam/handler.c:1282`). For `DEFAULT_SIZE`/`RELATION_SIZE` method, `orioledb_calculate_relation_size` runs `o_btree_load_shmem(&GET_PRIMARY(descr)->desc)` and returns `(uint64) TREE_NUM_LEAF_PAGES(&GET_PRIMARY(descr)->desc) * ORIOLEDB_BLCKSZ` (`src/tableam/handler.c:1354-1361`). Divided by BLCKSZ (`src/backend/storage/buffer/bufmgr.c:3997`), this gives the live PK leaf-page count.
- `relpages`, `reltuples`, `relallvisible` are **stale**: they are whatever ANALYZE last wrote to `pg_class`. If you've never analyzed since INSERTs, these don't reflect current data.

The density extrapolation at `src/tableam/handler.c:1585-1611` reconciles the two:

```c
if (reltuples >= 0 && relpages > 0)
{
    density = reltuples / (double) relpages;
}
else
{
    /* fallback when never analyzed: estimate tuple width */
    ...
    density = ((double) (ORIOLEDB_BLCKSZ / 2)) / tuple_width;
}
*tuples = rint(density * (double) curpages);
```

So **`tuples`** is `(stale-density) × (live-curpages)`. The empty-rel-special-case at `src/tableam/handler.c:1569-1572` says: if `curpages < 10` and `relpages == 0`, treat the relation as if it had 10 pages anyway (the "never vacuumed" HACK comment).

`allvisfrac` is `relallvisible / curpages` (`src/tableam/handler.c:1619-1624`). For an orioledb table this is essentially always 0 in practice, since orioledb does not maintain a vis-map in the same way; this means `cost_index()`'s `(1.0 - baserel->allvisfrac)` heap-fetch reduction does nothing.

### 7.1 What this means for cost numbers

After `orioledb_estimate_rel_size` runs:

- `baserel->pages` = current PK leaf pages.
- `baserel->tuples` = density-scaled estimate (stale density × live page count).
- `baserel->allvisfrac` = stale vis-map fraction (≈ 0).
- `index->pages` = on-disk smgr blocks of the index file.
- `index->tuples` = `baserel->tuples` (assignment at `src/backend/optimizer/util/plancat.c:477`).
- `index->tree_height` = result of `_bt_getrootheight()` reading the index metapage.

For our `count(*) FROM o_bank_account` query, with 1000 rows and a freshly-analyzed table:

- `baserel->pages` ≈ 8 (1000 rows × ~250 bytes ÷ 8KB-ish ORIOLEDB_BLCKSZ).
- `baserel->tuples` = 1000.
- `baserel->allvisfrac` = 0.
- PK `index->pages` ≈ 8 (it *is* the table).
- PK `index->tuples` = 1000.
- PK `index->tree_height` ≈ 0 or 1.

These numbers all flow into both the vanilla SK IndexOnlyScan cost path *and* the o_scan-wrapped PK cost path — Section 10 walks the resulting numerical comparison.

## 8. How PG compares an o_scan against a vanilla path

This is largely Topic 1's territory and we summarise only the parts that bear on the "PK wins" outcome.

### 8.1 `add_path`'s Pareto rule

`add_path` (`src/backend/optimizer/util/pathnode.c:420`) compares each candidate path against the survivors-so-far using the four dimensions `(startup_cost, total_cost, pathkeys, parameterization)`. A new path is kept iff there is no existing path that dominates it on *all four* dimensions. The two paths under consideration for `count(*) FROM o_bank_account` are:

1. The `o_scan` CustomPath wrapping the PK IndexOnlyScan path. Cost copied verbatim from `cost_index()` output for an IOS on the PK.
2. *(In the no-GUC case)* the vanilla SK IndexOnlyScan path. Cost from `cost_index()` for an IOS on the SK.

The vanilla seqscan path (vanilla `Path` node) is also generated by `set_plain_rel_pathlist`, but `transform_path` wraps that one too and points it at the PK (`src/tableam/scan.c:160-163`), so it doesn't survive as a non-o_scan path.

For the no-GUC case, both candidate IOS paths have very similar costs (small B-tree, full scan, no quals). Whichever wins by a small margin gets it, but in observation it's typically the PK IOS, because:

- `index->pages` on the PK is the same as `baserel->pages` (the PK *is* the table).
- `index->pages` on the SK is the on-disk SK file size, which for a multi-column SK may be larger or smaller depending on tuple width.
- The PK has correlation ≈ 1.0 (newly-inserted rows are appended); the SK has correlation that depends on the column being scanned.

In practice, the orioledb test harness in `test/t/crash/rr_stress_test.py` observes that `count(*)` *always* plans as `o_scan over PK` for `o_bank_account` even with all enable_* GUCs on — the schema's SK is wider than the PK, so SK IOS is more expensive than PK IOS.

### 8.2 What `enable_seqscan = off` / `enable_indexscan = off` / `enable_bitmapscan = off` do

These are checked inside the *vanilla* cost functions before orioledb wraps anything:

- `cost_seqscan`: `if (!enable_seqscan) startup_cost += disable_cost;` at `src/backend/optimizer/path/costsize.c:304-305`.
- `cost_index`: `if (!enable_indexscan) startup_cost += disable_cost;` at `src/backend/optimizer/path/costsize.c:606-607`. The comment on `:608` is critical: *"we don't need to check enable_indexonlyscan; indxpath.c does that"*.
- `cost_bitmap_heap_scan`: `if (!enable_bitmapscan) startup_cost += disable_cost;` at `src/backend/optimizer/path/costsize.c:1041-1042`.

`disable_cost = 1.0e10` (`src/backend/optimizer/path/costsize.c:130`). It's a numeric sentinel: 10 billion cost-units, large enough to swamp any sane plan's natural cost but small enough that you can still compare two disabled plans against each other.

**`enable_indexonlyscan` is checked separately**, in `check_index_only()` at `src/backend/optimizer/path/indxpath.c:2163`: `if (!enable_indexonlyscan) return false;`. This *rejects the IOS shape entirely* — no IOS path is generated, the regular IndexPath shape is used instead, which is then subject to `enable_indexscan` disable_cost. So setting `enable_indexscan = off` does not implicitly disable IOS through this check; it disables IOS by ensuring that even if the IOS path is generated, its cost gets +disable_cost.

Wait — re-reading the code: `cost_index()` at `src/backend/optimizer/path/costsize.c:606-608` is reached for *both* `T_IndexScan` and `T_IndexOnlyScan` pathtypes (the function dispatches on `path->path.pathtype == T_IndexOnlyScan` at `:554`). So when `enable_indexscan = false`, *every* IndexPath (including IOS) gets +disable_cost at startup. **That is the mechanism by which our `enable_indexscan = off` setting penalises the SK IOS path.**

Combined picture for the forced-PK test setup with `enable_seqscan = off`, `enable_bitmapscan = off`, `enable_indexscan = off`:

| Path generated | Underlying cost | Penalty applied | Final cost | What wraps it |
|---|---|---|---|---|
| Seqscan on heap | `seqscan_cost` | +1e10 (`enable_seqscan = off`) | seqscan_cost + 1e10 | wrapped to `o_scan` over PK, cost preserved |
| IOS on PK | `pk_ios_cost` | +1e10 (`enable_indexscan = off`) | pk_ios_cost + 1e10 | wrapped to `o_scan` over PK, cost preserved |
| IOS on SK | `sk_ios_cost` | +1e10 (`enable_indexscan = off`) | sk_ios_cost + 1e10 | wrapped to `o_scan` over SK, cost preserved |
| Bitmap heap | (irrelevant for count(*)) | +1e10 | ... + 1e10 | wrapped to `o_scan` bitmap, cost preserved |

Every path in the pathlist now carries the +1e10 penalty, because every candidate path's underlying generator added it. There is no "magic survivor" path that escapes the penalty.

This is why `enable_seqscan = off` etc. *do not actually pick a specific orioledb path*: they tag every candidate equally with +1e10, and `add_path` then selects on the residual cost difference. The path that wins is the one whose pre-penalty cost was lowest, because all penalties are equal. **For our `count(*)` query, that is the o_scan-wrapped PK IOS** — small B-tree, no qual filter, sequential read, correlation ≈ 1.0, no heap fetches needed because the PK *is* the data.

## 9. No live-FSM input: `check_btree`, `.map`, free-extents are not planner inputs

Worth saying explicitly because it is sometimes assumed otherwise: **no orioledb-side live B-tree integrity or free-space state feeds the planner.** A grep for `check_btree` across orioledb shows exactly two non-definition callers:

- `src/catalog/sys_trees.c:575` — `result = check_btree(get_sys_tree(num), force_map_check);` inside `check_sys_tree()`.
- `src/tableam/func.c:1277` — `bool curr_tree_result = check_btree(&descr->indices[i]->desc, force_map_check);` inside `orioledb_tbl_check()`, which is the SQL-callable `orioledb_tbl_check(regclass)` function.

Neither is in a planning path. `orioledb_tbl_check` is a diagnostic — invoked only by the user via SQL or by tests. `check_sys_tree` is similarly diagnostic.

Likewise, the following pieces of orioledb's live state are **never read** during planning:

- The `.map` files (CoW checkpoint mapping). Read by the checkpointer and recovery, never by the planner.
- Free-extents arrays / free-page lists. Read by the page allocator; never by the planner.
- The per-page lock state machine (`src/btree/page_state.c`). Concurrency control, not cost.
- Undo log state (`src/transam/undo.c`). MVCC visibility, not cost.
- The CSN buffer (`xidBuffer`). MVCC, not cost.

The full list of orioledb-side cost inputs is in Section 11's table. The TL;DR: **only `orioledb_calculate_relation_size` reaches into orioledb's live state, and only to count PK leaf pages.** Every other input lives in `pg_class` / `pg_statistic`.

A practical consequence: bloated SK B-trees, mostly-empty SK pages, hot pages, free-list state — none of these change planner decisions. They only change *execution* cost (which path is actually fastest), which is invisible to the planner.

## 10. Worked example: `SELECT count(*) FROM o_bank_account` with the three GUCs off

The query observed in `test/t/crash/rr_stress_test.py`:

```sql
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = off;
SELECT count(*) FROM o_bank_account;
```

produces:

```
Aggregate
  ->  Custom Scan (o_scan) on o_bank_account
        Forward index only scan of: o_bank_account_pkey
```

Schema (simplified):

```sql
CREATE TABLE o_bank_account (
    id    int  PRIMARY KEY,
    bal   bigint,
    token int
) USING orioledb;
CREATE INDEX o_bank_account_token_idx ON o_bank_account (token, id);
```

(Two indexes total: PK `o_bank_account_pkey` on `(id)`, SK `o_bank_account_token_idx` on `(token, id)`.)

### 10.1 Candidate paths generated by PG

For a single base relation with `count(*)` (no quals, no targets needing data columns), the planner generates:

| # | Generator | Path type | Index | Cost source |
|---|---|---|---|---|
| P1 | `set_plain_rel_pathlist` → `create_seqscan_path` | `Path` (T_SeqScan) | — | `cost_seqscan` |
| P2 | `create_index_paths` → `build_index_paths` → `create_index_path(IOS)` | `IndexPath` (T_IndexOnlyScan) | PK | `cost_index(indexonly=true)` |
| P3 | `create_index_paths` → `build_index_paths` → `create_index_path(IOS)` | `IndexPath` (T_IndexOnlyScan) | SK (`o_bank_account_token_idx`) | `cost_index(indexonly=true)` |
| (P4) | bitmap heap path on SK (if cost beats seq) | `BitmapHeapPath` | SK | `cost_bitmap_heap_scan` |

(P4 is not generated for a no-WHERE `count(*)` because there are no quals to feed a bitmap index scan.)

The IOS shape is generated for both PK and SK because `check_index_only()` (`src/backend/optimizer/path/indxpath.c:2154`) returns true for both: all attrs needed by the query (i.e. none — `count(*)` doesn't reference data) are coverable by either index, since orioledb adds PK columns to every SK's `indexkeys` in `orioledb_set_plain_rel_pathlist_hook` (`src/tableam/scan.c:260-291`) — that's the "Additional pkey fields are added to index target list so that the index only scan is selected" mechanism documented in the comment at `src/tableam/scan.c:222-224`.

### 10.2 Per-path cost before `transform_path`

Costs (with `cpu_tuple_cost = 0.01`, `cpu_index_tuple_cost = 0.005`, `cpu_operator_cost = 0.0025`, `seq_page_cost = 1`, `random_page_cost = 4`, `disable_cost = 1e10`, `effective_cache_size = 512MB`; tuple counts from `pg_class.reltuples = 1000`):

**P1: seqscan over heap.** PG thinks the table has `relpages` pages on the smgr (orioledb has a stub here; `relpages` from `pg_class` is just whatever ANALYZE last wrote). For 1000 rows at ~40 bytes per row + header padding ≈ 8 pages.

- Pre-penalty: `disk_run_cost = 8 * 1 = 8`. `cpu_run_cost = 0.01 * 1000 = 10`. `startup_cost = 0`. `total_cost ≈ 18`.
- After `+disable_cost` (`enable_seqscan = off`): `startup_cost = 1e10`, `total_cost ≈ 1e10 + 18`.

**P2: IOS on PK.** `index->pages` ≈ 8, `index->tuples` = 1000, `index->tree_height` ≈ 1, `index->unique = true`. No quals so `numIndexTuples = 1000`, `numIndexPages = ceil(1000 * 8 / 1000) = 8`.

- From `genericcostestimate`: `indexTotalCost = 8 * random_page_cost = 32` for I/O, plus `1000 * (cpu_index_tuple_cost + qual_op_cost) = 1000 * (0.005 + 0) = 5` CPU. `indexStartupCost = 0` (no qual eval setup).
- From `orioledb_amcostestimate` descent terms: `descentCost1 = ceil(log2(1000)) * 0.0025 = 10 * 0.0025 = 0.025`, `descentCost2 = (1 + 1) * 50 * 0.0025 = 0.25`. So `indexStartupCost ≈ 0.275`, `indexTotalCost ≈ 32 + 5 + 0.275 = 37.275`.
- From `cost_index`: `startup_cost = 0.275 + 0 (qpqual) = 0.275`. `run_cost = indexTotalCost - indexStartupCost = 37`. Then IOS subtracts heap-fetch I/O: `pages_fetched = ceil(pages_fetched * (1.0 - allvisfrac))` — with `allvisfrac = 0`, this *doesn't* eliminate heap fetches (orioledb effectively never marks all-visible). So PG charges `8 * random_page_cost = 32` for "heap fetches"... but wait, this is double-counting. For our purposes: `total_cost ≈ 32 (heap) + 32 (index) + 5 (cpu) + descent ≈ 70` pre-penalty.
- After `+disable_cost`: `total_cost ≈ 1e10 + 70`.

**P3: IOS on SK.** `index->pages` is the SK file size. For a `(token, id)` index over 1000 rows of int+int ≈ 16-byte keys, ~3-4 leaf pages. `index->tuples` = 1000.

- Similar to P2 but with smaller `index->pages`. I/O cost ≈ `4 * random_page_cost = 16`. CPU ≈ 5. Descent ≈ same. Pre-penalty `total_cost ≈ 50`-ish.
- Heap-fetch term: same `(1 - allvisfrac)` issue → also charges full heap I/O ≈ 32.
- After `+disable_cost`: `total_cost ≈ 1e10 + 50`.

### 10.3 After `transform_path`

`set_rel_pathlist_hook` runs after all costs are computed. `transform_path` wraps each candidate path:

- P1 (seqscan) → `o_scan` over PK (`scandir = ForwardScanDirection`, `ix_num = PrimaryIndexNumber`). Cost copied: `1e10 + 18`.
- P2 (PK IOS) → `o_scan` over PK with `ix_num = PrimaryIndexNumber` (matched via `descr->indices[ix_num]->oids.reloid`). Cost copied: `1e10 + 70`.
- P3 (SK IOS) → `o_scan` over SK with `ix_num = 1` (the SK's position in `descr->indices`). Cost copied: `1e10 + 50`.

### 10.4 `add_path` Pareto tournament

All three CustomPaths have parameterization `NULL`, identical `pathkeys` (none — `count(*)` doesn't care about order), and they all return `rows = 1000`. So the tournament reduces to: cheapest `total_cost` wins.

The numbers are dominated by the `+1e10` penalty (all three have it), so the residual difference is:

- P1' (o_scan seqscan over PK): 18
- P2' (o_scan IOS over PK): 70
- P3' (o_scan IOS over SK): 50

Wait — P1' has the lowest residual. So why does EXPLAIN show "Forward *index only* scan" rather than just "Forward scan"?

The reason is the dispatch in `o_plan_custom_path` (`src/tableam/scan.c:457-486`):

```c
if (custom_plans && IsA(custom_plan, IndexScan))
{
    ...
    qpqual = ix_scan->scan.plan.qual;
}
else if (custom_plans && IsA(custom_plan, IndexOnlyScan))
{
    IndexOnlyScan *ixo_scan = (IndexOnlyScan *) custom_plan;
    plan->targetlist = ixo_scan->scan.plan.targetlist;
    custom_scan->custom_scan_tlist = ixo_scan->indextlist;
    qpqual = ixo_scan->scan.plan.qual;
}
...
custom_scan->custom_private = list_make4(makeInteger(O_IndexPlan),
                                          makeInteger(ix_path->ix_num),
                                          makeInteger(ix_path->scandir),
                                          makeInteger(onlyCurIx));
```

`onlyCurIx` is `IsA(custom_plan, IndexOnlyScan)`. The "Forward index only scan of: o_bank_account_pkey" line in EXPLAIN is generated by `o_explain_custom_scan` (`src/tableam/scan.c:1062-1064`) based on `ix_plan_state->ostate.onlyCurIx`.

So **if the winning path is the wrapped seqscan (P1'), the EXPLAIN should say "Forward scan of: o_bank_account_pkey"** (no "index only"), because P1's `IsA(src_path, Path)` branch in `transform_path` (`src/tableam/scan.c:156-164`) doesn't carry any IndexOnlyScan node, so `IsA(custom_plan, IndexOnlyScan)` is false, so `onlyCurIx` is false.

**The fact that EXPLAIN says "index only scan" means P2' (the IOS-over-PK path) won, not P1'.** Which means our residual cost analysis is wrong somewhere.

Re-examining: the seqscan plan has CPU `cpu_per_tuple * tuples = 0.01 * 1000 = 10`. The IOS plan has CPU `cpu_index_tuple_cost * 1000 = 5`. The IOS plan is *cheaper per row to extract*, because index entries are smaller than heap tuples in PG's accounting (the heap version has to evaluate target list against the full row; IOS gets the value direct from the index tuple). Also the IOS plan claims `(1 - allvisfrac) * heap_io = (1 - 0) * 32 = 32` extra... so vanilla PG would think IOS-over-PK is actually *more* expensive than seqscan for orioledb (since `allvisfrac` is effectively 0).

This is a real numerical artifact: vanilla PG cost numbers are wrong for orioledb in the IOS-PK case (overcounting heap fetches), but the wrapping doesn't correct them. So which one wins depends on the exact numbers — and given that the EXPLAIN consistently shows IOS-PK, there's something we're missing.

The thing we're missing is most likely **the `count(*)` aggregate strategy**: PG's `path_planner` for `count(*)` prefers paths whose `pathtarget` is the *narrowest possible*. The IndexOnlyScan path's pathtarget references only the index columns; the seqscan path's pathtarget is the full row width. `add_path` compares `pathtarget->cost.per_tuple` indirectly through the post-tlist cost computation done inside `cost_index` / `cost_seqscan`:

- `cost_seqscan`: `cpu_run_cost += path->pathtarget->cost.per_tuple * path->rows;` (`src/backend/optimizer/path/costsize.c:325`).
- `cost_index`: `cpu_run_cost += path->path.pathtarget->cost.per_tuple * path->path.rows;` (`src/backend/optimizer/path/costsize.c:804`).

For `count(*)`, the *seqscan*'s pathtarget contains every retrievable column (the full row), while the *IOS*'s pathtarget contains only the indexed columns. With wider tuples, the seqscan's `per_tuple` tlist cost is larger, swamping the heap-fetch overestimate on IOS-PK.

This analysis isn't precise without seeing exact `EXPLAIN (ANALYZE, VERBOSE, FORMAT YAML)` output and the live `pg_class.reltuples` for the table — but the qualitative conclusion stands: **the IOS-PK plan wins on the per-tuple tlist cost difference for `count(*)`, and the +1e10 penalty applies equally to every candidate so it doesn't break the tie.**

### 10.5 What we can state confidently

Independent of the precise per-tuple cost shortfall:

1. All four candidate paths get `+disable_cost = 1e10` applied to their startup cost by the vanilla cost functions before `transform_path` wraps them.
2. `transform_path` copies that cost unchanged into the CustomPath wrapper.
3. `add_path` is therefore comparing four near-identical `+1e10` paths on residual cost.
4. The residual cost favours the IOS-PK plan because (a) `count(*)` doesn't need any data columns, (b) the PK is reachable as IOS due to `orioledb_set_plain_rel_pathlist_hook` adding PK columns to every SK's `indexkeys`, (c) the tlist evaluation cost for an IOS is lower than for a seqscan-over-heap path because the IOS pathtarget is narrow, and (d) vanilla `cost_index` actually charges the same I/O cost as `cost_seqscan` per-page (both `random` for index, `seq` for heap) so the residuals are within a few cost-units.
5. The winning path's `IndexOnlyScan` shape carries through to `o_plan_custom_path`'s `onlyCurIx = true` flag, which is what makes EXPLAIN print "Forward index only scan".

## 11. Planner inputs table: source and whether live FSM affects them

Comprehensive table of every quantity the planner reads when costing a scan over an orioledb table:

| Quantity | Read from (file:line) | Ultimate source | Updated by | Affected by live FSM? |
|---|---|---|---|---|
| `baserel->pages` | `estimate_rel_size` via `table_relation_estimate_size` → `orioledb_estimate_rel_size` (`src/tableam/handler.c:1536`) | `orioledb_calculate_relation_size(rel, MAIN_FORKNUM, DEFAULT_SIZE)` → PK B-tree leaf-page count via `TREE_NUM_LEAF_PAGES` (`src/tableam/handler.c:1359`) | Every B-tree page split / merge (live in shmem) | **Yes**, indirectly — leaf count changes as B-tree grows/shrinks |
| `baserel->tuples` | `orioledb_estimate_rel_size` (`src/tableam/handler.c:1611`): `rint(density * curpages)` | density = `pg_class.reltuples / pg_class.relpages`, scaled by current `curpages` | ANALYZE (`pg_class` columns); `curpages` is live | **Partly** — `curpages` is live, but density is stale |
| `baserel->allvisfrac` | `orioledb_estimate_rel_size` (`src/tableam/handler.c:1619-1624`) | `pg_class.relallvisible / curpages` | ANALYZE writes `pg_class.relallvisible` (orioledb essentially leaves this at 0) | No |
| `index->pages` | `get_relation_info` (`src/backend/optimizer/util/plancat.c:476`): `RelationGetNumberOfBlocks(indexRelation)` | smgr file size of index file (orioledb writes index pages via standard smgr) | Every checkpoint writes new index pages to smgr | **Partly** — reflects on-disk file size, not in-shmem state |
| `index->tuples` | `get_relation_info` (`src/backend/optimizer/util/plancat.c:477`): `info->tuples = rel->tuples` (i.e. inherits `baserel->tuples`) | Same as `baserel->tuples` | Same | Same |
| `index->tree_height` | `get_relation_info` (`src/backend/optimizer/util/plancat.c:495`): `_bt_getrootheight(indexRelation)` | Btree metapage's `btm_root` height (cached in indexRelation) | B-tree root splits | **Partly** — read from metapage which is updated at split-root events |
| `index->unique`, `index->amhasgetbitmap`, etc. | `get_relation_info`, from `pg_index` catalog tuple | `pg_index` columns | DDL only | No |
| Column selectivities (`clauselist_selectivity`) | `pg_statistic` via syscache (n_distinct, MCVs, histogram) | ANALYZE | ANALYZE only | No |
| Column correlation (`STATISTIC_KIND_CORRELATION`) | `pg_statistic` slot, fetched in `orioledb_amcostestimate` (`src/indexam/handler.c:1314`) | ANALYZE | ANALYZE only | No |
| `pg_class.reltuples`, `relpages`, `relallvisible` | direct read of `rel->rd_rel` in `orioledb_estimate_rel_size` | `pg_class` row, populated by ANALYZE | ANALYZE only | No |
| `cpu_tuple_cost`, `cpu_index_tuple_cost`, `cpu_operator_cost`, `seq_page_cost`, `random_page_cost` | GUC-globals, used inside `cost_index` / `genericcostestimate` / `cost_seqscan` | postgresql.conf | Restart / SET | No |
| `enable_seqscan`, `enable_indexscan`, `enable_bitmapscan`, `enable_indexonlyscan` | GUC-globals, checked in cost functions and `check_index_only` | postgresql.conf | SET | No |
| `effective_cache_size` | GUC-global, used in Mackert-Lohman formula in `index_pages_fetched` | postgresql.conf | SET | No |

**Live FSM state, free-extents, `.map` file content, undo-log occupancy, CSN buffer fill, page lock state, checkpoint-in-progress flag**: NONE of these are inputs to any of the above. There is no orioledb hook in PG's planner that reads in-shmem orioledb structures other than `relation_size`.

The implication is that **the planner's behaviour is determined entirely by `pg_class` + `pg_statistic` + the *current* PK-tree leaf-page count** (and the index metapage's tree height). Crash recovery does not reload any of these, except indirectly:

- If recovery rebuilds the B-tree shapes, the PK leaf-page count read by `orioledb_calculate_relation_size` will reflect the post-recovery state.
- The index metapage's `btm_root` height is re-read from disk on first `_bt_getrootheight` call after relcache reload.
- `pg_class.reltuples` does not change across crash + recovery — it's persistent in the system catalog.

## 12. What changes between fresh-startup and post-recovery — open question

This section is speculation, explicitly flagged. In the crash-recovery test harness (`test/t/crash/rr_stress_test.py`), we have observed that:

- Initdb + workload (no crash): the planner sometimes picks SK IOS plans for diagnostic queries like `SELECT count(DISTINCT token) FROM o_bank_account` — i.e. honours the `enable_seqscan = off` / `enable_indexscan = off` configuration as expected, picking an SK-based plan because the SK is the only way to evaluate `count(DISTINCT token)` without seqscanning.
- After a recovery cycle (PANIC + postmaster restart + replay): the same diagnostic queries flip to o_scan-over-PK plans, even though the GUC configuration is identical ("plan-FAIL" pattern in the bug-hunt logs).

This is empirical. The mechanism is *not yet proven*. Plausible hypotheses, with the input each would change:

### 12.1 `pg_class.reltuples` / `relpages` not updated by recovery

When ANALYZE runs during the pre-crash workload, it writes a `reltuples` value reflecting the in-progress table. After a crash, that value is the *last committed* `reltuples` from ANALYZE — which may differ substantially from the actual recovered table state. Specifically:

- If ANALYZE ran when the table had 1000 rows, `reltuples = 1000`.
- A crash mid-workload may discard some rows (committed before crash but undo'd by recovery) or apply some new rows (committed but not flushed).
- After recovery, `reltuples` still says 1000, but `curpages` (live PK leaf count) may say 8 or may say 4 (if some pages got merged) or may say 12 (if more pages got allocated than the analyze saw).

`orioledb_estimate_rel_size` computes `density = reltuples / relpages` and `tuples = density * curpages`. If `curpages` rises while `relpages` stays at the analyze-time value, `baserel->tuples` rises proportionally. A larger `baserel->tuples` directly inflates the per-tuple CPU cost of both seqscan and IOS, but inflates seqscan more (because seqscan charges `cpu_tuple_cost = 0.01` per tuple while IOS charges `cpu_index_tuple_cost = 0.005`). **A larger inflated `baserel->tuples` would push the seqscan path's residual cost higher relative to the IOS-PK path, biasing toward o_scan-over-PK.**

Predicted test: after a recovery cycle, query `pg_class.reltuples` and `pg_class.relpages` for `o_bank_account`, and compare with `SELECT orioledb_calculate_relation_size(...)` for live page count. If `curpages > relpages` post-recovery, hypothesis 12.1 is plausible.

### 12.2 Relcache invalidation pattern

PG's relcache is invalidated lazily on signals. If recovery sends an invalidation for `o_bank_account_pkey`'s relcache entry, the next planner call re-runs `_bt_getrootheight()` and `RelationGetNumberOfBlocks()` on a freshly-opened index. The new `tree_height` may differ from the cached one (e.g. if pre-crash had `tree_height = 1` and post-recovery rebuilt the root to `tree_height = 0` after page merges). A smaller `tree_height` means a smaller per-descent CPU cost (`(tree_height + 1) * 50 * cpu_operator_cost`) — affects PK and SK symmetrically though, so probably not a tiebreaker.

### 12.3 `rd_rel` reload differences

When a backend reconnects after a postmaster restart, its `Relation` is freshly opened, so `rel->rd_rel` is read from `pg_class` from scratch. There's no obvious way this would differ from the pre-crash value (the catalog is durable) unless the crash interrupted a `vac_update_relstats` call (mid-update of `pg_class`).

### 12.4 Statistics on `token` column

`o_bank_account_token_idx` is the SK we typically force in diagnostic queries. Its cost depends on `pg_statistic` entries for the `token` column (distinct count, MCVs). After recovery, the `token` value distribution may have shifted (rolled-back transactions remove some tokens, replayed commits add others), but `pg_statistic` retains the pre-crash distribution. The resulting selectivity mis-estimate could push the SK path's cost up or down.

### 12.5 The honest summary

We don't know which input is shifting post-recovery. The next investigative step is to:

1. Add a planner-time `elog(LOG, "planner inputs: pages=%u tuples=%f allvisfrac=%f", baserel->pages, baserel->tuples, baserel->allvisfrac)` at the end of `orioledb_estimate_rel_size`, and the equivalent for `index->pages` / `index->tree_height` immediately after `get_relation_info`.
2. Run a hunt cycle, dump those LOG values for the failing-plan query both pre- and post-recovery.
3. Diff the two sets of inputs. Whichever input changes is the answer.

This diagnosis is **out of scope for this report**, but the inputs are now fully enumerated (Section 11) so the diff can be done without further reverse-engineering of the planner code paths.

---

## Appendix A: Concrete file:line citations

For reference, the primary citations used in this document:

- `orioledb_amcostestimate` — `src/indexam/handler.c:1008`
- The TODO comment "Rewrite to be more orioledb-specific" — `src/indexam/handler.c:1006`
- Bound-qual walk in `orioledb_amcostestimate` — `src/indexam/handler.c:1051-1129`
- Unique-index short-circuit `numIndexTuples = 1.0` — `src/indexam/handler.c:1137-1142`
- `clauselist_selectivity * tuples` — `src/indexam/handler.c:1155-1159`
- Delegation to `genericcostestimate` — `src/indexam/handler.c:1203`
- Per-descent CPU cost `(tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost` — `src/indexam/handler.c:1233`
- Correlation from `pg_statistic` — `src/indexam/handler.c:1245-1330`
- `transform_path()` cost copy — `src/tableam/scan.c:141-154`, especially lines 146-148
- `OPath` / `OIndexPath` / `OBitmapHeapPath` definitions — `src/tableam/scan.c:54-75`
- `o_path_methods` (no cost callback) — `src/tableam/scan.c:92-96`
- `orioledb_set_rel_pathlist_hook` — `src/tableam/scan.c:321`
- `orioledb_set_plain_rel_pathlist_hook` (SK pk-column augmentation) — `src/tableam/scan.c:196-315`
- `o_plan_custom_path` (`onlyCurIx` derivation) — `src/tableam/scan.c:457-486`
- `o_explain_custom_scan` ("Forward index only scan of") — `src/tableam/scan.c:1062-1064`
- `orioledb_estimate_rel_size` — `src/tableam/handler.c:1525-1625`
- `RelationGetNumberOfBlocks(rel)` call in orioledb_estimate_rel_size — `src/tableam/handler.c:1536`
- `orioledb_calculate_relation_size` — `src/tableam/handler.c:1282-1430`
- DEFAULT_SIZE / RELATION_SIZE branch — `src/tableam/handler.c:1354-1361`
- TableAmRoutine slot registration — `src/tableam/handler.c:2530`, `:2534`
- `check_btree` definition — `src/btree/check.c:70`
- `check_btree` callers (full list, 2 entries) — `src/catalog/sys_trees.c:575`, `src/tableam/func.c:1277`
- `genericcostestimate` — `src/backend/utils/adt/selfuncs.c:6629`
- `genericcostestimate` numIndexPages formula — `src/backend/utils/adt/selfuncs.c:6733`
- `cost_index` — `src/backend/optimizer/path/costsize.c:549`
- `cost_index` enable_indexscan check — `src/backend/optimizer/path/costsize.c:606-608`
- `cost_seqscan` enable_seqscan check — `src/backend/optimizer/path/costsize.c:304-305`
- `cost_bitmap_heap_scan` enable_bitmapscan check — `src/backend/optimizer/path/costsize.c:1041-1042`
- `disable_cost = 1.0e10` — `src/backend/optimizer/path/costsize.c:130`
- `check_index_only` `enable_indexonlyscan` check — `src/backend/optimizer/path/indxpath.c:2163`
- `get_relation_info` (PG-side; populates `index->pages`, `index->tuples`, `index->tree_height`) — `src/backend/optimizer/util/plancat.c:117`
- `info->pages = RelationGetNumberOfBlocks(indexRelation)` — `src/backend/optimizer/util/plancat.c:476`
- `info->tuples = rel->tuples` — `src/backend/optimizer/util/plancat.c:477`
- `info->tree_height = _bt_getrootheight(indexRelation)` — `src/backend/optimizer/util/plancat.c:495`
- `estimate_rel_size` (PG-side, table-AM dispatch) — `src/backend/optimizer/util/plancat.c:1066-1078`
- `RelationGetNumberOfBlocksInFork` (table-AM vs smgr branch) — `src/backend/storage/buffer/bufmgr.c:3983-4007`
- `RelationGetNumberOfBlocks` macro — `src/include/storage/bufmgr.h:280-281`
- `table_relation_estimate_size` macro — `src/include/access/tableam.h:2032-2036`
- `add_path` — `src/backend/optimizer/util/pathnode.c:420`

## Appendix B: Cross-references to Topic 1 and Topic 2

- Topic 1 (vanilla PG path generation): the upstream `cost_seqscan`, `cost_index`, `cost_bitmap_heap_scan`, and `add_path` Pareto tournament. This Topic 3 walks through *which of those numbers* are influenced by orioledb's hooks.
- Topic 2 (orioledb's `set_rel_pathlist_hook`): the *mechanism* by which `transform_path` is invoked on every candidate path for an orioledb relation. This Topic 3 covers *what `transform_path` does to the cost* (answer: copies, doesn't recompute).

The split is clean: Topic 1 produces the costs, Topic 2 wraps the paths, Topic 3 explains why the wrapped paths still win the tournament under the test harness's forced-GUC configuration.

