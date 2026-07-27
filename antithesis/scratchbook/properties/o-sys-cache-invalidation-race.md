# o-sys-cache-invalidation-race

## Focus

Concurrency (attention focus 2). Lower-confidence / more exploratory than the other properties in this pass — flagged in `sut-analysis.md` §1 ("Non-transactional catalog cache synced via syscache invalidation (`src/catalog/o_sys_cache.c:425`) — a backend seeing a stale table descriptor would show up if invalidation delivery races with DDL commit + WAL") and given a lighter follow-up look here.

## What was examined

`src/catalog/o_sys_cache.c` implements a second, OrioleDB-owned cache ("Generic interface for sys cache duplicate trees" per the file header) that duplicates rows from Postgres catalog tables (`pg_type`, `pg_opclass`, `pg_amop`, `pg_enum`, etc. — the full `#include` list at the top of the file names most core catalogs) into OrioleDB's own B-tree-backed storage, plus a small in-memory "fast cache" front-end (`OSysCacheHashEntry`/`last_fast_cache_entry`).

Invalidation wiring found:

- `orioledb_setup_syscache_hooks()` (`o_sys_cache.c` ~line 410) registers `orioledb_syscache_hook` via `CacheRegisterSyscacheCallback(sys_cache->cacheId, ...)` for every registered `OSysCache` — i.e., it piggybacks on Postgres's own standard catcache invalidation delivery mechanism (the same `SharedInvalBackendInit`/`AcceptInvalidationMessages` machinery every backend already uses), rather than inventing a new one.
- `orioledb_syscache_hook()` → `invalidate_fastcache_entry()` clears the small in-memory fast-path pointer (`sys_cache->last_fast_cache_entry`) when a matching invalidation arrives, forcing the next lookup to go through the full tree-backed cache path instead of the O(1) fast pointer.
- `o_sys_cache_search()` (the actual lookup entry point) checks the fast-cache pointer first, falls back to the tree-backed cache search on a miss.
- Found `Assert(!is_recovery_in_progress())` guarding `o_sys_cache_delete_callback()` (an undo callback), suggesting this particular cache's mutation/undo path is asserted not to run during recovery — recovery worker interaction with this cache is likely funneled through a different mechanism (probably `o_invalidate_oids()`, used pervasively elsewhere for OrioleDB's own table/index metadata cache, a related but distinct cache from this one) rather than the syscache-callback path examined here. This distinction was not fully resolved (see Open Questions) — it matters because it determines whether recovery replay of DDL is even in-scope for this specific race, or whether the race (if real) is confined to live-backend DDL-commit-vs-invalidation-delivery timing only.

This is standard Postgres catcache-invalidation-delivery timing (invalidation messages are only guaranteed processed at the next `AcceptInvalidationMessages()` call, typically at transaction/statement start) layered under a second cache that duplicates the same underlying data — so the interesting question isn't "does Postgres's own invalidation have a race" (a much-hardened, decades-old mechanism) but "does this second, OrioleDB-specific cache correctly participate in that same delivery-timing contract, or does its own fast-path pointer (`last_fast_cache_entry`) introduce a staleness window Postgres's own catcache doesn't have" — e.g., if the fast pointer is read without re-checking invalidation state at a point ordinary catcache lookups would.

## Honest assessment of confidence

This property is speculative relative to the other four in this pass — it is built primarily on the SUT analysis's original one-line flag plus a shallow follow-up read of `o_sys_cache.c`'s invalidation wiring, not a traced concrete failure scenario. It's included because the task's Concurrency focus explicitly calls for "thread-safety assumptions in documentation vs. implementation," and a second cache layered on top of Postgres's own invalidation-delivery contract is exactly the shape of thing worth flagging even without a fully worked failure mechanism yet.

## The property

**Type:** Safety.

**Property:** A backend never observes a table/type/catalog descriptor via `o_sys_cache_search()`'s fast-path (`last_fast_cache_entry`) that is stale relative to a concurrently-committed DDL change on the same object — i.e., the fast-cache short-circuit never returns data older than what a full catcache lookup would return at the same logical point.

**Invariant:** `Always` — but this needs a concrete, checkable formulation before it's implementable, which this pass did not fully produce. A workable version: run concurrent DDL (`ALTER TYPE`, `CREATE OPERATOR CLASS`-adjacent changes, etc. — whatever underlies the specific `OSysCache` entries registered) against one backend while a second backend repeatedly queries/uses the affected object, and assert the second backend's observed definition is never older than the last DDL commit it has otherwise causally observed (e.g., via a value written and committed by the first backend, then read-after-write by the second through a different channel).

**Antithesis Angle:** Concurrent DDL + concurrent DML/queries touching the same catalog objects, with Antithesis's scheduling-fault injection targeting the exact window between a DDL transaction's commit and the invalidation message actually being processed by other backends (`AcceptInvalidationMessages()` timing) — the interesting adversarial case is a backend that delays entering its next transaction/statement (and thus delays processing queued invalidations) for as long as possible while still using stale fast-cache data for an operation that should see the new definition.

**Why It Matters:** If real, this would be a silent-wrong-behavior bug (using a stale type/catalog definition), which per `sut-analysis.md` §10 is the worst class of failure for a database engine — but this pass could not confirm the mechanism is actually exploitable (as opposed to Postgres's own well-tested invalidation-delivery contract already fully covering this second cache's correctness, which is plausible given it reuses the same `CacheRegisterSyscacheCallback` primitive rather than inventing new invalidation logic).

**Open Questions:**

- Does `o_sys_cache_search()`'s fast-path check anything beyond pointer/key equality before trusting `last_fast_cache_entry` — i.e., is there any staleness check at all, or does it rely entirely on `invalidate_fastcache_entry()` having already cleared the pointer by the time of the read? If invalidation delivery and the fast-path read aren't ordered by the same mechanism ordinary catcache reads use, this could be a real gap; if they are (e.g., the fast-cache clear happens inside the same invalidation-processing pass as catcache's own clears), the property may already be vacuously true. Not resolved in this pass. `(needs human input or a deeper trace of `o_sys_cache_search()`'s full body, which was only partially read here.)`
- How do recovery workers interact with this cache during WAL replay of DDL — do they bypass it entirely (per the `Assert(!is_recovery_in_progress())` seen in the delete-undo callback, suggesting yes for at least that path), or does some other part of `o_sys_cache.c` participate in recovery replay in a way this pass didn't find? This determines whether the property's scope is "live DDL vs. live DML only" or also includes a recovery-replay angle. `(partial: one data point found suggesting recovery bypasses this specific callback; not a full trace.)`

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No assertion touches `o_sys_cache_search`, `invalidate_fastcache_entry`, or any catalog-cache staleness scenario anywhere in `test/antithesis/`. Given the confidence caveats above, a first useful SUT-side addition would be a `reachable()`/counter at `invalidate_fastcache_entry()`'s clear point and at `o_sys_cache_search()`'s fast-path hit, to establish (from real run data) how often the fast path is actually exercised under concurrent DDL before investing further in this property's precise mechanism.

### Investigation Log

#### Does `o_sys_cache_search()`'s fast-path check anything beyond pointer/key equality before trusting `last_fast_cache_entry`, or does it rely entirely on `invalidate_fastcache_entry()` having already cleared the pointer?

- Examined: `orioledb_setup_syscache_hooks()` (registers `orioledb_syscache_hook` via `CacheRegisterSyscacheCallback`), `orioledb_syscache_hook()` → `invalidate_fastcache_entry()`, and `o_sys_cache_search()`'s fast-path-then-fallback structure (`o_sys_cache.c`).
- Found: the fast cache is invalidated through the same standard Postgres catcache invalidation-delivery mechanism (`CacheRegisterSyscacheCallback`) every backend already uses, rather than a bespoke one; `o_sys_cache_search()` checks the fast pointer first and falls back to the tree-backed cache on a miss.
- Not found: `o_sys_cache_search()`'s full body was only partially read — whether it performs any additional staleness check beyond trusting the pointer was not confirmed either way.
- Conclusion: tagged `(needs human input or a deeper trace of o_sys_cache_search()'s full body, which was only partially read here.)`.

#### How do recovery workers interact with this cache during WAL replay of DDL — do they bypass it entirely, or does some other part of `o_sys_cache.c` participate in recovery replay?

- Examined: `o_sys_cache_delete_callback()` (an undo callback) in `o_sys_cache.c`.
- Found: `Assert(!is_recovery_in_progress())` guards `o_sys_cache_delete_callback()`, suggesting this cache's mutation/undo path is asserted not to run during recovery — recovery is likely funneled through the separate `o_invalidate_oids()` mechanism instead.
- Not found: a full trace confirming no other part of `o_sys_cache.c` participates in recovery replay of DDL.
- Conclusion: tagged `(partial: one data point found suggesting recovery bypasses this specific callback; not a full trace.)`.
