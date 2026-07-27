# bgwriter-worker-slot-exhaustion-silent

## Focus

Resource Boundaries / Infrastructure faults — filling the gap flagged by
evaluation: "no property covers ... process-count exhaustion." This targets
`max_worker_processes` exhaustion for OrioleDB's *statically*-registered
background writer worker(s), a different registration mechanism (and
different failure mode) from the *dynamically*-registered recovery/index-build
worker pools covered by `recovery-idxbuild-registration-fallback-bug`.

## What led to this

`register_bgwriter()` (`src/workers/bgwriter.c:45-62`) is called
`bgwriter_num_workers` times from `_PG_init()` (`src/orioledb.c:1241-1243`,
inside `shared_preload_libraries` loading, i.e. in the postmaster before it
starts accepting connections):

```c
void
register_bgwriter(int num)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = 0;
    worker.bgw_main_arg = Int32GetDatum(num);
    strcpy(worker.bgw_library_name, "orioledb");
    strcpy(worker.bgw_function_name, "bgwriter_main");
    ...
    RegisterBackgroundWorker(&worker);
}
```

This calls Postgres's **static** background-worker registration API
(`RegisterBackgroundWorker`, not `RegisterDynamicBackgroundWorker`) — the same
unpatched, stock Postgres mechanism used by dozens of third-party extensions
(this call is not among the OrioleDB-specific core patches `sut-analysis.md`
enumerates; it was not re-verified in `/Users/artur/supabase/orioledb_postgres`
per this pass's scope restriction, since it's a plain, well-documented upstream
API, not a customized hook). `RegisterBackgroundWorker()` returns `void` — it
gives the caller **no way to know whether a slot was actually available**.
Per Postgres's long-standing, documented behavior for `bgw_start_time =
BgWorkerStart_PostmasterStart` workers registered during preload: if
`max_worker_processes` doesn't have enough free slots for every
statically-registered worker across every loaded library, Postgres logs a
`WARNING` ("too many background workers", with a hint to increase
`max_worker_processes`) for the workers that don't fit, and **the postmaster
still starts successfully** — it does not refuse to start, and it does not
retry later. The workers that didn't get a slot simply never run, for the
lifetime of that postmaster.

`bgwriter_num_workers` defaults to `1` (min 1, max `MAX_BACKENDS`,
`PGC_POSTMASTER`; `src/orioledb.c:797-808`), so by default only one slot is
needed — a much smaller ask than the recovery-worker pools' default of 6
(`recovery_pool_size` + `recovery_idx_pool_size`, both default 3), but the
mechanism and the blast radius if it fails are qualitatively different and
worse: **there is no fallback code path at all**. Contrast with the recovery
pools, which explicitly check `handle == NULL` and log an OrioleDB-specific
WARNING plus fall back to a documented degraded mode
(`recovery-idxbuild-registration-fallback-bug`'s evidence file). Here, if the
bgwriter's slot request is denied, OrioleDB has **zero visibility into the
failure** — the only signal is a generic, one-time Postgres startup log line
that doesn't mention OrioleDB by name, easy to miss among other startup
noise, and nothing in OrioleDB re-checks or retries.

### Why losing the bgwriter is not merely "one less writer thread"

`sut-analysis.md` §5 already flags: "bgwriter undo-location advancement for
replica cleanup has a single named point of responsibility (`BGWriterNum ==
0`, `src/workers/bgwriter.c:200-214`) — if that specific worker stalls,
crashes, or is disabled (`orioledb.debug_disable_bgwriter`), the claimed
replica-cleanup liveness may not hold." Reading the code directly confirms
the mechanism: inside the main per-undo-type loop
(`bgwriter.c:190-214`), when eviction isn't currently needed, **only the
worker whose `BGWriterNum == 0`** calls `update_min_undo_locations(...)`
— the comment explicitly says "Only first bgwriter does this to avoid
unnecessary concurrency" and warns "Without this, minProcRetainLocation set
during recovery may never be advanced on a synced replica." If *no* bgwriter
ever starts (registration denied at postmaster start), this call never
happens from *any* process — not degraded throughput, but complete absence
of a liveness-critical undo-retention-advancement mechanism, for the entire
lifetime of the instance, with the only trace being a generic startup-time
WARNING that was already there before OrioleDB even entered the picture (it's
a stock Postgres bgworker-subsystem message, not something OrioleDB adds
context to).

`orioledb.debug_disable_bgwriter` already exists as a *deliberate* way to
reach the "no bgwriter running" state for testing (`bgwriter_main()` checks it
and exits promptly, per `bgwriter.c:110-113`) — but that's a clean,
intentional, single-worker-at-a-time shutdown. Slot exhaustion is a
different, *accidental* way to reach the same end state (zero bgwriters ever
running, for every configured `bgwriter_num_workers`, not just one), triggered
purely by resource-limit misconfiguration or contention with other
`max_worker_processes` consumers (other extensions, `max_parallel_workers`,
the recovery pools above), not by a deliberate GUC.

## Property

| | |
|---|---|
| **Type** | Liveness (silent degradation), with a Reachability companion |
| **Property** | If `max_worker_processes` is insufficient to grant a background-worker slot to every one of OrioleDB's statically-registered bgwriter workers at postmaster start, the instance still starts (matching stock Postgres's documented "skip the worker, log a warning, keep running" behavior) — but OrioleDB itself never running any bgwriter for the rest of that postmaster's lifetime is neither retried nor surfaced as an OrioleDB-specific, easily alertable condition, and specifically the `BGWriterNum == 0` replica-undo-retention-advancement responsibility (`update_min_undo_locations`) never executes at all, not merely less often. |
| **Invariant** | `Sometimes(zero_bgwriters_running_after_postmaster_start)` to confirm the exhaustion condition is actually reached (set `max_worker_processes` below what's needed given the concurrently active recovery pools + `bgwriter_num_workers`, confirm via `pg_stat_activity`/`pg_stat_progress_...`-style introspection or a direct process-count check that no `"orioledb background writer %d"`-named process ever appears), paired with `Always(minProcRetainLocation_advances_within_bound_when_at_least_one_bgwriter_is_running)` as the contrasting positive control, and `Sometimes(minProcRetainLocation_stalls_permanently_when_zero_bgwriters_running)` to make the degraded state's actual, observable consequence (unbounded undo retention / undo-file growth) checkable rather than merely inferred from "the process isn't in the list." |
| **Antithesis Angle** | A pure resource-limit workload, no fault injection needed to *trigger* the condition: configure `max_worker_processes` low enough (or hold other slots busy via `max_parallel_workers`/other extensions/the recovery pools above) that OrioleDB's `bgwriter_num_workers` slot request(s) are denied at startup, then run sustained DML + periodic checkpoints + (ideally, given the "replica" framing in the source comment) a streaming standby, and watch whether undo retention/file size on the standby grows unboundedly compared to a control run with the bgwriter present. Antithesis's fault injection adds value on top by also covering the *dynamic* loss of the sole running bgwriter (kill the specific PID mid-run) as an equivalent, runtime-triggered variant of the same "zero bgwriters running" state. |
| **Why It Matters** | This is exactly the "does the system degrade safely if a required OrioleDB worker can't be spawned" question the evaluation gap explicitly asks about — the honest answer found by reading the code is: *it degrades, but silently and without OrioleDB's own knowledge*, unlike the dynamic recovery-worker pools which at least detect and log their own failure. A user or automated deployment tool who sets `max_worker_processes` without accounting for OrioleDB's static + dynamic worker needs (bgwriter(s) + up to 6 recovery workers by default + rewind/S3 workers if enabled, though those are out of this catalog's scope) gets a running, seemingly healthy instance whose replica undo-cleanup liveness guarantee has quietly stopped holding, discoverable today only by noticing unbounded undo growth well after the fact. |

**Open Questions:**

- Does Postgres's static-bgworker-registration slot-exhaustion behavior
  ("skip the worker, WARNING, keep running the postmaster") hold identically
  in the patched Postgres core this project depends on, or could the patch
  surface (checkpointer/startup-process changes documented in
  `sut-analysis.md` §3) have altered background-worker slot accounting in a
  way this analysis didn't check? This pass deliberately did not consult
  `/Users/artur/supabase/orioledb_postgres` per the stated scope restriction,
  and relies instead on well-established, long-documented stock PostgreSQL
  `bgworker.c` behavior used identically by many third-party extensions.
  `(needs human input / needs a live repro to confirm definitively against this specific patched build)`
- Is there any other, indirect signal (a metric, a `pg_stat_activity` row
  count, a periodic self-check) by which an operator or monitoring tool
  *could* detect "zero orioledb bgwriters running" today, short of grepping
  startup logs for the one-time generic WARNING? Not found in `doc/` or
  `src/` this pass — assumed absent, but not exhaustively confirmed.
- Precisely how large is the practical exposure — does any single other
  common `max_worker_processes` consumer (parallel query workers, logical
  replication workers, another popular extension) plausibly coexist with
  OrioleDB's default worker footprint (`bgwriter_num_workers=1` +
  `recovery_pool_size=3` + `recovery_idx_pool_size=3` = up to 7 slots) closely
  enough that a realistic deployment could hit this without deliberately
  misconfiguring `max_worker_processes`? Not measured — flagged as relevant
  to how aggressively Antithesis should need to constrain the config to reach
  this state versus how likely it is "in the wild."

## SUT-side instrumentation

`existing-assertions.md`: 0 hits in `src/workers/bgwriter.c` or anywhere in
`src/`/`include/` (**missing**). Suggested (both **missing**):
- `Reachable("orioledb bgwriter started", {BGWriterNum})` at
  `bgwriter_main()`'s startup (`bgwriter.c:75` onward) — combined with a
  workload-side `Sometimes(max_worker_processes constrained and this was
  never observed)` check, gives a direct, positive-or-negative signal for
  "did any bgwriter actually start this run" without needing to parse
  Postgres startup logs for the generic WARNING.
- A workload-side periodic sample of `minProcRetainLocation`/undo-file size
  (there is no existing SQL-exposed view read in this pass that surfaces
  `minProcRetainLocation` directly — worth checking whether
  `orioledb_get_evicted_trees()`/a similar existing SQL function already
  exposes it, or whether a new one is needed) to make the liveness
  consequence (not just the process's absence) directly checkable.

### Investigation Log

#### Does Postgres's static-bgworker-registration slot-exhaustion behavior hold identically in the patched Postgres core this project depends on?

- Examined: `src/workers/bgwriter.c` (`register_bgwriter`), `src/orioledb.c` (`_PG_init`), stock PostgreSQL's documented `bgworker.c` slot-exhaustion behavior (not `orioledb_postgres` directly, per scope restriction).
- Found: `register_bgwriter()` uses the plain, unpatched `RegisterBackgroundWorker()` API, identical to how many third-party extensions register static workers — well-documented stock behavior, not an OrioleDB-specific hook.
- Not found: whether the patched checkpointer/startup-process changes noted in `sut-analysis.md` §3 altered background-worker slot accounting in this specific patched build — not verified against the actual patched core.
- Conclusion: tagged `(needs human input / needs a live repro to confirm definitively against this specific patched build)` — the stock-behavior assumption is reasonable but unverified against the actual patch.
