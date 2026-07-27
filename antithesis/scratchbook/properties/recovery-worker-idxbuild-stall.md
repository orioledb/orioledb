# recovery-worker-idxbuild-stall

## Focus

Concurrency (attention focus 2), extending `sut-analysis.md` §3's note: "Recovery worker parallel index build: workers wait on `recovery_index_cv` for an index-build leader; no timeout observed in that wait path (open question — a stuck leader could stall other recovery workers indefinitely)."

## What was checked

`src/recovery/recovery.c:4647-4688`, function `delay_if_queued_for_idxbuild()`:

```c
static void
delay_if_queued_for_idxbuild(void)
{
	while (idxbuild_oids_hash)
	{
		...
		if (AmStartupProcess())
			ProcessStartupProcInterrupts(); /* or HandleStartupProcInterrupts() pre-PG18 */
		else
			o_worker_handle_interrupts();

		/* Remove hash entries for completed indexes */
		hash_seq_init(&hash_seq, idxbuild_oids_hash);
		while ((cur = ...) != NULL)
			if (cur->position <= pg_atomic_read_u64(recovery_index_completed_pos))
				hash_search(idxbuild_oids_hash, &cur->oids, HASH_REMOVE, NULL);

		if (hash_get_num_entries(idxbuild_oids_hash) == 0)
			break;

		ConditionVariableTimedSleep(recovery_index_cv, 1000,
									WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}
	ConditionVariableCancelSleep();
}
```

This refines (rather than simply confirms) the SUT analysis's framing. There **is** a timeout on the individual `ConditionVariableTimedSleep` call (1000ms), and the loop re-checks interrupts (`HandleStartupProcInterrupts`/`o_worker_handle_interrupts`) every iteration — so this is not literally an unbounded, uncancellable block; a `SIGTERM`/query-cancel/config-reload can still be observed and processed. But the *outer* `while (idxbuild_oids_hash)` loop has no give-up bound: if `recovery_index_completed_pos` (advanced only by the leader/index-build worker via `worker.c:530-532`, `pg_atomic_write_u64(...); ConditionVariableBroadcast(recovery_index_cv);`) never reaches the position this worker is waiting on, the loop polls forever at 1-second granularity. This is a genuine liveness gap under the exact condition the SUT analysis named — a leader/index-build worker that crashes, hangs, or is delayed indefinitely (e.g., itself blocked on another lock, or killed by fault injection) without ever advancing `recovery_index_completed_pos` — but it is a "spin-poll with escape valve for signals" pattern, not a hard hang; worth stating precisely rather than as an unqualified "unbounded, unkillable stall."

Called from `recovery_finish()` (`recovery.c:1704`, `delay_if_queued_for_idxbuild();` — the very first line) and from the recovery worker path per the AmStartupProcess()/else branching, i.e. both the startup/leader process and ordinary recovery workers can be the one waiting.

## The property

**Type:** Liveness (progress) — with a Safety companion (the wait must be actually cancellable via signal, since that's the only escape valve).

**Property:** If a parallel index-build leader/worker fails to advance `recovery_index_completed_pos` past a position other recovery workers are delayed on (`delay_if_queued_for_idxbuild()`), those other workers do not block correctness indefinitely — either the stalled leader is itself detected and recovered from within a bounded time (no such mechanism was found in this pass — see Open Questions), or an external actor (operator, supervisor, Antithesis's own health/liveness expectations) can reliably interrupt and recover the whole recovery process via a signal, since the interrupt-check path is confirmed live.

**Invariant:** `Sometimes` for reaching the interesting contended state — `sometimes(recovery_worker_entered_idxbuild_wait_loop)` confirms a worker actually blocks in this loop (not just the common case of no pending parallel index build) — combined with `always(idxbuild_wait_resolves_or_interrupt_is_honored_within_bound)`: under fault injection that kills/delays the specific process responsible for `recovery_index_completed_pos` advancement, assert that either the wait resolves within a generous bound, or a subsequent interrupt (simulated cancel/restart) is honored promptly rather than the loop swallowing it.

**Antithesis Angle:** Requires a workload that triggers OrioleDB's parallel-recovery index build (concurrent index creation activity being replayed during recovery/on a standby) combined with process-level fault injection targeting specifically the index-build leader/worker process (kill it, or freeze its scheduling) mid-build, then observing whether the rest of recovery: (a) hangs forever with no operator-visible signal, (b) eventually times out/errors cleanly, or (c) responds correctly to an external interrupt. This is a good target for Antithesis's process-kill fault primitive specifically, since it needs to selectively target one process among several cooperating ones (the recovery worker pool) rather than the whole container.

**Why It Matters:** A stuck recovery process blocks all subsequent WAL replay for that instance — on a standby, this means replication falls further and further behind (or stalls entirely) with no automatic recovery, which is a serious availability degradation that (unlike the S3-mode hangs already known per `sut-analysis.md` §6) has not been previously flagged as a lead anywhere in the existing analysis.

**Open Questions:**

- Is there any mechanism (timeout, leader-liveness check, watchdog) elsewhere in the recovery worker supervision code that would detect and recover from a permanently-stalled index-build leader, which this specific function's code doesn't show? Not found in this pass — searched only `delay_if_queued_for_idxbuild()` and its immediate call sites; a broader search of `src/recovery/worker.c`'s process-supervision code (worker restart/respawn logic, if any) was not performed. `(partial: confirmed no such mechanism inside the wait function itself; broader worker-supervision code not checked)`.
- Whether Postgres's own background-worker infrastructure (which OrioleDB's recovery workers are built on, per `sut-analysis.md` §1) provides any automatic detection of a hung/crashed worker that would break this specific spin independent of the code in `recovery.c` — this is a patched-Postgres question (`/Users/artur/supabase/orioledb_postgres`) not investigated in this pass.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing.** No assertion touches `delay_if_queued_for_idxbuild`, `recovery_index_cv`, or `recovery_index_completed_pos` anywhere in `test/antithesis/`. A SUT-side counter/assertion recording how long a given wait iteration count grew to (or a `reachable()` at the point the outer `while` loop's iteration count crosses some high-iteration threshold) would give visibility into a state that's otherwise invisible from outside the process — the SQL client sees only "recovery/replication is behind," not why.

## No standby topology actually required (correction, evaluation R12)

This property's framing above ("during recovery/on a standby") should not be
read as implying a standby is *necessary* to reach it. Confirmed via
Implementability: `orioledb.recovery_pool_size`/`orioledb.recovery_idx_pool_
size` both default to 3 (`PGC_POSTMASTER`), so parallel recovery workers and
the parallel index-build sub-pool are already active during **ordinary
single-node crash recovery**, with no config change and no second Postgres
node. The catalog entry's Antithesis Angle has been corrected accordingly —
this is reachable in the existing single-node harness today; a future
standby would additionally let the *consequence* (replication falling
behind) be observed, but isn't required to reach the underlying wait-loop
condition itself.

## Cross-cutting pattern (added by evaluation pass, R14)

One of four properties sharing the "unbounded busy-wait, no
`CHECK_FOR_INTERRUPTS()`"-shaped gap identified by the Wildcard evaluation
lens (the others: `sk-fixup-sentinel-spin-livelock`, `recovery-worker-
stall-blocks-leader`, `checkpointer-startup-lock-drain-progress` — see
`property-relationships.md` Cluster 11). Unlike its cluster-mates, this
specific wait loop's inner `ConditionVariableTimedSleep` does call
`CHECK_FOR_INTERRUPTS()` each iteration (a 1s bound), so the more precise
gap here is the *outer* loop's lack of a give-up bound, not raw
uninterruptibility. A cheaper first test than full process-freeze fault
injection: `pg_cancel_backend()`/`statement_timeout` targeted at the
backend/session driving the stalled workload, confirming the outer loop's
lack of bound is observable directly via a cancel request that never
resolves, before investing in the fuller kill/freeze-the-leader scenario
above.

### Investigation Log

#### Is there any mechanism (timeout, leader-liveness check, watchdog) that would detect and recover from a permanently-stalled index-build leader?

- Examined: `delay_if_queued_for_idxbuild()` (`src/recovery/recovery.c:4647-4688`) and its immediate call sites (`recovery_finish()` at `recovery.c:1704`).
- Found: the wait function itself has only a per-iteration 1000ms `ConditionVariableTimedSleep` with interrupt handling each pass; the outer `while (idxbuild_oids_hash)` loop has no give-up bound and no watchdog of its own.
- Not found: broader `src/recovery/worker.c` process-supervision code (worker restart/respawn logic, if any) was not searched.
- Conclusion: tagged `(partial: confirmed no such mechanism inside the wait function itself; broader worker-supervision code not checked)`.
