# recovery-worker-commit-visibility-barrier

## Status

**Open lead, not yet in the catalog before this pass.** New property filling
Task A, item 2, of a follow-up gap-filling pass: the doc claim at
`doc/architecture/overview.mdx:153` — "we assume the transaction to be
committed and visible for readers only once all the workers have completed
all the pieces of work associated with that transaction" — was named in
`sut-analysis.md` §4 as a claimed guarantee that was never turned into a
catalog property. This file turns it into one. Not a regression target for a
known-fixed bug; this is a **claimed guarantee** in the "validating external
claims" sense — treated here as a claim to test, per `validating-claims.md`,
not a verified fact, even though the enforcement mechanism was traced and
found to be real (see below).

## The doc claim, in full context

`doc/architecture/overview.mdx` (~line 145-153):

> OrioleDB implements parallel application of WAL records. It launches
> `orioledb.recovery_pool_size` number of workers. Each worker is responsible
> for its own set of primary key values (according to hash value). The
> startup process distributes row-level WAL records to the queues connected
> to workers.
>
> Queues might be processed at different paces. In order to avoid MVCC
> anomalies, we assume the transaction to be committed and visible for
> readers only once all the workers have completed all the pieces of work
> associated with that transaction.

This matters because a single transaction's row-modify WAL records can be
routed to *different* recovery workers (routing is `hash(PK) %
recovery_pool_size_guc` per row), and those workers' queues drain at
different, independent paces (`src/recovery/worker.c`). If the commit's
visibility were gated on anything less than "every worker that touched a
piece of this transaction has actually applied its piece," a reader could
observe a transaction as fully committed while one of its rows hasn't
actually been replayed yet — a genuine MVCC anomaly (a torn/partial
transaction visible as whole).

## The actual enforcement mechanism (traced directly, not just inferred from the doc)

This investigation dispatched a focused code-reading pass to answer: is this
a real, per-transaction completion barrier, an emergent side effect of some
other mechanism, or unenforced? Findings, with file:line citations:

- On replay of `WAL_REC_COMMIT`/`WAL_REC_ROLLBACK`, `recovery_finish_current_oxid()`
  (`src/recovery/recovery.c:2047-2150`) does **not** write the transaction's
  final CSN inline for the common (non-sync) case — it pushes the oxid onto a
  `finished_list` (`recovery.c:2074-2087`), deferring the actual commit.
- The real CSN write happens later, in `update_proc_retain_undo_location()`
  (`recovery.c:2752-2825`), which drains `finished_list` entries whose
  `state->ptr <= listPtr`, where `listPtr = recovery_get_current_ptr()` calls
  `get_workers_commit_ptr()` (`recovery.c:1312-1366`) — **the minimum
  `commitPtr` across every recovery worker** (`recovery.c:1327-1350`). Only
  once that global-minimum WAL position has advanced past the commit record's
  LSN does `set_oxid_csn(oxid, COMMITSEQNO_COMMITTING)` run, followed by the
  real CSN (`recovery.c:2797-2799`).
- Each worker's own `commitPtr` (`worker.c:227,348-356`) only advances when
  that worker processes a commit/rollback/synchronize message for an oxid it
  actually participated in (`worker.c:584-613`), or — for a worker not
  involved in this particular transaction — opportunistically, when its
  queue drains empty and it catches up to the leader's own dispatch pointer
  (`worker.c:727-741`).
- Readers spin-wait on a transient `COMMITSEQNO_COMMITTING` marker in
  `oxid_get_csn()` (`oxid.c:1820-1839`), so no reader can observe a
  half-written CSN mid-transition — but nothing about that spin-wait is
  what enforces cross-worker completion; it only prevents observing a torn
  *write* of the CSN value itself.

**Conclusion: this is a real, code-enforced barrier, but implemented as
global WAL-position synchronization across every worker, not a per-oxid
"pieces remaining" counter or targeted ack from specifically the workers
that touched this transaction.** `used_by[i]` bookkeeping (`recovery.c:100-160,4985`)
only routes the commit/rollback control *message* to participating workers —
it is never consulted to determine "have all participants finished," which
is instead an emergent, over-conservative consequence of gating on the
*slowest* worker's commit pointer (participating or not). It works
correctly as a side effect: a worker's `commitPtr` can only cross a given
LSN once it has processed everything dispatched to it up to that point, so
gating the drain on the globally-slowest worker transitively guarantees
every actually-participating worker is also done — at the cost of also
waiting on workers that had nothing to do with this specific transaction.

## Property

| | |
|---|---|
| **Type** | Safety |
| **Property** | A recovering/replaying process (crash recovery or a streaming standby) never marks an oxid's CSN as committed (transitioning out of `COMMITSEQNO_INPROGRESS`/`COMMITSEQNO_COMMITTING`) before every recovery worker's `commitPtr` has advanced past that commit record's WAL position — i.e., the `finished_list`/`get_workers_commit_ptr()` deferred-CSN-write mechanism in `update_proc_retain_undo_location()` never releases a transaction's visibility ahead of the slowest worker actually finishing replay of everything dispatched up to that LSN, matching the doc's claimed guarantee. |
| **Invariant** | `Always`: needs new SUT-side instrumentation, since "CSN release timing vs. actual per-worker completion" isn't observable from SQL alone. Suggested: instrument `update_proc_retain_undo_location()`'s drain loop (`recovery.c:2752-2825`) to assert, at the moment an oxid is drained from `finished_list`, that every worker index in that oxid's `used_by[]` set has a `commitPtr >= this oxid's commit-record LSN` — an `Always()` directly on the invariant the mechanism is supposed to guarantee, rather than waiting to observe a violation externally (which, per the mechanism traced above, would show up as a reader seeing a partially-applied transaction's rows as already visible). |
| **Antithesis Angle** | Needs multiple recovery workers actually receiving pieces of the *same* transaction (a multi-row transaction with PK values hashing to different workers — deliberately construct or just use enough distinct keys per transaction and enough workers that this is a near-certainty) combined with scheduling-delay/CPU-throttling fault injection targeting one specific worker to maximize the leader-vs-slowest-worker skew right as the commit record is processed. A concurrent reader thread repeatedly checking whether all rows of a known transaction are visible together (never some-but-not-all) is the natural client-observable form of this property, complementing the SUT-side instrumentation above. |
| **Why It Matters** | If this barrier were ever weakened — e.g., a future optimization that tries to shrink the "wait for the globally slowest worker" cost by gating on only the participating workers' `used_by[]` set, and gets the completion check wrong — a reader could observe a transaction as fully committed while one of its rows hasn't actually replayed, a genuine MVCC anomaly (the exact failure class `sut-analysis.md` §10 calls the worst-case for a database engine) that the current, more conservative implementation happens to prevent as a side effect rather than by direct design intent per transaction. |

**Open Questions:**

- Is there any existing stopevent or test that specifically targets "reader sees a transaction as committed while one of its rows, dispatched to a different worker, hasn't actually replayed yet"? None found — `test_checkpoint_snapshot_resurrects_aborted_oxid` (`test/t/replication_test.py`) uses the `replay_on_record` stopevent to force worker-commit-pointer lag, but for a different purpose (forcing a stale undo-location PANIC), not for observing a premature-visibility MVCC anomaly. `(partial: adjacent test coverage found, not this specific anomaly)`
- Historical precedent: commit `0cf76e17` ("Fix visibility of xids provided by checkpoint file in recovery workers") is a real, previously-fixed bug in `workers_send_oxid_finish()`'s participant-notification logic — confirms this exact code area (worker participant notification tied to commit visibility) has produced real bugs before, strengthening the case that this is a legitimate area to test directly rather than assume correct. Was this specific fix's coverage (if any) ever converted into a permanent regression test, or does the class of bug it fixed remain untested going forward? `(needs further investigation — the fix's own test coverage, if any, was not identified in this pass)`
- Since the mechanism is global-WAL-position-gated rather than per-oxid-participant-gated, is there a plausible fault sequence where a *non-participating* worker's `commitPtr` somehow advances past the commit LSN before a truly *participating* worker's does — which would break the "transitively guarantees participants are done" argument the current implementation relies on? Not identified as reachable in this pass (the per-worker `commitPtr` advancement is itself sequential per worker's own queue position), but not exhaustively ruled out either. `(needs further investigation)`

## Distinctness from adjacent catalog properties (explicitly checked per task instructions)

- **`recovery-worker-stall-blocks-leader.md`**: covers the *leader* blocking
  on `worker_queue_flush()`/`workers_synchronize()` when distributing WAL
  records to a wedged worker — a distribution-side backpressure/liveness
  concern with no per-transaction visibility semantics at all. This property
  is about a *different* mechanism entirely: the *consumption*-side deferred
  CSN-write gate, which is a safety (correctness) concern, not a liveness one.
- **`replica-globalxmin-catchup-lag.md`**: covers `globalXmin`/`runXmin`
  horizon bookkeeping (a separate concept — the oldest-still-needed-snapshot
  watermark) and its own catch-up/monotonicity properties, tracked
  independently of per-oxid CSN visibility. Confirmed distinct: `globalXmin`
  tracking and the `finished_list`/`get_workers_commit_ptr()` CSN-release
  path are different data structures serving different purposes, even though
  both live in `recovery.c` and both involve per-worker progress tracking.

## SUT-side instrumentation

`existing-assertions.md` confirms zero assertions in `src/recovery/`. This
property is a strong candidate for the "needs SUT-side instrumentation"
category per `references/property-catalog.md`'s guidance: the dangerous state
(a reader observing a transaction as committed before every worker touching
it has actually replayed) is timing-sensitive and not directly observable
from a SQL client without either (a) instrumenting the drain loop directly
(suggested above), or (b) a very deliberately constructed client-side probe
that knows in advance exactly which rows belong to which worker.

## Investigation Log

#### Is this a true per-transaction completion barrier, an emergent side effect, or unenforced?

- Examined: `src/recovery/recovery.c` (`recovery_finish_current_oxid`,
  `update_proc_retain_undo_location`, `get_workers_commit_ptr`,
  `recovery_get_current_ptr`), `src/recovery/worker.c` (`commitPtr`
  advancement, queue-catch-up logic), `src/transam/oxid.c` (`oxid_get_csn`'s
  `COMMITSEQNO_COMMITTING` spin-wait).
- Found: a real, code-enforced barrier, implemented as global
  minimum-commit-pointer gating across all workers (not a targeted per-oxid
  participant ack). Historical bug `0cf76e17` confirms this area has broken
  before.
- Not found: any existing test or stopevent directly targeting the
  visibility-ordering anomaly itself (as opposed to adjacent crash/PANIC
  scenarios that happen to use the same worker-lag-forcing stopevent).
- Conclusion: legitimate, distinct, safety-typed property; requires new
  SUT-side instrumentation to be checkable with precision, since the
  over-conservative nature of the current mechanism (gating on ALL workers,
  not just participants) means an external client-only observation would
  need to get lucky with timing to ever catch a violation even if one
  existed.
