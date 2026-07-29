# chaos-driver-skips-check-on-fault-landing

> **Update (2026-07-29): resolved by design, not by patching the old file.**
> `test/antithesis/sk-recovery-race-chaos/` was retired and its checkpoint-
> timing coverage folded into `test/antithesis/sk-rebuild-desync/` (see
> `property-catalog.md`'s `recovery-sk-rebuild-desync` entry). The new
> workload's consistency check is not a single scripted burst-then-check —
> `anytime_sk-rebuild-desync-check` and `finally_sk-rebuild-desync-check`
> each independently open a fresh connection and run the check, so a
> connection lost during one `parallel_driver_` DML invocation no longer
> suppresses the *next* independent check the way chaos's single end-of-burst
> `assert_consistent` call used to. The systematic bias described below no
> longer applies to the current harness. Left in place as a historical
> record of the finding and the reasoning, not updated further.

## Focus

Wildcard (attention focus 12) — a property about the test harness's own verification logic, discovered by reading the most recent commit on this branch (`a975c702`, "sk-recovery-race-chaos/driver.py: Handle lost connection") rather than any single SUT focus area. This is the kind of "the fix for one problem quietly reintroduces a different, opposite-shaped problem" pattern the Wildcard lens is meant to catch.

## What led to this

The tip commit of this branch changes `test/antithesis/sk-recovery-race-chaos/driver.py` to gracefully handle a lost connection instead of crashing the workload container. Reading the current file (`driver.py`, full contents):

```python
CONNECTION_LOST_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

def dml_worker(stop_at, errors, index):
    ...
    try:
        while time.monotonic() < stop_at:
            try:
                ... # DML
                conn.commit()
            except CONNECTION_LOST_ERRORS:
                # Connection is gone - most likely Antithesis's own fault
                # injection killed the target mid-transaction. rollback()
                # would itself raise InterfaceError on a dead connection,
                # so don't attempt it; let this propagate as a graceful
                # exit instead of masking it into an unhandled crash.
                raise
            except psycopg2.Error:
                conn.rollback()
    except Exception as exc:
        errors[index] = exc
    ...

def dml_burst(ctl_conn):
    ...
    for t in threads:
        t.join(timeout=BURST_SECONDS + 30)
    for err in errors:
        if err is not None:
            raise err          # <-- propagates CONNECTION_LOST_ERRORS out of dml_burst()
    ...

def main():
    ctl_conn = None
    try:
        ctl_conn = connect("s_ctl")
        ensure_schema(ctl_conn)
        assert_consistent(ctl_conn, "startup")
        dml_burst(ctl_conn)                 # <-- can raise CONNECTION_LOST_ERRORS
        assert_consistent(ctl_conn, "post-burst")   # <-- SKIPPED if dml_burst raised
    except CONNECTION_LOST_ERRORS as exc:
        print(f"lost connection to target (likely fault injection landed "
              f"mid-burst), will re-check on next run: {exc}", flush=True)
        return                              # <-- clean, silent exit; no assertion fired
    finally:
        ...
```

Tracing the control flow: if **any** of the 8 `dml_worker` threads hits a `CONNECTION_LOST_ERRORS` exception, it's re-raised, stored in `errors[index]`, and then re-raised again by the `for err in errors: raise err` loop inside `dml_burst()`. That propagates up to `main()`'s `except CONNECTION_LOST_ERRORS`, which prints a message and **returns** — `assert_consistent(ctl_conn, "post-burst")` (the call that actually checks `pk_rows == sk_distinct` and `orioledb_tbl_check()`, i.e. the orioledb#855 invariant this entire workload exists to test) **never executes**.

This is a deliberate, reasonable-looking design choice (the commit's own comment explains the reasoning: don't call `rollback()` on a dead connection, don't mask a real problem as an "unhandled crash"). But the consequence is a coverage gap that isn't obviously visible from reading the diff in isolation: **the run "passes" (no assertion failure, no exception surfaces to Antithesis) for exactly the runs where a fault landed hard enough to kill a connection mid-burst** — and those are disproportionately likely to be the runs where Antithesis's fault injection was landing *near* the checkpoint/DML overlap this workload is designed to stress (a killed connection is a strong signal something disruptive — a backend kill, a container restart, an OOM event — happened in the target's process space right during the window of interest).

## The property

**Type:** Meta / Reachability — a property about the *verification coverage* of the existing workload, not about OrioleDB's data consistency directly.

**Property:** The chaos driver's post-burst consistency check (`assert_consistent(ctl_conn, "post-burst")`) is not systematically skipped specifically on the runs where a disruptive fault landed during the burst. Equivalently: the presence of a `CONNECTION_LOST_ERRORS` exception during a burst should not silently suppress the very assertion meant to catch corruption caused by that same class of disruption.

**Invariant:** Add a new, distinct signal that fires exactly in the branch that currently just prints-and-returns: `sometimes(connection_lost_during_burst, "sk-recovery-race-chaos burst was interrupted by a lost connection before post-burst consistency could be checked", {...})` — this doesn't fix the gap by itself, but makes it *visible and countable* rather than invisible, which is the prerequisite for deciding whether it matters (see Antithesis Angle). The stronger fix — reconnect and run `assert_consistent` anyway once the target is reachable again, so the invariant is actually checked rather than skipped — turns this into a real `always()` check instead of a `sometimes()` visibility marker; whether that's feasible depends on whether reconnecting after a target crash-restart lands on a data state where "post-burst" is even a meaningful label (the burst's in-flight transactions are gone either way, but the PK/SK state *before* the disruption is still exactly what #855 is about).

**Antithesis Angle:** This is entirely about whether Antithesis's own fault injection (which this workload depends on for chaos-style, non-deterministic race construction — the driver's docstring says so explicitly: "Antithesis's own fault injection has to both land near an automatic checkpoint AND overlap the DML burst for this variant to ever hit the race") is being *told about* the runs where it actually landed a hit. Right now, the workload has no way to distinguish, from Antithesis's outside view, "the race never got constructed this run" from "the race was quite possibly constructed and then verification was skipped." Both currently look identical: a clean exit with a log line, no assertion outcome recorded either way. Adding the `sometimes()` marker above lets Antithesis's own results correlate "did a connection-loss event happen" against "did we ever see a violation" — if the answer is "many connection-loss events, zero violations, and zero completed post-burst checks," that's a very different (and much weaker) confidence level than "many completed post-burst checks, zero violations."

**Why It Matters:** This workload exists specifically to extend #855 coverage into the chaos/fault-injection regime (as opposed to `sk-recovery-race`'s deterministic stopevent-pinned variant). If the exact fault conditions Antithesis is good at producing (process kills, scheduling delays severe enough to break a TCP connection) are also the conditions under which this driver silently opts out of checking anything, then the chaos variant may be providing much less additional assurance than its presence in the test suite implies — a false sense of "we're chaos-testing this" when the highest-signal runs are quietly unverified. This is the same shape of problem the property catalog's "Honest Summaries" section warns about, just applied to test infrastructure instead of a code property: a property whose invariant only gets checked in the easy case is a weaker property than its name suggests.

**Open Questions:**

- Is the "singleton driver" pattern (per `entrypoint`/`singleton_driver_sk-recovery-race-chaos`) invoked repeatedly over the course of one Antithesis run, or once per container lifetime? If invoked repeatedly (the standard Antithesis singleton-driver convention), a single skipped post-burst check is less severe — the next invocation gets another chance — but the *systematic bias* (skipped checks correlate with disruptive-fault runs specifically) still holds across the whole run's set of invocations, so the aggregate verification coverage is still weighted away from the highest-value windows. `(partial: confirmed the entrypoint pattern matches Antithesis's singleton-driver convention by file naming and the `exec sleep infinity` fallback under `ANTITHESIS_OUTPUT_DIR`, but did not confirm the exact re-invocation frequency/cadence Antithesis uses for singleton drivers)`
- Does the deterministic `sk-recovery-race/driver.py` (not the chaos variant) have the same gap? A quick structural read suggests it doesn't rely on chaos-timed connection loss (it pins the race with `pg_stopevent_set`), so a lost connection there would be more surprising and is caught by a plain, unhandled exception rather than a designed "return silently" branch — but this wasn't independently re-verified line-by-line for this property.
- Should the fix be "reconnect and check anyway" or "just make the skip visible"? The evidence file above states both as options without picking one — this is a design decision for whoever implements the workload change (`antithesis-workload` skill), not something this research pass should prescribe.

## SUT-side instrumentation cross-reference (existing-assertions.md)

This property is itself about the two existing assertions in `sk-recovery-race-chaos/driver.py` (`always` at 87-93, `sometimes` at 182-188) — no new SUT-side (in-process) instrumentation is obviously needed here, since the gap is entirely in workload-side control flow, not in observing OrioleDB internal state. The fix is a workload-side change (add the `sometimes(connection_lost_during_burst, ...)` marker described above), which `existing-assertions.md` doesn't currently have any analogue of — every existing assertion in that file assumes the burst completed normally.

### Investigation Log

#### Is the "singleton driver" pattern invoked repeatedly over the course of one Antithesis run, or once per container lifetime?

- Examined: entrypoint naming (`singleton_driver_sk-recovery-race-chaos`) and its `exec sleep infinity` fallback under `ANTITHESIS_OUTPUT_DIR`.
- Found: the naming and fallback pattern matches Antithesis's standard singleton-driver convention.
- Not found: the exact re-invocation frequency/cadence Antithesis uses for singleton drivers.
- Conclusion: tagged `(partial: ...)` — even under repeated invocation, the systematic bias (skipped checks correlate with disruptive-fault runs) still weakens aggregate coverage, so the core concern stands regardless of cadence.
