# jepsen-verdict-not-sdk-visible

## Focus

Wildcard (attention focus 12) — directly answers the task prompt's question: "Does the jepsen workload's lack of SDK-assertion wiring... suggest a property about making its verdict actionable?" This is a property about the harness's own signal quality, cross-referenced from `existing-assertions.md`'s own stated gap.

## What led to this

`existing-assertions.md` documents, under `test/antithesis/jepsen/`:

> "No direct Antithesis SDK assertion calls found in this repo's jepsen wrapper scripts... The jepsen tool itself (external Clojure image, not vendored in this repo) performs its own serializability/anomaly analysis and writes `results.edn`/`history.edn`; `finally_jepsen-postgres` only copies those files to `$ANTITHESIS_OUTPUT_DIR` for external inspection. This means the jepsen workload's correctness verdict is not currently wired into an Antithesis SDK assertion... it produces an artifact for post-hoc review rather than a signal Antithesis's search can act on directly."

I confirmed this independently by checking the jepsen compose config (`test/antithesis/config/workload/jepsen-repeatable-read/compose.yaml`): the jepsen container is a plain client with no `antithesis` Python/SDK dependency visible in its environment, and the workload's correctness claim — Postgres "repeatable read" on OrioleDB providing snapshot isolation (`JEPSEN_ISOLATION: repeatable-read`, `JEPSEN_EXPECTED_CONSISTENCY_MODEL: snapshot-isolation`) — is exactly the kind of high-value, whole-database correctness claim (§4/§10 of `sut-analysis.md` rank "wrong query results / lost writes" as the single worst-case failure for a database engine) that would benefit most from being a first-class Antithesis signal rather than a file nobody scores during the run.

Why this matters mechanically, not just philosophically: Antithesis's fault-injection search is guided in part by which assertions exist and how they behave (coverage instrumentation plus assertion outcomes shape what the platform explores next, per how `Sometimes`/`Reachable` are described in `property-catalog.md` as "exploration hints and replay checkpoints"). A property that's never expressed as an SDK assertion is invisible to that guidance loop — Antithesis has no way to know jepsen's workload found (or didn't find) an anomaly, so it can't prioritize fault-timing variations toward whatever conditions correlate with anomalies actually appearing. The verdict sits in a file that only a human reviewing `$ANTITHESIS_OUTPUT_DIR` after the fact would ever see.

## The property

**Type:** Meta / Reachability — like `chaos-driver-skips-check-on-fault-landing` and `jepsen-verdict-not-sdk-visible`'s sibling properties in this pass, this is a property about the harness's verification pipeline, not about OrioleDB's runtime state directly. (It is closely related to, but distinct from, whatever Data Integrity-focus property asserts "no serializability anomalies" as a substantive claim about OrioleDB — this property is specifically about *whether that claim is wired to a signal Antithesis's search can use*, which is a testable, harness-level property in its own right.)

**Property:** Every jepsen run's serializability verdict (whether `results.edn`'s `:valid?` is `true`, and whether `:anomalies` is empty) is expressed as an explicit Antithesis SDK assertion outcome before the run ends, not only as a file artifact.

**Invariant:** `Always(no_anomalies_found)` — a small addition to `finally_jepsen-postgres` (or a new step run after jepsen's own analysis completes but before container teardown) that parses `results.edn`, extracts `:valid?`/`:anomalies`, and calls `always(valid and not anomalies, "jepsen detected no serializability/consistency anomalies against orioledb", {"valid": ..., "anomaly_types": [...]})`. This is squarely an `Always` (not `Sometimes` or `Reachable`) because the semantic claim — "jepsen found the target consistent" — is a safety invariant that should hold on every evaluation, exactly the kind of case `property-catalog.md`'s assertion-type guidance describes ("an acknowledged write is never lost once committed").

**Antithesis Angle:** This doesn't change what jepsen does or what faults get injected — it changes what Antithesis's own search *sees*. Today, a run that hits a genuine serializability violation looks, from Antithesis's scoring perspective, identical to a run that never got near one: no assertion fires either way, only the log/artifact differs. Wiring the verdict into `always()` means a real violation becomes a scored, triageable finding that Antithesis's own reporting surfaces directly (with the property catalog's severity/frequency framing) instead of requiring a human to remember to open `results.edn` after every run. It also means Antithesis's fault-guided search, over many runs, can start correlating "which fault-timing patterns preceded an `always()` failure here" — which is the entire point of using SDK assertions instead of post-hoc logs.

**Why It Matters:** Per `sut-analysis.md` §10, "wrong query results or lost writes (serializability anomalies) is the worst-case failure for a database engine" — and jepsen is the one workload in this harness built specifically to catch that class of failure. An unwired verdict means the harness's single highest-severity-failure-class check is also its least actionable one from Antithesis's point of view. This is a low-effort, high-leverage fix relative to most of the other findings in this pass (parsing an EDN file and calling one SDK function, vs. e.g. standing up a standby topology), which is worth calling out explicitly since it's implementable without new infrastructure.

**Open Questions:**

- What exactly does jepsen's `results.edn` schema look like for this specific jepsen-postgres variant — is `:anomalies` always present/absent in a way a simple parser can key off reliably, or does the schema vary by workload/version in ways that would make a naive parser produce false negatives (parsing failure silently treated as "no anomalies")? Not verified in this pass — the jepsen tool itself is an external Clojure image not vendored in this repo, so its exact output schema needs to be checked against a real `results.edn` sample before implementing the parser. `(needs a real jepsen run's results.edn to confirm the schema)`
- Should the assertion be `always()` per-run (one aggregate check at the end) or per-anomaly-type (separate assertions for G0/G1/G2 etc., since jepsen's Elle checker categorizes anomalies by type)? Splitting would follow `property-catalog.md`'s "every planned assertion message must be unique and specific... split into distinct properties" guidance more faithfully, but requires knowing the anomaly taxonomy jepsen actually reports for this workload — deferred to whoever implements this in the `antithesis-workload` phase.
- Is there a reason the team deliberately did *not* wire this up (e.g., jepsen's own process already fails the CI job / exits non-zero on an invalid result, making an SDK assertion seem redundant from a "does the test fail" perspective)? If jepsen's own exit code already propagates as a container failure Antithesis notices, the marginal value of this property shrinks to "better triage ergonomics" rather than "closing a blind spot" — worth confirming with whoever owns the jepsen wrapper before treating this as a hard gap. `(needs human input / needs checking finally_jepsen-postgres's actual exit-code handling, which existing-assertions.md's pass did not analyze in that much depth)`

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing entirely**, per `existing-assertions.md`'s own explicit finding (quoted above) — this property's evidence file exists specifically to convert that noted gap into a catalog property with a concrete assertion type and invariant, per the task's own hint to do so. No SUT-side (in-process OrioleDB) instrumentation is implicated here; the fix is entirely in the jepsen wrapper scripts (`test/antithesis/jepsen/finally_jepsen-postgres` or a new post-processing step).

### Investigation Log

#### What does jepsen's `results.edn` schema look like for this variant — is `:anomalies` reliably present/absent for a simple parser to key off?

- Examined: the jepsen compose config (`test/antithesis/config/workload/jepsen-repeatable-read/compose.yaml`) and `existing-assertions.md`'s note that the jepsen tool is an external Clojure image, not vendored in this repo.
- Found: the workload's expected fields are named (`:valid?`, `:anomalies`) based on how jepsen/Elle results are conventionally structured, but no actual `results.edn` sample was inspected.
- Not found: the real, concrete schema for this specific jepsen-postgres variant/version — whether `:anomalies` is always present/absent in a form a naive parser can rely on without false negatives.
- Conclusion: tagged `(needs a real jepsen run's results.edn to confirm the schema)` — the external, non-vendored tool means this can only be resolved by inspecting an actual run's output.

#### Did the team deliberately not wire jepsen's verdict into an SDK assertion because `finally_jepsen-postgres` already fails the job on jepsen's own exit code?

- Examined: `existing-assertions.md`'s original pass over `test/antithesis/jepsen/` (referenced above), which documents that the wrapper only copies `results.edn`/`history.edn` to `$ANTITHESIS_OUTPUT_DIR`.
- Found: no exit-code propagation logic was analyzed in that pass — `existing-assertions.md` doesn't state whether the wrapper's own process exit code already reflects jepsen's verdict.
- Not found: `finally_jepsen-postgres`'s actual exit-code handling — not analyzed in this pass or the prior one.
- Conclusion: tagged `(needs human input / needs checking finally_jepsen-postgres's actual exit-code handling, which existing-assertions.md's pass did not analyze in that much depth)` — needs confirmation from whoever owns the jepsen wrapper.
