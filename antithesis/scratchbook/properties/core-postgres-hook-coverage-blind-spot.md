# core-postgres-hook-coverage-blind-spot

## Focus

Wildcard (attention focus 12) — a property about the build/instrumentation pipeline itself, discovered by reading `test/antithesis/orioledb/Dockerfile` (the image build, not the SUT source) directly rather than any per-focus code area. This crosses "Version Compatibility"/"Protocol Contracts"-adjacent territory (the patched-Postgres hook surface) with pure test-infrastructure engineering, which is why no single fixed-lens focus would construct it on its own.

## What led to this

`sut-analysis.md` §1/§3 identifies the patched-Postgres hook call sites (`get_xidless_commit_lsn_hook`, `xact_redo_hook`, `CheckPoint_hook`/`after_checkpoint_cleanup_hook`, `base_init_startup_hook`, `get_relation_info_hook`, `skip_tree_height_hook`, `database_size_hook`, `snapshot_register_hook`/`snapshot_deregister_hook`/`reset_xmin_hook`, `AcceptInvalidationMessagesHookType`, `ReindexConcurrentlySkipHook`) as the highest-value additional concurrency/correctness seam — "any timing mismatch between a hook firing and OrioleDB's internal state update is structurally interesting for Antithesis."

I read `test/antithesis/orioledb/Dockerfile` (the actual image the harness runs) to check how Antithesis's coverage-guided search gets its feedback signal. The comment at the top of the file is explicit about scope:

```
# Antithesis-instrumented OrioleDB image.
# PostgreSQL is built with assertions; only orioledb.so receives coverage instrumentation.
```

Tracing the two build steps:

1. Core Postgres is built with a plain `./configure ... --enable-debug --enable-cassert ...` and `make -j ... && make install` — **no `-fsanitize-coverage` flag anywhere in this step.**
2. `orioledb.so` alone is rebuilt afterward with explicit sancov flags:
   ```
   make -j "$(nproc)" USE_PGXS=1 IS_DEV=1 \
       COPT="-fsanitize-coverage=trace-pc-guard -flto=thin -g" \
       LDFLAGS_SL="-flto=thin -fsanitize-coverage=trace-pc-guard -fuse-ld=lld -Wl,--build-id /usr/src/antithesis/antithesis_instrumentation.o"
   ```
   with a sanity check immediately after: `nm ... | grep -q 'T antithesis_load_libvoidstar'`, `readelf -S ... | grep -q 'sancov_guards'`.

So the coverage-guided fuzzing signal Antithesis's search actually uses to decide "which code paths haven't been explored yet, let's bias fault timing toward them" **only exists inside `orioledb.so`**. The patched Postgres core binary — where every hook call site `sut-analysis.md` flags as structurally interesting actually lives (the hook *definitions* are in orioledb.so, but the hook *call sites*, i.e. the exact spot where `xact_redo_hook()` gets invoked from `xact_redo()`, or `CheckPoint_hook()` from `CreateCheckPoint()`, are in core Postgres) — has zero sancov instrumentation. This is a deliberate, documented tradeoff ("we may eventually run a build with the patched postgres instrumented as well as this current build will not focus on those changes"), not an oversight, but it means the highest-value seam identified by the architecture analysis is exactly the part of the binary the search's guidance signal can't see into.

## The property

**Type:** Meta / Reachability — a property about the test infrastructure's exploration-guidance coverage, not about OrioleDB's runtime correctness directly.

**Property:** The set of code locations Antithesis's coverage-guided search can distinguish as "explored" vs. "not yet explored" includes the patched-Postgres hook call sites that mediate OrioleDB/core-Postgres state synchronization — not just the orioledb.so-internal logic downstream of them.

**Invariant:** Since core Postgres isn't sancov-instrumented in this image (and the Dockerfile comment suggests that's an intentional, possibly cost-driven scope decision, not obviously wrong on its own), the practical fix isn't "instrument all of Postgres" — it's targeted: add explicit Antithesis SDK `reachable()` markers at each orioledb-relevant hook call site in the patched Postgres core, since the SDK is already vendored and linked (the `antithesis_instrumentation.o` object is already built and linked into orioledb.so; the same technique — or a lighter-weight explicit assertion call — could be added at hook call sites even without full sancov coverage of the whole core binary). Concretely: `reachable(hook_fired, "xact_redo_hook invoked during WAL replay", {...})` at the `xact_redo_hook()` call site in `xact_redo()`, and similarly for `CheckPoint_hook`, `get_xidless_commit_lsn_hook`, etc. — turning "hidden from coverage-guided search" into "explicitly visible as a named reachability signal" for exactly the locations the architecture analysis flagged as most interesting, at much lower cost than full-binary instrumentation.

**Antithesis Angle:** Coverage-guided fault injection works by using code-coverage feedback to decide where "new" behavior is being explored and biasing future runs toward less-explored paths. Without any signal from inside core Postgres, the search has no basis to know whether a given run's fault timing actually varied *which hook fired when relative to OrioleDB's internal state transitions* — the exact class of bug the architecture analysis flagged (§1: "any timing mismatch between a hook firing and OrioleDB's internal state update"). Explicit `reachable()` markers at hook call sites wouldn't give full coverage-guided search benefits (no per-branch guidance inside core Postgres itself), but they would at least let Antithesis's exploration and triage tooling confirm "did this run actually exercise the CheckPoint_hook-during-active-recovery-workers path" (the exact scenario flagged in `sut-analysis.md` §11's `get_checkpoint_xlog_ptr()` finding) rather than inferring it indirectly from orioledb.so-side coverage alone.

**Why It Matters:** This is a low-cost, high-leverage instrumentation gap: the team already pays the cost of vendoring and linking the Antithesis C SDK for orioledb.so; extending a handful of explicit `reachable()` calls to core-Postgres hook call sites is a much smaller lift than full-binary sancov instrumentation, and it directly targets the seam the architecture analysis independently flagged as the single highest-value additional concurrency surface beyond the already-covered PK/SK race. Without it, a future investigator debugging "did Antithesis actually explore the checkpoint-during-recovery hook-timing space" has no direct evidence either way.

**Open Questions:**

- Is there a cost/complexity reason the team scoped instrumentation to orioledb.so only that isn't visible from the Dockerfile comment alone (e.g., core Postgres coverage previously tried and found too noisy/slow, or LTO complications linking sancov into a binary built by the standard `./configure`/`make` path rather than PGXS)? The comment ("we may eventually run a build with the patched postgres instrumented as well as this current build will not focus on those changes") suggests this is a known, deliberate, revisitable scope choice, not an oversight — but the specific blocker isn't stated. `(needs human input from whoever wrote this Dockerfile)`
- Would explicit `reachable()` markers at hook call sites require patching `/Users/artur/supabase/orioledb_postgres` (the patched-Postgres source) directly, which is a separate repository/build dependency from this one? Confirmed yes — the hook *call sites* are core-Postgres source (`xact.c`, `xlog.c`, `snapmgr.c`, etc.), not orioledb.so source, so this property's remedy crosses a repository boundary this analysis doesn't have write access to evaluate the cost of. This changes who would implement it (patched-Postgres maintainers, not purely the orioledb/Antithesis-harness team) — worth flagging explicitly rather than assuming it's a same-repo change.

## SUT-side instrumentation cross-reference (existing-assertions.md)

**Missing entirely** — `existing-assertions.md` confirms 0 assertions exist in the patched PostgreSQL core (`/Users/artur/supabase/orioledb_postgres`) and notes this was "checked... as expected — it's a plain Postgres source fork, not workload code." This property pushes back gently on that "as expected" framing: given the architecture analysis's own conclusion that the hook call sites are the highest-value uncovered seam, and given the SDK is already available and linked in this build, the absence of any signal there is worth treating as a gap to close rather than a settled non-goal.

### Investigation Log

#### Is there a cost/complexity reason the team scoped instrumentation to orioledb.so only that isn't visible from the Dockerfile comment alone?

- Examined: `test/antithesis/orioledb/Dockerfile` build steps and comments.
- Found: the Dockerfile comment states the scoping is deliberate and revisitable ("we may eventually run a build with the patched postgres instrumented as well"), but doesn't state the specific blocker.
- Not found: the actual reason (cost, noise, LTO/build-path complications) for scoping instrumentation to orioledb.so only.
- Conclusion: tagged `(needs human input from whoever wrote this Dockerfile)` — the specific blocker can only come from whoever made the scoping decision.
