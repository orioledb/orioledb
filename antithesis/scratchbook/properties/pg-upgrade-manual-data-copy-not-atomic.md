---
slug: pg-upgrade-manual-data-copy-not-atomic
attention_focus: Version Compatibility (gap-filling follow-up pass)
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
external_references:
  - path: (none — orioledb_postgres explicitly out of scope per task instructions)
---

# pg-upgrade-manual-data-copy-not-atomic

## Status: speculative, lower confidence — reasoned from documented procedure, not from an observed crash

Unlike the sibling property `pg-upgrade-cross-major-cache-reset-gap.md`, this
one is **not** backed by a reproduced failure in the branch history read for
this pass — it is a gap identified by reading the documented upgrade
procedure and checking what does/doesn't validate its precondition, in the
same spirit as `orioledb-requires-preload-clean-failure.md` (a defensively-
motivated property reconstructed from code/docs, not from a demonstrated
bug) and `o-sys-cache-invalidation-race.md` (explicitly flagged "speculative,
lower confidence"). Treat accordingly: real risk by construction, but not
independently confirmed to manifest.

Same branch-ancestry caveat as the sibling property: this concerns
`origin/pg_upgrade`/`origin/nickb/pg_upgrade_test`, neither an ancestor of
`a975c702`; the feature does not exist on `main` today.

## What led to identifying this property

Reading `doc/usage/pg-upgrade.mdx` (as it exists on `origin/pg_upgrade`,
tip `63e7fdc1`'s version, later commits only reworded it) to understand the
documented upgrade procedure, per the task's explicit prompt: "does
`pg_upgrade`'s file-copy/link mode interact safely with OrioleDB's own
checkpoint-control-file versioning if interrupted partway?"

The documented procedure (§ "Procedure" in the doc) is:

1. Stop the old cluster cleanly, run `pg_upgrade` as usual. Standard
   `pg_upgrade` restores the catalog/schema and (depending on its own
   `--copy`/`--link`/`--clone` mode) transfers the heap relation files it
   knows about — but OrioleDB tables are "restored as empty shells" at this
   step; **`pg_upgrade` itself never touches `orioledb_data/`/`orioledb_undo/`
   at all.**
2. **The operator manually runs:**
   ```bash
   rm -rf "$NEW_DATA/orioledb_data" "$NEW_DATA/orioledb_undo"
   cp -R "$OLD_DATA/orioledb_data" "$NEW_DATA/orioledb_data"
   [ -d "$OLD_DATA/orioledb_undo" ] && cp -R "$OLD_DATA/orioledb_undo" "$NEW_DATA/orioledb_undo"
   ```
3. Start the new cluster and run `orioledb_upgrade_refresh()`.

This is a critical, easily-missed distinction from stock `pg_upgrade`: for
every file `pg_upgrade` itself manages, it offers `--link` (hardlink,
near-instant, same filesystem only), `--clone` (reflink/CoW where supported),
or `--copy` (full copy) modes, and its own internal machinery is written to
either complete a relation's transfer or leave the old cluster's copy
untouched — it does not silently leave a *partially*-transferred relation
file in the new cluster on an interrupted run (a re-run/retry is the
documented recovery path). **OrioleDB's storage bypasses all of that.** The
`cp -R` of `orioledb_data/` is a plain, uninstrumented shell command run
outside `pg_upgrade`'s own process, with none of its resumability/atomicity
conventions, and — critically — **the OrioleDB checkpoint control file
(`orioledb_data/control`, confirmed via `CHECKPOINT_CONTROL_FILE_SIZE` in
`include/checkpoint/control.h` and its use in `src/checkpoint/control.c`)
lives inside the very tree being manually, non-atomically copied.**

## What goes wrong if this is violated

If the `cp -R` (or whatever copy tool an operator substitutes — `rsync`,
`tar`, a cloud-storage sync utility, etc.) is interrupted partway — host
crash, OOM-killer, disk-full, or simply a script that doesn't check the
copy command's exit status before proceeding to start the new cluster — the
new cluster's `orioledb_data/` directory would contain:
- Some files copied completely, some partially (truncated), some not at all.
- Critically, **no WAL-replayable record of this at all** — this isn't an
  OrioleDB checkpoint being interrupted (which the crash-recovery machinery
  is specifically built to handle via `replayStartPtr`/undo-stack replay);
  it's an out-of-band bulk file copy that OrioleDB's own crash-consistency
  guarantees were never designed to reason about, because from OrioleDB's
  own perspective nothing crashed — the *files themselves* are simply
  incomplete before the new cluster ever starts.

Whether the new cluster detects this at startup depends entirely on *which*
file was left incomplete:
- If `orioledb_data/control` itself was left truncated or partially
  overwritten, the existing (and, per `checkpoint-control-version-gate-fails-
  safe.md`, verified-correct) CRC gate in `check_checkpoint_control()` would
  very likely catch it (`elog(ERROR, "Wrong CRC in control file")`) — a clean,
  attributable failure.
- If a B-tree data file or undo-log segment was left truncated instead, no
  startup-time check reads the full contents of every file in
  `orioledb_data/` to confirm completeness — `orioledb_tbl_check()`/
  `verify_orioledb()` is an on-demand, SQL-callable consistency check, not
  something the server runs automatically at every startup. The truncation
  would only surface lazily, whenever a page at or beyond the truncation
  point is actually read — as a short-read I/O error, a checksum failure (if
  `orioledb_checksums_enabled`, default `true`, per `disk-leaf-header-read-
  before-validation.md`), or possibly not at all if the truncation happened
  to land exactly on an extent boundary that's simply never scanned again.
  This is a **detection gap**, not necessarily silent corruption of *returned
  data* (a checksum/short-read failure is still a loud, attributable error
  when it does trigger) — but the *timing* of that discovery (potentially
  much later, against unrelated workload traffic, long after the actual
  upgrade event) makes root-causing it back to "the upgrade's manual copy
  step was interrupted" much harder in practice than a normal crash-recovery
  failure would be.

## Antithesis angle

Not implementable today (feature doesn't exist on `main`, and this concerns
a manual, outside-the-server operator procedure rather than server code —
even once the feature merges, this would need the harness to model the
*procedure* (kill the `cp -R` process partway, or the whole upgrade-runner
container, at a randomized point during the copy) rather than fault-inject
the server itself. This is a good candidate for Antithesis's disk/process
fault injection aimed at the *upgrade tooling* process specifically, distinct
from every other property in this catalog (which target the long-running
server process): interrupt the copy step at a randomized byte offset/file
boundary, start the new cluster exactly as the (real or a hardened) procedure
specifies, and assert either (a) the new cluster refuses to start / raises a
clear, attributable error identifying the incomplete copy, or (b) if it does
start, every subsequent read of the affected relation fails loudly (checksum/
short-read error) rather than returning wrong-but-plausible data.

## SUT-side instrumentation candidates

None exist, and none are obviously addable inside `orioledb.so` itself, since
the gap is in the *procedure* (an un-instrumented shell copy), not in server
code reachable by the SDK. The most direct remedy, if this is judged worth
addressing, would be a startup-time (or `orioledb_upgrade_refresh()`-time)
completeness check — e.g., verifying every file referenced by the sys-trees'
own metadata is present and at least as large as its last-known extent —
which does not exist today in any form found in this pass.

## Open Questions

- Does `pg_upgrade`'s own `--check` mode, or any part of the standard upgrade
  procedure, offer a hook OrioleDB could use to fold its own data transfer
  into `pg_upgrade`'s own (already resumable/atomic-per-file) transfer
  machinery, rather than requiring a fully separate manual step? Not
  investigated — would require reading `pg_upgrade`'s own extension points,
  which is out of scope for this pass (and would likely require consulting
  the patched-Postgres source, itself out of scope per the task's scope
  restriction). `(needs human input / follow-up investigation once this
  branch is prioritized)`
- Is there already an operational runbook/automation layer (outside this
  repo) that wraps the documented manual steps with its own atomicity
  checks (e.g., checksumming the source and destination trees before
  starting the new cluster)? Not knowable from this repo alone. `(needs
  human input)`
- Would a truncated file actually be silently unreadable-but-not-erroring in
  any realistic scenario (e.g., a truncation that happens to leave a
  syntactically-valid-looking but stale page at the new EOF, if the copy
  tool preallocates space)? Not verified — `cp -R`'s specific behavior on
  interruption (sparse-file handling, whether partial writes are flushed)
  was not tested. `(needs further investigation / empirical test if this
  property is prioritized)`

### Investigation Log

#### Does `pg_upgrade`'s own `--check` mode or extension points offer a hook to fold OrioleDB's data transfer into its resumable/atomic-per-file machinery?

- Examined: `doc/usage/pg-upgrade.mdx`'s documented procedure only.
- Found: nothing — `pg_upgrade`'s own extension-point mechanics were not read.
- Not found: whether such a hook exists at all; answering this requires reading `pg_upgrade`'s source, which lives in the patched-Postgres tree and is out of scope for this pass.
- Conclusion: tagged `(needs human input / follow-up investigation once this branch is prioritized)` — the answer requires out-of-scope source reading.

#### Is there already an operational runbook/automation layer (outside this repo) that wraps the documented manual steps with its own atomicity checks?

- Examined: this repo's docs (`doc/usage/pg-upgrade.mdx`) and codebase for any wrapping automation.
- Found: no such runbook or automation exists inside this repository.
- Not found: whether one exists outside the repo (operator-side tooling) — not knowable from repo contents alone.
- Conclusion: tagged `(needs human input)`.
