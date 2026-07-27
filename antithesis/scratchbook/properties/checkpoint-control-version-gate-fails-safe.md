---
slug: checkpoint-control-version-gate-fails-safe
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# checkpoint-control-version-gate-fails-safe

## Focus

Protocol Contracts, binary-format angle (b): "the on-disk page/checkpoint-control
binary formats and their version constants." Directly targets the SUT-analysis
§2 lead: "`ORIOLEDB_BINARY_VERSION` mismatch is checked first; if it matches
but a finer-grained constant was bumped without bumping the binary version,
the finer check might never be consulted (untested lead)."

## What was examined

- `include/checkpoint/control.h:21-68` (`CheckpointControl` struct) and
  `ORIOLEDB_CHECKPOINT_CONTROL_VERSION 2` (`control.h:35`).
- `src/checkpoint/control.c:76-128` (`check_checkpoint_control`), called from
  `get_checkpoint_control_data` (`control.c:31-73`) on every startup read of
  the control file. The check order is, explicitly and deliberately:
  1. `control->controlFileVersion != ORIOLEDB_CHECKPOINT_CONTROL_VERSION` →
     `ereport(FATAL, "checkpoint files are incompatible with server")`.
     The code has an explicit comment explaining *why* this must be checked
     **before** the CRC: "a version bump can move `offsetof(CheckpointControl,
     crc)`, which would otherwise fail as a misleading 'Wrong CRC' rather than
     a version mismatch" (`control.c:86-89`). This is a deliberately-reasoned
     ordering choice, not an oversight.
  2. CRC check (`control.c:106-111`) → `elog(ERROR, "Wrong CRC in control
     file")` if it doesn't match.
  3. `control->binaryVersion != ORIOLEDB_BINARY_VERSION` (`control.c:113-119`)
     → `ereport(FATAL, "database files are incompatible with server", ...
     errhint("It looks like you need to initdb."))`.
  4. `control->s3Mode != orioledb_s3_mode` (`control.c:121-127`) →
     `ereport(FATAL, "database files are incompatible with server")`.
- Separately, `ORIOLEDB_SYS_TREE_VERSION`, `ORIOLEDB_PAGE_VERSION`, and
  `ORIOLEDB_COMPRESS_VERSION` are **not** gated behind the checkpoint-control
  check at all — they are independent, per-object fields checked at their own
  read call sites every time that object is read, not once at startup:
  - `ORIOLEDB_SYS_TREE_VERSION`: checked in every system-cache
    deserialization path (`o_database_cache.c:265`, `o_aggregate_cache.c:174`,
    `o_proc_cache.c:321`, `o_collation_cache.c:177`, `o_class_cache.c:168`,
    `o_indices.c:867`, `o_tables.c:2002`, and their symmetric
    serialize-side counterparts) — each `elog(ERROR, ...)`s independently on
    mismatch.
  - `ORIOLEDB_PAGE_VERSION` / `ORIOLEDB_COMPRESS_VERSION`: checked per-page at
    read time in `src/btree/io.c` (see
    `page-version-mismatch-fails-safe.md` for the details and a distinct,
    real gap found there).

## Conclusion

The SUT-analysis's worry, read literally, does not hold for the
**checkpoint-control-file** level: `ORIOLEDB_BINARY_VERSION` is not "checked
first, gating whether other checks are consulted" in a way that could skip a
later check — `controlFileVersion`, CRC, `binaryVersion`, and `s3Mode` are four
*independent* sequential gates within the same function, all of which run on
every control-file read, and the ordering among them is deliberately reasoned
(version-before-CRC) rather than accidental. Separately, the finer per-object
version constants (`SYS_TREE`/`PAGE`/`COMPRESS`) were never "gated" by the
control-file check in the first place — they live at a different layer
(per-object read paths) and are checked unconditionally every time, regardless
of whether `binaryVersion` matched. So the specific mechanism the SUT analysis
worried about (a finer check silently skipped because an earlier gate passed)
was not found. This resolves that open question for the control-file/version
layer rather than just restating it — treat this layer's version-gate
discipline as solid based on direct reading, though still worth a smoke-test
(see Antithesis angle).

## What goes wrong if this is violated

If any of these four checks were ever bypassed (e.g., a refactor that
short-circuits before reaching a later check, or an incorrect `==` inverted to
`!=`), a binary would silently interpret a checkpoint control file — and by
extension the entire on-disk B-tree/undo/page state it describes — under the
wrong structural assumptions. This is the highest-blast-radius binary-format
contract in the codebase (the control file is "the single authoritative
persistence-boundary record," per `sut-analysis.md` §2), so even though direct
reading found the gates intact, it is exactly the kind of invariant worth
pinning with a live assertion so a future regression here is caught
immediately rather than surfacing as unexplained downstream corruption.

## Antithesis angle

This is a structural/regression-guard property more than a fault-injection
target: the natural way to falsify it is a deliberate compatibility-break test
(bump `ORIOLEDB_CHECKPOINT_CONTROL_VERSION` or `ORIOLEDB_BINARY_VERSION` in a
build, start it against an old data directory, assert `FATAL` and a specific
errdetail substring) rather than something Antithesis's generic fault
injection would organically trigger (bit-flip corruption of the control file
is more likely to hit the CRC check than exactly the version field, and CRC
failure is `elog(ERROR)` not `FATAL` — worth noting this is a *severity*
inconsistency: version-mismatch is `FATAL` with an `initdb` hint, but CRC
mismatch, which is at least as serious a corruption signal, is only `ERROR`).
The CRC-vs-version severity asymmetry is a smaller, secondary finding worth
flagging even though it wasn't the primary target of this pass.

## Open Questions

- Is `elog(ERROR, "Wrong CRC in control file")` (control.c:111) sufficient to
  stop startup, or could a caller catch/retry past it in some code path
  (e.g., a PG_TRY around checkpoint control reads during recovery) in a way
  that's weaker than the `FATAL` used for version/binaryVersion/s3Mode
  mismatches? Not traced beyond `get_checkpoint_control_data`'s direct
  callers in this pass. `(needs follow-up: grep all call sites of
  get_checkpoint_control_data for PG_TRY/CATCH wrapping)`
- Whether bit-flip-style Antithesis disk corruption of the control file
  would ever land specifically on the version fields (vs. the much larger
  CRC-protected remainder, which fails via the weaker `ERROR` path) was not
  measured — this bears on which severity path is actually the one Antithesis
  would exercise organically. `(needs human input / empirical run)`

## Cross-reference (added by evaluation pass, R11)

The Wildcard evaluation lens flagged this property as near-redundant with
`page-version-mismatch-fails-safe`: both are "verified-correct-today,
dormant, no live workload" findings, discovered via the identical
investigation ("does an earlier-passing gate silently skip a later,
finer-grained version check" — the worry `sut-analysis.md` §2 originally
raised about `ORIOLEDB_BINARY_VERSION`), differing only in *which* version
constant and on-disk artifact each one gates: this property covers the
checkpoint-control-file-level gates (`controlFileVersion`, CRC,
`binaryVersion`, `s3Mode` in `check_checkpoint_control()`); the sibling
covers the page/compression-format version fields checked per-object at
read time (a different layer entirely, never gated by this file's checks).

Both are kept as distinct catalog entries because they gate genuinely
different constants and different on-disk artifacts — merging them would
lose that distinction — but a fixture or workload built for one (e.g. a
deliberate version-bump-and-restart test harness) should be checked for
reuse against the other before being built twice, since the "deliberate
compatibility-break test, not organic fault injection" testing shape is
identical for both. See also `property-relationships.md` Cluster 6, which
now cross-references both.

### Investigation Log

#### Would bit-flip-style Antithesis disk corruption of the control file ever land specifically on the version fields, vs. the much larger CRC-protected remainder?

- Examined: `check_checkpoint_control()`'s check ordering and the CRC-vs-version severity split (`control.c`), discussed under "Antithesis angle" above.
- Found: bit-flip corruption is more likely to hit the CRC-protected region than exactly the version fields; a CRC mismatch only triggers `elog(ERROR)`, not the `FATAL` used for version mismatches — a severity asymmetry.
- Not found: no empirical measurement of how often Antithesis's actual fault injection would land on the version fields vs. the CRC-protected remainder.
- Conclusion: tagged `(needs human input / empirical run)` — settling which severity path Antithesis exercises organically requires a live run, not further code reading.
