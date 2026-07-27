---
slug: page-version-mismatch-fails-safe
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# page-version-mismatch-fails-safe

## Focus

Protocol Contracts, binary-format angle (b). Follows directly from
`checkpoint-control-version-gate-fails-safe.md`'s finding that
`ORIOLEDB_PAGE_VERSION`/`ORIOLEDB_COMPRESS_VERSION` are checked independently
per-page, not gated by the control-file check. This property is about what
happens when that per-page check actually fires, and about a documentation/
implementation gap found along the way.

## What was examined

- `include/orioledb.h:88-98`: the version-scheme comment claims, for
  `SYS_TREE`/`PAGE`/`COMPRESS` versions: "if read version is lower than
  current — seamless conversion will occur at the first reading." This is a
  forward-compatibility promise: an older on-disk page format should be
  transparently upgraded when read by a newer binary.
- `src/btree/io.c:1246-1267` (`check_orioledb_page_version`,
  `convert_orioledb_page_version`):
  ```c
  /*
   * Now we have only one page version (1). When we have
   * different versions we'll need to bump ORIOLEDB_PAGE_VERSION
   * and implement on-the-fly conversion function from all
   * previous page versions to use _after_ decompression.
   */
  static bool
  check_orioledb_page_version(OrioleDBOndiskPageHeader ondisk_page_header)
  {
      if (ondisk_page_header.page_version != ORIOLEDB_PAGE_VERSION)
          elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", ...);
      return false;
  }

  static void
  convert_orioledb_page_version(Pointer img)
  {
      Assert(ORIOLEDB_PAGE_VERSION == 1);
      elog(FATAL, "Page version conversion is not implemented");
  }
  ```
  `check_orioledb_compress_version` (`io.c:1339-1346`) is the same pattern for
  `ORIOLEDB_COMPRESS_VERSION`.
- Both checks currently `elog(FATAL)` on **any** mismatch, in **either**
  direction (both "older, should convert" and "newer, should reject" collapse
  to the same unconditional FATAL), because `ORIOLEDB_PAGE_VERSION` and
  `ORIOLEDB_COMPRESS_VERSION` have been `1` since introduction — no second
  version has ever existed, so the "seamless conversion for lower versions"
  branch documented in `orioledb.h` has **never been implemented or exercised**
  — `convert_orioledb_page_version` is a stub that itself `elog(FATAL)`s with
  "Page version conversion is not implemented," guarded only by
  `Assert(ORIOLEDB_PAGE_VERSION == 1)`.

## Conclusion

This is a genuine (if currently dormant) gap between documented and actual
behavior: the doc promises seamless backward-compatible conversion for these
three format families; the code has no conversion logic at all yet, just a
placeholder that fails loudly. The good news for safety: the current behavior
*fails safe* — a version mismatch always halts with `FATAL` rather than
silently reading page bytes under the wrong layout assumptions (no
misinterpretation-of-bytes risk today). The gap is a **forward-looking process
risk**: the day someone bumps `ORIOLEDB_PAGE_VERSION` to 2, this stub must
actually be filled in with a real conversion function *and* the FATAL-on-any-
mismatch logic must be split into "convert if lower, reject if higher" — and
nothing in the current codebase tests that split because it's never been
exercised. `sys_tree` conversion (the catalog-cache side, e.g.
`o_database_cache.c:265-268`) has the same "not among supported for conversion
from/to" wording split into two distinct messages (from/to), suggesting the
sys-tree side may be closer to having real directional logic than the page
side — this asymmetry between the three format families was noted but not
fully chased down within this pass.

## What goes wrong if this is violated

Two distinct risks, at two different times:
1. **Today**: none directly — the fail-safe FATAL prevents misreading, at the
   cost of the documented "seamless" promise not being honored (an
   operational/upgrade-experience gap, not a correctness bug).
2. **Future**: when `ORIOLEDB_PAGE_VERSION` is next bumped, if the real
   conversion function introduced to replace the stub has a bug (e.g., an
   off-by-one in a field added between page-version 1 and 2, or a conversion
   applied in the wrong direction), that would be a genuine "misinterpreting
   bytes" bug of exactly the kind this focus is looking for — and there is
   currently no test scaffolding (no synthetic old-format page fixture, no
   test that exercises the conversion path) to catch it, because the path has
   never had two versions to convert between.

## Antithesis angle

Not organically reachable today (single page version in existence). This is
better framed as a **process/coverage note** for whenever a page-format change
next ships: at that point, add a fixture with an old-version page image and
assert the conversion round-trips correctly under concurrent read load, ideally
under Antithesis fault injection (crash mid-conversion, etc.) rather than only
in a deterministic unit test. Recording this now so the gap isn't rediscovered
from scratch at that time.

## Open Questions

- ~~Does the `sys_tree` conversion path (catalog caches) have real from/to
  conversion logic implemented anywhere, or is it the same
  fail-on-any-mismatch stub as pages/compression?~~ Resolved below
  (investigated as part of the Version Compatibility focus pass).

### Investigation Log

#### Does the sys_tree conversion path have real from/to conversion logic implemented anywhere?

- Examined: `git log -S"controlFileVersion != ORIOLEDB_CHECKPOINT_CONTROL_VERSION"`-style
  history search on the version constants; specifically
  `git show 4e0b28ea` ("Bump ORIOLEDB_BINARY_VERSION").
- Found: real, directional from/to conversion logic **did** exist for the
  sys-tree-level constant (then called `ORIOLEDB_DATA_VERSION`, later
  renamed `ORIOLEDB_SYS_TREE_VERSION`) prior to this commit — e.g.
  `src/catalog/o_indices.c` had `if (oIndex->data_version >= 2) { ... } else
  oIndex->tablespace = DEFAULTTABLESPACE_OID;` and a second `>= 3` gate for
  `exclops`/`immediate`, with equivalent logic in
  `src/catalog/o_tables.c:deserialize_o_table_index`. So the message-wording
  hint ("from %u"/"to %u") was correct: directional conversion logic is a
  real, previously-implemented pattern in this codebase, not just
  aspirational wording — it's just not currently *active* for pages/sys-tree
  because both were last reset to their v1 baseline.
- Also found: commit `4e0b28ea` **removed all of that conversion code** in
  the same change that bumped `ORIOLEDB_BINARY_VERSION` (7→8) and reset
  `ORIOLEDB_DATA_VERSION` 3→1 ("Reset ORIOLEDB_DATA_VERSION that makes sense
  only within one value of ORIOLEDB_BINARY_VERSION" / "Remove
  data_version-dependent code after reset ORIOLEDB_DATA_VERSION" per the
  commit message). This is a deliberate, documented pattern, not an
  oversight: since `orioledb.h`'s master comment states the finer version
  constants "make sense only within one `ORIOLEDB_BINARY_VERSION` value,"
  every time `ORIOLEDB_BINARY_VERSION` bumps, any accumulated sys-tree/page
  conversion code becomes provably dead (old data of any prior sys-tree
  version is already unreadable due to the coarser binary-version FATAL) and
  is cleaned up rather than left as dead code.
- Conclusion: the pattern is real and has historical precedent of being
  implemented correctly (multiple `>=` gates handled at once, across two
  files, for the 1→2→3 progression). The forward-looking risk this property
  already identifies stands, refined: the risk isn't "will anyone implement
  conversion at all" (they have, before) but "will *every* one of the ~7
  cache-type call sites (`o_tables.c`, `o_indices.c`, `o_proc_cache.c`,
  `o_aggregate_cache.c`, `o_collation_cache.c`, `o_class_cache.c`,
  `o_database_cache.c`) that independently check `data_version` get the new
  gate added consistently, given each is a separate hand-written
  serialize/deserialize pair" — a manual-consistency risk across a
  multi-site change, not a "will it be skipped entirely" risk. Not
  independently testable via Antithesis today (requires an actual future
  version bump to construct the scenario), so this refines rather than
  changes the property's Antithesis-angle conclusion (still a process/
  coverage note, not a reachable fault-injection target today).
- Is there any existing unit/regression test that fabricates a
  wrong-page-version image on disk to verify the FATAL path itself (as
  opposed to the unimplemented conversion path)? Not found in `test/sql`,
  `test/t`, or `test/specs` during this pass — a quick follow-up grep for
  `page_version` in `test/` would settle it. `(needs follow-up grep)`

## Cross-reference (added by evaluation pass, R11)

The Wildcard evaluation lens flagged this property as near-redundant with
`checkpoint-control-version-gate-fails-safe`: both are "verified-correct-
today, dormant, no live workload" findings, discovered via the identical
investigation into whether an earlier-passing gate could silently prevent a
later, finer-grained version check from being consulted (`sut-analysis.md`
§2's original worry about `ORIOLEDB_BINARY_VERSION`). They differ only in
which layer/constant is gated: this property is about `ORIOLEDB_PAGE_
VERSION`/compression-format version fields, checked per-object at read time
(`convert_orioledb_page_version`'s currently-stub conversion path); the
sibling is about the checkpoint-control-file's own sequential gates
(`controlFileVersion`/CRC/`binaryVersion`/`s3Mode`) — a structurally
different artifact and code path, not merely a different field in the same
file.

Kept as two distinct properties (they gate genuinely different constants),
but a fixture/workload built for one (a deliberate version-bump-and-restart
test) should be checked for reuse against the other, since both share the
same "not organically fault-injectable, needs a deliberate compatibility-
break test" testing shape. See also `property-relationships.md` Cluster 6.
