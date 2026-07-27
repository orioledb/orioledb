# wal-older-version-seamless-conversion

## Focus

Version Compatibility (attention focus pass). Companion to (but distinct
from) `wal-recovery-rejects-future-version.md` and
`wal-decode-rejects-future-version.md`, which were written by a different
("Protocol Contracts") focus pass and cover the **newer-than-supported** WAL
version case. This property covers the opposite, complementary half of the
documented contract: WAL from an **older, still-supported** version
(`FIRST_ORIOLEDB_WAL_VERSION` = 16, current `ORIOLEDB_WAL_VERSION` = 17) is
claimed to be converted "seamlessly" — and finds that the current Antithesis
harness build configuration makes this specific claim structurally
untestable today.

## What was examined

- `include/orioledb.h:61-69` and `include/recovery/wal.h:22-52`: the
  documented contract is explicitly **directional and asymmetric**: "if read
  versions are lower than current, WAL will be converted seamlessly at its
  reading. But if read versions are greater than current there is a
  difference..." (recovery shuts down; logical decoding errors softly). The
  "lower version → seamless conversion" half is a distinct claim from the
  "higher version → refuse" half already covered by the sibling properties.
- `src/recovery/wal_reader.c:392-462` (`wal_container_read_header`) is the
  single shared gate for **all** WAL consumers (recovery redo, logical
  decoding, replay, and the `pg_waldump`-style `wal_desc` reader) — every
  consumer's `wal_parse_container()` call routes through it first
  (`wal_reader.c:539`, `wal_parse_container`). Its handling of the two
  directions is genuinely asymmetric in the code, exactly mirroring the doc:
  ```c
  if (wal_version > ORIOLEDB_WAL_VERSION)
  {
  #ifdef IS_DEV
      if (allow_logging)
          elog(FATAL, "... newer than supported %u. Intentionally fail tests", ...);
      return WALPARSE_BAD_VERSION;
  #else
      if (allow_logging)
          elog(WARNING, "Can't apply WAL container version %u ...", ...);
      /* Further fail and output is caller-specific */
  #endif
  }
  else if (wal_version < ORIOLEDB_WAL_VERSION)
  {
  #ifdef IS_DEV
      if (allow_logging)
          elog(FATAL, "WAL container version %u is older than current %u. Intentionally fail tests", ...);
      return WALPARSE_BAD_VERSION;
  #else
      if (allow_logging)
          elog(LOG, "WAL container version %u is older than current %u. Applying with conversion.", ...);
  #endif
  }
  ```
  (`wal_reader.c:420-447`, condensed). **Crucially, the `IS_DEV` branch is
  symmetric where the doc's claim is not**: under `IS_DEV`, *both* directions
  — including the one the docs and non-`IS_DEV` code path treat as
  legitimately convertible — hit `elog(FATAL, ..."Intentionally fail
  tests")`. This is a deliberate test-hardening choice (the comment says so
  explicitly, twice), not a bug — but it has a real consequence for what
  Antithesis can observe.
- **The Antithesis harness builds with `IS_DEV=1`.** Confirmed at
  `test/antithesis/orioledb/Dockerfile:174-177`:
  ```
  make -j "$(nproc)" USE_PGXS=1 IS_DEV=1 ...
  make USE_PGXS=1 IS_DEV=1 ...
  ```
  This matches `CLAUDE.local.md`'s own stated local dev workflow
  (`make USE_PGXS=1 IS_DEV=1`), so it is very likely the "natural" build
  configuration for this codebase generally, not an accidental harness
  choice — but it means the harness's own binary can never legitimately
  observe the "older WAL version, seamless conversion" branch: it will
  always `FATAL` first, before the per-record conversion logic in
  `wal_reader.c`'s parse routines (e.g. `wal_parse_rec_xid`,
  `wal_reader.c:68-84`: `if (r->container.version >= 17) WR_PARSE(r,
  &rec->heapXid); else rec->heapXid = InvalidTransactionId;` — genuine,
  correct-looking version-gated field parsing that exists specifically to
  support this claim) is ever reached.
- Confirmed no existing test anywhere in `test/` (`test/t`, `test/sql`,
  `test/specs`, `test/antithesis`) references `ORIOLEDB_WAL_VERSION`,
  `wal_version`, or `WAL_VERSION` at all — this conversion path has **zero**
  test coverage today, in any build mode.

## Conclusion

The "older WAL version converts seamlessly" claim is real, directional, and
has genuine supporting implementation (the per-record `>= 17` gates in
`wal_reader.c`) — this is not a hollow doc claim like the page/sys-tree
conversion stubs found by the sibling "Protocol Contracts" pass
(`page-version-mismatch-fails-safe.md`). But it can only ever be exercised
in a **non-`IS_DEV` (production-style) build**, and the current Antithesis
harness Dockerfile builds `IS_DEV=1` unconditionally. This is a genuine,
concrete environment/build-configuration gap for this specific attention
focus: as configured today, no Antithesis run can ever reach or falsify this
claim, regardless of what fault injection or workload is added, because the
build itself intercepts the scenario with an intentional `FATAL` before the
claim-under-test's code path runs.

## What goes wrong if this is violated

If the per-record version gates in `wal_reader.c` have a bug (missing a
field, wrong version threshold, wrong default value substituted for a
pre-v17 record missing a v17+ field), replaying WAL written by an older
OrioleDB version against a newer binary could silently apply wrong data
(e.g., a wrong `heapXid` linking a heap and OrioleDB transaction incorrectly)
rather than failing loudly — exactly the kind of "wrong query results /
silent corruption" failure mode that is this project's worst-case per
`sut-analysis.md` §10. Because this path is untested in every build mode
today, such a bug would not currently be caught by anything — CI, the
existing Python test suite, or Antithesis.

## Antithesis angle

To make this reachable at all requires a deliberate (not organic) harness
change: either (a) a second orioledb build variant compiled *without*
`IS_DEV` specifically for a mixed-version scenario (e.g., a standby or
recovery-replay step running a newer orioledb binary against WAL generated
by an older one), or (b) adding a supported "poison"/override mechanism to
force the older-version branch under `IS_DEV` for testing purposes (does not
currently exist). Absent either, recommend flagging this to the
`antithesis-workload` skill as a build-matrix gap rather than assuming a
workload change alone can cover it — no amount of DML/fault-injection variety
in the current single-binary-version harness will ever reach this code.
If a non-`IS_DEV` variant is built, a natural `Always` assertion: WAL written
by an older-`ORIOLEDB_WAL_VERSION` build and replayed by a newer build
produces byte-identical logical results (e.g., same row visibility/xid
linkage) as if it had been replayed by the version that wrote it — this is
the kind of invariant that needs SUT-side instrumentation (e.g. a stopevent
or debug counter recording how many records were actually converted via the
`< ORIOLEDB_WAL_VERSION` path) since "conversion happened and was correct" is
not directly observable from SQL alone. See `existing-assertions.md` — no
SUT-side assertions exist anywhere in `src/`/`include/` today; this would be
a first.

## Open Questions

- Is a non-`IS_DEV` ("production") orioledb build variant feasible to add to
  the Antithesis harness at reasonable cost, or does the harness depend on
  `IS_DEV`-only test hooks/functions elsewhere (the `sk-recovery-race*`
  drivers use `pg_stopevent_set()`, which is likely `IS_DEV`-gated) such that
  dropping `IS_DEV` would break other existing workloads sharing the same
  image? `(needs human input — depends on harness build-matrix decisions
  out of scope for this research pass)`
- Whether a genuinely mixed-version WAL stream (old-version records followed
  by new-version records from the same binary after some in-place upgrade
  event) is even a real supported scenario for this project, or whether
  `ORIOLEDB_WAL_VERSION` is only ever meant to matter for a replica/reader
  running different software than the writer (e.g., logical decoding
  consumers lagging a software upgrade) — this affects how a workload should
  be constructed to reach the claim at all. `(needs human input)`

### Investigation Log

#### Is a non-`IS_DEV` build variant feasible to add to the Antithesis harness without breaking other workloads?

- Examined: `test/antithesis/orioledb/Dockerfile:174-177` (confirms `IS_DEV=1` build); `sk-recovery-race*` drivers' use of `pg_stopevent_set()`.
- Found: the harness builds `IS_DEV=1` unconditionally; other existing workloads (`sk-recovery-race*`) depend on `pg_stopevent_set()`, which is likely `IS_DEV`-gated.
- Not found: whether a second, non-`IS_DEV` build variant is actually feasible without breaking those workloads' shared image — not traced further; this is a build-matrix/ownership decision.
- Conclusion: tagged `(needs human input — depends on harness build-matrix decisions out of scope for this research pass)`.

#### Is a genuinely mixed-version WAL stream (same binary, pre/post in-place upgrade) even a real supported scenario, or does `ORIOLEDB_WAL_VERSION` only matter cross-binary?

- Examined: `include/orioledb.h:61-69`, `include/recovery/wal.h:22-52` (the documented WAL version-compatibility contract).
- Found: the contract describes lower-versioned WAL as convertible "seamlessly," but is directional only — it does not specify whether the source scenario is cross-binary (a replica/reader running different software than the writer) or same-binary-after-upgrade.
- Not found: no doc or code was found scoping which scenario "older WAL version" is meant to cover.
- Conclusion: tagged `(needs human input)` — scenario intent isn't documented, and it determines how a workload should be constructed to reach the claim.
