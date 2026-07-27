---
slug: wal-recovery-rejects-future-version
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# wal-recovery-rejects-future-version

## Focus

Protocol Contracts (attention focus pass). Targets the SUT-analysis §2/§11 lead:
"`ORIOLEDB_BINARY_VERSION` is checked first; if it matches but a finer-grained
constant was bumped without bumping the binary version, the finer check might
never be consulted (untested lead)" and the explicit task prompt "does the WAL
format version check actually gate every incompatible format change end-to-end."

This property is scoped specifically to the **crash-recovery redo path** (one of
the two readers that actually parse the OrioleDB WAL container format — see the
correction in the Open Questions / cross-cutting note below about the "three
readers" framing in `sut-analysis.md` §1).

## What was examined

- `include/orioledb.h:47-104` — the master version-scheme comment block.
  `ORIOLEDB_WAL_VERSION` (17) is explicitly documented as compatible
  independent of `ORIOLEDB_BINARY_VERSION`: "WAL can be transferred between
  different clusters ... compatibility is not limited to the same
  ORIOLEDB_BINARY_VERSION." So for WAL specifically, the binary-version gate
  does **not** pre-empt the WAL-version gate — they're independent checks (this
  narrows the SUT-analysis's general worry to the finer on-disk-format
  constants, handled in `checkpoint-control-version-gate-fails-safe.md` and
  `page-version-mismatch-fails-safe.md`).
- `include/recovery/wal.h:25-52` — `ORIOLEDB_WAL_VERSION 17`,
  `FIRST_ORIOLEDB_WAL_VERSION 16`, `ORIOLEDB_CONTAINER_FLAGS_WAL_VERSION 17`.
- `src/recovery/wal_reader.c:392-462` (`wal_container_read_header`) — reads the
  version tag first. If `wal_version > ORIOLEDB_WAL_VERSION`:
  - `IS_DEV` build: `elog(FATAL, ...)` immediately (test builds always fail
    hard on any version skew) and returns `WALPARSE_BAD_VERSION`.
  - non-`IS_DEV` (production) build: only logs a `WARNING` and **falls
    through** — the comment explicitly says "Further fail and output is
    caller-specific." This looked, on first read, like production builds might
    silently continue parsing a container in a format newer than they
    understand.
- `src/recovery/wal_reader.c:522-582` (`wal_parse_container`) — after the
  header check, it unconditionally calls the consumer's `check_version()`
  callback (if set) *before* consuming container flags or scanning any record.
  This is the "caller-specific" enforcement the header comment refers to.
- `src/recovery/recovery.c:4049-4063` (`replay_check_version`, the consumer
  wired into the actual redo path via `replay_container` at
  `recovery.c:4542-4573`) — explicitly re-checks
  `r->container.version > ORIOLEDB_WAL_VERSION` and returns
  `WALPARSE_BAD_VERSION` in that case (this executes even in production
  builds, closing the gap left open by the header-level warn-only branch).
- `src/recovery/recovery.c:1157-1258` (`orioledb_redo`, the actual
  `rm_redo` callback registered at `src/orioledb.c:404`) — calls
  `replay_container()`; if it returns `false` (which it does whenever
  `wal_parse_container` returns anything other than `WALPARSE_OK`, including
  `WALPARSE_BAD_VERSION`), `orioledb_redo` calls `abort_recovery(workers_pool,
  false)` and then `elog(ERROR, "orioledb recovery worker failed to replay WAL
  container.")`. An `elog(ERROR)` raised inside a redo callback during crash
  recovery is fatal to the startup process in standard Postgres semantics —
  recovery aborts and the cluster refuses to come up. This matches the
  documented contract in `orioledb.h:66-69`: "For recovery: Cluster will shut
  down (recovery failed)."
- `src/recovery/wal_reader.c` record-level parsers (e.g.
  `wal_parse_rec_xid`, `wal_reader.c:67-84`) additionally gate
  version-introduced fields per-field via `r->container.version >= 17` checks,
  and all reads go through the bounds-checked `WR_PARSE`/`WR_REQUIRE_SIZE`
  macros (`include/recovery/wal_reader.h:224-245`), which return
  `WALPARSE_EOF` rather than over-reading the buffer. Unknown record tags hit
  the `default:` case in the scan loop (`wal_reader.c:633-639`) and return
  `WALPARSE_BAD_TYPE` immediately, without attempting to keep scanning under a
  guessed layout.

## Conclusion

Chasing the "production build logs a WARNING and falls through" branch all the
way to its actual caller confirms the version gate **is** enforced end-to-end
for the recovery-redo path specifically: the outer `check_version` re-check in
`replay_check_version`, plus `orioledb_redo`'s handling of a `false` return,
together ensure a WAL container from a version newer than the running binary
understands is never handed to a record parser — recovery aborts instead. This
is a case where reading past the first "looks like a gap" comment to the actual
call chain overturned the initial suspicion; it should not be taken as
generalizing to the other version constants (see the sibling properties for
those).

## What goes wrong if this is violated

If a future WAL-format change added new mandatory fields without properly
gating them by `r->container.version`, or if the `replay_check_version` /
`orioledb_redo` chain were refactored and the newer-version rejection dropped
silently, replay could proceed to interpret bytes belonging to fields the
current binary doesn't know about as if they were the *next* record's tag byte
— silent stream desynchronization corrupting arbitrary subsequent WAL replay,
not just the one incompatible record. This is the "misinterpreting bytes" risk
the task prompt specifically asked to check for; it is not what the code
currently does, but it is the failure mode a regression in this area would
produce.

## Antithesis angle

This is hard to reach with *organic* fault injection in a single-binary-version
fleet (the harness never runs two different OrioleDB binary versions
concurrently today). Reaching it would require either (a) a deliberately
constructed two-version-in-one-run harness config (old binary writes WAL,
newer/older binary — or a synthetically bumped `ORIOLEDB_WAL_VERSION` build —
replays it), or (b) direct fault injection that flips bits in the version tag
byte of an in-flight WAL record before replay (a targeted, not organic,
corruption). Absent either, this stays a static-analysis-verified contract, not
something the existing harness will stumble into. Recorded as an open question
below rather than assumed reachable.

## Open Questions

- No config in `test/antithesis/` runs two different OrioleDB binary versions
  against the same data directory/WAL stream. Without that, this property can
  only be asserted as `AlwaysOrUnreachable` in practice, and is likely to sit
  at "never reached" for a long time unless a version-skew scenario is
  deliberately built. `(partial: mechanism verified by static analysis; no
  reachability path identified in the current harness)`

### Investigation Log

#### Is there a reachability path in the current harness for a genuine cross-version WAL-replay scenario?

- Examined: `test/antithesis/` harness configs for multi-binary-version scenarios; the full call chain `wal_container_read_header` → `wal_parse_container` → `replay_check_version` → `orioledb_redo` (`src/recovery/wal_reader.c:392-462,522-582`; `src/recovery/recovery.c:4049-4063,1157-1258`).
- Found: the version-gate mechanism is confirmed correct end-to-end by static analysis (recovery aborts cleanly on newer-version WAL, matching the documented contract); no harness config runs two different OrioleDB binary versions against the same data directory/WAL stream.
- Not found: no reachability path in the current single-binary-version harness; reaching it would need either a deliberate two-version harness config or targeted bit-flip fault injection on the version tag byte.
- Conclusion: tagged `(partial: mechanism verified by static analysis; no reachability path identified in the current harness)`.
