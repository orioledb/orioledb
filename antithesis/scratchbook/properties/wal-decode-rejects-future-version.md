---
slug: wal-decode-rejects-future-version
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# wal-decode-rejects-future-version

## Focus

Protocol Contracts. Companion to `wal-recovery-rejects-future-version.md`,
covering the **logical-decoding** reader of the same WAL container format
instead of the crash-recovery reader. The task prompt specifically asks:
"does `orioledb_decode` reject WAL records with an incompatible version
cleanly, or does it risk misinterpreting bytes?"

## What was examined

- `src/recovery/logical.c:630-645` (`decode_check_version`) — the
  `check_version` consumer wired into logical decoding
  (`logical.c:1352-1361`). If `r->container.version > ORIOLEDB_WAL_VERSION`:
  `elog(ERROR, "Can't logically decode WAL version %u that is newer than
  supported %u", ...)`. This matches `orioledb.h:66-69`'s documented contract
  precisely: "For logical decoding: Logical decoding will fail and throw
  error. Cluster will continue working" — `elog(ERROR)` inside a backend
  performing logical decoding aborts that backend's current transaction/command
  and is recoverable at the session level; it does not bring down the
  postmaster or other backends.
- `src/recovery/logical.c:1326-1375` (`orioledb_decode`, the actual `rm_decode`
  callback registered at `src/orioledb.c:408`) — calls `wal_parse_container()`;
  its own catch-all is `if (st != WALPARSE_OK) elog(FATAL, "[WAL PARSE ERROR
  %d]", st);`. Because `decode_check_version`'s `elog(ERROR)` already
  transfers control via `longjmp` and never returns normally, this outer
  `elog(FATAL)` branch is **not** actually reached for the version-mismatch
  case — it only fires for the *other* `WalParseResult` failure modes
  (`WALPARSE_BAD_TYPE`, `WALPARSE_EOF`). See
  `wal-decode-malformed-container-fails-safe.md` for that distinct case, whose
  severity (`FATAL`, not `ERROR`) is a real, separate finding from this
  property.

## Conclusion

For the specific "WAL from a newer, unsupported version reaches logical
decoding" case, the code matches its own documentation: it throws a normal,
session-scoped `ERROR`, not a crash, and does so *before* any record payload
bytes are interpreted (the check happens immediately after container-header
parsing, before flags or records are consumed — see
`wal_reader.c:539-562`). No misinterpretation-of-bytes risk was found for this
specific path.

## What goes wrong if this is violated

If `decode_check_version` were ever bypassed or its version comparison
inverted, a decoding backend would proceed to parse record payloads whose
layout it doesn't understand (e.g., missing/extra fields introduced by a WAL
version this binary predates), which given the single-pass/no-lookahead parser
design (`wal_reader.c` header comment: "Parsing is strictly single-pass and
forward-only") could produce wrong decoded output (a corrupted logical
replication stream) rather than an early, attributable failure.

## Antithesis angle

Same reachability caveat as `wal-recovery-rejects-future-version.md`: no
existing harness config exercises a genuine WAL-version mismatch. This
property is most useful as a regression guard if/when a mixed-version
replication or logical-decoding-across-upgrade scenario is added to the
harness, or if the codebase ever adds a synthetic "poison" WAL version for
testing (none currently exists).

## Open Questions

- Same as the recovery-side property: no reachability path in the current
  harness. `(partial: mechanism verified by static analysis only)`
- Whether `elog(ERROR)` inside `decode_check_version`, called from within
  `LogicalDecodingProcessRecord()`'s call chain, could under some
  configuration (e.g., a walsender with no way to retry) still cascade into a
  slot being marked unusable indefinitely (a liveness question, not a
  correctness one) was not traced further — out of scope for this focus pass
  but worth a Focus 5 (Liveness) follow-up if logical replication is ever
  added to the harness.

### Investigation Log

#### Is there a reachability path in the current harness for a genuine WAL-version mismatch reaching logical decoding?

- Examined: `src/recovery/logical.c:630-645` (`decode_check_version`), `src/recovery/logical.c:1326-1375` (`orioledb_decode`); `test/antithesis/` harness configs for a WAL-version-mismatch or logical-decoding-consumer setup.
- Found: the version-check mechanism itself is confirmed correct by static reading (matches the documented contract; `elog(ERROR)` fires before any record payload bytes are interpreted); no existing harness config runs a mixed-WAL-version scenario or includes a logical-decoding consumer.
- Not found: no reachability path in the current harness; no synthetic "poison" WAL version mechanism exists to force this branch.
- Conclusion: tagged `(partial: mechanism verified by static analysis only)` — correctness confirmed statically, but the path is unreachable by today's harness.
