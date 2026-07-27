---
slug: wal-decode-malformed-container-fails-safe
attention_focus: Protocol Contracts
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
---

# wal-decode-malformed-container-fails-safe

## Focus

Protocol Contracts. A distinct finding surfaced while investigating
`wal-decode-rejects-future-version.md`: `orioledb_decode`'s handling of
non-version parse failures is *stronger* (more severe) than the documented
"logical decoding will fail and throw error, cluster continues" contract
implies, and it's worth its own property because the severity difference
(`FATAL` vs `ERROR`) matters operationally.

## What was examined

- `src/recovery/logical.c:1371-1374`:
  ```c
  st = wal_parse_container(&r, true);
  if (st != WALPARSE_OK)
      elog(FATAL, "[WAL PARSE ERROR %d]", st);
  ```
  This fires whenever `wal_parse_container` returns `WALPARSE_BAD_TYPE`
  (unknown/unparseable record tag, `wal_reader.c:633-639`) or `WALPARSE_EOF`
  (container payload truncated mid-record, from the bounds-checked
  `WR_PARSE`/`WR_REQUIRE_SIZE` macros, `wal_reader.h:224-245`). Unlike the
  version-mismatch case (`decode_check_version`, which uses `elog(ERROR)`),
  these structural-corruption cases escalate straight to `elog(FATAL)`.
- `elog(FATAL)` terminates the current backend process (here: the logical
  decoding backend/walsender), not the whole postmaster — so this is still
  "cluster continues working" in the sense that other backends are
  unaffected, but it is a harder failure than a plain `ERROR`: the current
  session is unconditionally torn down (no `ROLLBACK`-and-continue), and if a
  client/tool is polling that connection expecting a recoverable error, it
  will instead see a connection drop.
- `src/s3/archive.c:89-101` — checked whether the third reader named in
  `sut-analysis.md` §1 ("S3 archiving") also parses this container format. It
  does not: it implements the stock Postgres `archive_module` interface
  (`ArchiveModuleCallbacks`) and archives whole WAL segment files as opaque
  bytes. **Correction to the SUT analysis**: only two readers actually parse
  the OrioleDB WAL container format end-to-end today — crash recovery
  (`orioledb_redo`) and logical decoding (`orioledb_decode`). A third,
  IS_DEV-only reader exists for display purposes: `orioledb_rm_desc` /
  `wal_desc_check_version` (`src/orioledb.c:278-379`), used by tools like
  `pg_waldump`; it fails soft by appending `" [PARSE ERROR %d]"` to the
  description string rather than erroring, which is the appropriately low-risk
  behavior for a display-only path.

## What goes wrong if this is violated / why it matters

The realistic way to reach `WALPARSE_BAD_TYPE`/`WALPARSE_EOF` in practice is
**on-disk or in-transit corruption** of WAL bytes feeding logical decoding —
exactly the kind of fault Antithesis's disk/memory fault injection can
produce. Two things are worth checking empirically rather than assuming from
the doc:

1. Does this `FATAL` genuinely stay backend-scoped, or does corruption of WAL
   bytes at exactly the point logical decoding reads them ever manifest at a
   point where Postgres's crash-handling semantics turn it into a full
   cluster restart (the same `HandleChildCrash`-style escalation flagged for
   S3 FATALs in `sut-analysis.md` §11)? A logical-decoding walsender is a
   regular backend; ordinary Postgres `FATAL` in a regular backend does not
   trigger `HandleChildCrash`-style full-cluster restart (that's reserved for
   crashes that bypass normal error handling, e.g. SIGSEGV). This distinction
   was not independently re-verified against the patched Postgres source for
   this specific hook path.
2. Whether the corrupted bytes could, in a narrower window, be interpreted as
   a *different but valid-looking* record type instead of an unknown one —
   i.e., whether `WALPARSE_BAD_TYPE`/`WALPARSE_EOF` are the only failure
   modes reachable from corrupted bytes, or whether some corruption patterns
   pass the tag-byte check by chance and get decoded as wrong-but-plausible
   data (a *worse*, silent-corruption outcome that wouldn't hit either
   `elog`). The current parser has no checksum/CRC over the container payload
   itself (unlike on-disk pages, which do) — version and record-tag checks are
   the only structural defenses.

## Antithesis angle

Combine with real WAL-page or WAL-segment bit-flip fault injection targeted at
a running logical-decoding slot (would require adding a logical replication
consumer to the harness, which does not exist today per `sut-analysis.md` §9
gap list). Assert: (a) other backends continue serving queries after the
decoding backend's `FATAL`/`ERROR` (rules out full-cluster escalation), and
(b) no logically-decoded output is produced from a corrupted record (rules out
the silent-wrong-decode risk in point 2 above).

## Open Questions

- Does `elog(FATAL)` in a logical-decoding backend ever cascade to
  `HandleChildCrash`-style full-cluster restart under the patched Postgres in
  `/Users/artur/supabase/orioledb_postgres`, or does it always stay
  backend-scoped as in stock Postgres? `(needs human input or a live repro —
  not settled by static reading of this hook alone)`
- Could bit-level corruption of a record tag byte or version tag ever produce
  a *valid-looking but wrong* record/version rather than hitting
  `WALPARSE_BAD_TYPE`/`WALPARSE_BAD_VERSION`/`WALPARSE_EOF`? No checksum
  protects the container payload itself (only the outer WAL record has the
  standard Postgres WAL CRC, which would catch this before OrioleDB's parser
  ever sees the bytes) — so in practice this is likely already ruled out by
  Postgres's own WAL record CRC, not by anything in `wal_reader.c`. Recorded
  as a `(partial: standard WAL CRC likely covers this before OrioleDB parses
  it, not independently confirmed for this repo's patched xlog reader path)`.

### Investigation Log

#### Does `elog(FATAL)` in a logical-decoding backend ever cascade to `HandleChildCrash`-style full-cluster restart?

- Examined: `src/recovery/logical.c:1371-1374` (the `elog(FATAL, ...)` call site); stock Postgres `FATAL`/`HandleChildCrash` semantics.
- Found: ordinary Postgres `FATAL` in a regular backend does not trigger `HandleChildCrash`-style full-cluster restart (reserved for crashes that bypass normal error handling, e.g. SIGSEGV); a logical-decoding walsender is a regular backend.
- Not found: this distinction was not independently re-verified against the patched PostgreSQL source for this specific hook path; no live repro was run.
- Conclusion: tagged `(needs human input or a live repro)` — the general-Postgres reasoning is a lead, not a confirmed fact for this patched build.

#### Could bit-level corruption of a record tag byte or version tag ever produce a valid-looking but wrong record/version?

- Examined: the container parsing in `wal_reader.c` for a checksum/CRC over the container payload itself; the relationship between the OrioleDB WAL container and the outer Postgres WAL record.
- Found: no checksum protects the container payload itself; the outer Postgres WAL record carries the standard WAL CRC, which would normally catch corruption before OrioleDB's parser ever sees the bytes.
- Not found: whether this outer CRC is independently confirmed to cover the OrioleDB container bytes for this repo's patched xlog reader specifically — not re-verified against the patched source.
- Conclusion: tagged `(partial: standard WAL CRC likely covers this before OrioleDB parses it, not independently confirmed for this repo's patched xlog reader path)`.
