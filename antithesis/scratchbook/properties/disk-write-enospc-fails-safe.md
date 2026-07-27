# disk-write-enospc-fails-safe

## Focus

Resource Boundaries / Infrastructure faults — filling the gap flagged by
evaluation: "no property covers disk-space exhaustion." Unlike the other
three properties in this batch, this one is primarily a **verified-correct,
completely untested contract** (the codebase gets this right, consistently,
across every call site checked) rather than a discovered bug — an honest,
positive finding, following the same framing style as the catalog's existing
`wal-recovery-rejects-future-version`/`page-version-mismatch-fails-safe`
"verified-correct contract" properties.

## What led to this

`sut-analysis.md` names disk-space exhaustion explicitly as unexercised
territory (§9: "Infrastructure-level faults (OOM-killer, disk-slow-not-dead,
CPU starvation, ...) — none exercised by the deterministic suites"). Tracing
every OrioleDB-managed disk write path (not Postgres's own WAL/heap I/O,
which is out of scope — this is specifically about OrioleDB's *own* custom
files: B-tree data files, the undo-log ring buffer's on-disk backing files,
the checkpoint control file, and checkpoint-time xid/free-extent/map files):

- **B-tree page writes**, `write_page_to_disk()` (`src/btree/io.c:1506-1632`),
  called from `perform_page_io()` (`io.c:2119`), `perform_page_io_autonomous()`
  (`io.c:2162`), and `perform_page_io_build()` (`io.c:2263`) — every one of
  these three callers checks the `bool` return value and, on failure,
  `ereport(PANIC, (errcode_for_file_access(), errmsg("could not write ...
  %m")))` (`io.c:2121-2125`, `2163-2180`, `2265-2269` and following). `%m`
  means the actual `errno` (e.g. `ENOSPC`, "No space left on device") is
  reported verbatim.
- **Undo-buffer writes**, `write_buffer_data()` (`src/utils/o_buffers.c`,
  the shared helper behind `o_buffers_write()`/`o_buffers_write_page_direct()`
  used by undo eviction): checks `OFileWrite(...)`'s return value and
  `ereport(PANIC, (errcode_for_file_access(), errmsg("could not write buffer
  to file %s: %m", ...)))` on any short write.
- **Checkpoint control file**, `write_checkpoint_control()`
  (`src/checkpoint/control.c:133-158`): checks both `OFileWrite(...) !=
  CHECKPOINT_CONTROL_FILE_SIZE` and `FileSync(...) != 0` in one combined
  condition, `ereport(FATAL, ...)` with `%m` on either failing.
- **Checkpoint xid-record files, free-extent/map files, and headers**
  (`src/checkpoint/checkpoint.c`, at least 11 distinct `OFileWrite(...)`
  call sites: lines 745, 762, 770, 864, 1970, 2154, 2218, 2902, 2949, 2967,
  3019) — every single one checks the return value (some combined with a
  `FileSync` check) and `ereport(FATAL, (errcode_for_file_access(), ...
  %m))` on failure. No exceptions found among these 11 sites.

Every one of these ~15+ call sites follows the same idiom Postgres's own core
uses for WAL-fsync/buffer-write failures: check the actual byte count / sync
return code (never assume success), and escalate to `FATAL`/`PANIC` with the
real `errno` surfaced via `%m` rather than continuing with a torn or partial
write. An `ereport(FATAL)`/`PANIC` in a shmem-attached backend or bgworker is
treated by the postmaster as an abnormal child exit, which — per
`sut-analysis.md` §11's `HandleChildCrash`-behavior note — triggers a
full-cluster crash-restart (every other backend killed, WAL replayed from the
last valid checkpoint). This is the same "hard failure now, rather than
silent corruption" doctrine Postgres itself applies to `fsync()` failures on
the WAL/heap side (the well-known "fsyncgate" lesson), and OrioleDB's own
custom-file I/O paths consistently follow it.

### The interesting residual question: torn-write detection on restart

`write_checkpoint_control()` overwrites `CONTROL_FILENAME` **in place** (same
offset 0, same file, no rename-swap or dual-copy scheme) — see
`control.c:146,151-155`. If `ENOSPC` (or a crash) interrupts this write
partway through (e.g. after N of `CHECKPOINT_CONTROL_FILE_SIZE = 8192` bytes
land), the file on disk is left as a mix of old bytes (untouched tail) and
new bytes (already-written head). `check_checkpoint_control()`
(`control.c:81-128`) checks, in order: `controlFileVersion` (raw field
compare), then a CRC over the whole struct (`COMP_CRC32C(crc, control,
offsetof(CheckpointControl, crc))` compared against `control->crc`), then
`binaryVersion`, then `s3Mode`. Because the CRC is computed over the *entire*
struct and any torn mix of old-and-new bytes is essentially guaranteed not to
match either the fully-old or fully-new CRC, a torn write is expected to be
caught by the CRC check (`elog(ERROR, "Wrong CRC in control file")`) on the
next read, not silently accepted as a valid (but wrong) checkpoint. This
mirrors — not diverges from — Postgres's own `pg_control` durability model
(also a single in-place `8192`-byte overwrite with no rename-swap, also
CRC-protected), so it is not a new risk OrioleDB introduces, but it is a
genuinely untested contract on OrioleDB's own control file specifically. Note
also the **severity asymmetry** already flagged by the catalog's
`checkpoint-control-version-gate-fails-safe` property (a version mismatch is
`FATAL` with an `initdb` hint, but the CRC mismatch — arguably an equally
serious corruption signal, and the one a torn ENOSPC-interrupted write would
actually trip — is only `ERROR`); this property is about whether the CRC
check itself reliably fires on the ENOSPC-torn-write scenario specifically,
not the pre-existing severity-asymmetry finding.

## Property

| | |
|---|---|
| **Type** | Safety (verified-correct contract via static analysis across ~15+ call sites; the CRC-torn-write angle is a corollary, not independently re-derived by a live ENOSPC repro this pass) |
| **Property** | A disk-full (`ENOSPC`) condition encountered while OrioleDB writes any of its own on-disk artifacts (B-tree pages, undo-buffer eviction writes, the checkpoint control file, or checkpoint-time xid/free-extent/map files) is always detected via the actual write/sync return value — never silently treated as success — and escalates to a `FATAL`/`PANIC` carrying the real `errno`, rather than leaving a torn or partially-written file that a later read could misinterpret as valid. If the interrupted write specifically hits the checkpoint control file, the pre-existing CRC check catches the resulting torn mix of old/new bytes on the next startup read rather than accepting it. |
| **Invariant** | `AlwaysOrUnreachable(disk_write_failure_produces_FATAL_with_ENOSPC_errno, never_silent_success)` — since organic `ENOSPC` may be rare/hard to hit without deliberate disk-space-limiting, this is best implemented as a deliberate low-disk-quota workload rather than waiting for it to occur by chance. Paired with `Always(control_file_crc_check_rejects_a_deliberately_truncated_control_file)` as a narrower, directly-constructible regression test (write a valid control file, truncate/corrupt its tail to simulate a torn write, confirm `check_checkpoint_control()` raises rather than accepting it) — this doesn't need real `ENOSPC` to falsify the CRC-catches-torn-writes half of the claim. |
| **Antithesis Angle** | Run the existing `sk-recovery-race-chaos`-style sustained DML + automatic-checkpoint workload against a data directory mounted on a deliberately small/quota-limited filesystem (a loop-mounted tmpfs/file-backed volume sized to fill up under the workload, or Antithesis's own disk-fault-injection primitives if a "disk full" fault type exists) so `write_page_to_disk()`/`write_checkpoint_control()`/the checkpoint xid-file writers are guaranteed to eventually hit `ENOSPC` under real concurrent load and adversarial timing (mid-checkpoint, mid-page-eviction, mid-undo-write) rather than only in a single deterministic spot. Confirm the instance always crash-restarts cleanly (never hangs, never silently drops the write) and that recovery afterward correctly resumes from the last valid checkpoint. |
| **Why It Matters** | Disk-space exhaustion is explicitly named in `sut-analysis.md` §9 as unexercised, high-value Antithesis territory, and this is the single most systemic, highest-blast-radius class of write in the codebase (every on-disk B-tree page and the checkpoint-control file that gates crash recovery itself). The actual code, read directly, is reassuring — every checked call site does the right thing — but "verified correct by reading 15+ call sites once" is exactly the kind of claim that silently rots: a future call site added without copying the same check-and-FATAL idiom would reintroduce the worst possible failure class (silent data loss/corruption) for this project, and there is currently no test — deterministic or Antithesis — that would catch such a regression, because none of them ever exercise real disk-full conditions. |

**Open Questions:**

- Does the Antithesis platform have a "disk full" / storage-quota fault-injection primitive that would let this be exercised as an actual infrastructure fault (rather than only via a deliberately size-capped filesystem mounted for the workload), and if so how does it interact with a containerized data directory? `(needs human input from whoever operates the harness/platform)`
- Are there any OrioleDB-managed write call sites beyond the ones enumerated here (B-tree pages, undo buffers, checkpoint control/xid/map files) that this pass didn't check — e.g. logical-decoding-related temp files, `WORKER_UNDO_TEMP_FILE` recovery-worker temp files (`recovery.c:184`), or any write path inside `src/tableam/` used for TOAST — that might not follow the same check-and-FATAL idiom? `(partial: the highest-traffic, highest-blast-radius paths were checked; the full inventory of every `File`/`write`-adjacent call site in the tree was not exhaustively enumerated)`
- Is a torn-write on the checkpoint control file ever *not* caught by the CRC check — e.g. if the torn write happens to leave the CRC field itself (whichever offset it occupies in `CheckpointControl`) coincidentally matching a valid CRC for the corrupted content, or if the OS/filesystem writes the buffer non-sequentially such that the CRC field lands correctly before some other field does? Treated as astronomically unlikely (this is the same argument that justifies trusting CRC32 anywhere) but not formally bounded this pass.

## SUT-side instrumentation

`existing-assertions.md`: 0 hits anywhere in `src/`/`include/` (**missing**).
Suggested (both **missing**):
- `Unreachable("orioledb wrote a torn/short write without raising FATAL")` — would require wrapping the common check-and-FATAL pattern in a single helper (today it's duplicated ad hoc across every call site) so a future regression that forgets the check is caught by a single always-on assertion rather than relying on each call site independently getting it right.
- `Reachable("orioledb disk write hit ENOSPC", {errno, file, operation})` at a natural chokepoint if one exists (there isn't a single shared low-level write wrapper today — `OFileWrite` is the closest, but callers check its return value themselves rather than `OFileWrite` raising anything itself) — would give direct positive confirmation that a fault-injection run actually landed a real `ENOSPC`, distinguishing "the invariant held because ENOSPC never happened" from "the invariant held under a real ENOSPC."

### Investigation Log

#### Does the Antithesis platform have a "disk full" / storage-quota fault-injection primitive, and how does it interact with a containerized data directory?

- Examined: this evidence file's own "Antithesis Angle" field, which only speculates ("...or Antithesis's own disk-fault-injection primitives if a 'disk full' fault type exists"); no Antithesis platform docs were consulted this pass.
- Found: nothing — no platform documentation or prior art was checked.
- Not found: whether such a fault-injection primitive exists at all, and if so its interaction with a containerized data directory.
- Conclusion: tagged `(needs human input from whoever operates the harness/platform)` — this is outside the codebase and requires asking the platform operator.

#### Are there any OrioleDB-managed write call sites beyond the ones enumerated (B-tree pages, undo buffers, checkpoint control/xid/map files) that don't follow the same check-and-FATAL idiom?

- Examined: `write_page_to_disk()` and its three callers (`io.c` `perform_page_io()`/`perform_page_io_autonomous()`/`perform_page_io_build()`), `write_buffer_data()` in `src/utils/o_buffers.c`, `write_checkpoint_control()` in `src/checkpoint/control.c:133-158`, and 11 `OFileWrite()` call sites in `src/checkpoint/checkpoint.c` (lines 745, 762, 770, 864, 1970, 2154, 2218, 2902, 2949, 2967, 3019).
- Found: every one of these ~15+ call sites checks the write/sync return value and escalates to `FATAL`/`PANIC` with `%m` on failure — no exceptions among them.
- Not found: full inventory of every `File`/write-adjacent call site in the tree — e.g. logical-decoding temp files, `WORKER_UNDO_TEMP_FILE` recovery-worker temp files (`recovery.c:184`), or TOAST-related write paths under `src/tableam/` were not checked.
- Conclusion: tagged `(partial: the highest-traffic, highest-blast-radius paths were checked; the full inventory of every File/write-adjacent call site in the tree was not exhaustively enumerated)`.
