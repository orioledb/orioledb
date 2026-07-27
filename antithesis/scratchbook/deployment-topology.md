---
sut_path: /Users/artur/supabase/orioledb
commit: a975c702156cd449e9c0a8db6f8d9bf5bca4537d
updated: 2026-07-27
external_references:
  - path: doc/
    why: In-repo documentation site consulted for existing deployment/usage guidance (no dedicated production-topology doc found beyond doc/usage/*.mdx feature docs already cited in sut-analysis.md).
---

# Deployment Topology: OrioleDB

## Scope restriction (read this first)

Per the same user-mandated scope narrowing recorded in `property-catalog.md`,
this topology **excludes S3-backed decoupled storage and the rewind
feature**. The existing `config/setup/s3` overlay (minio + bucket-init
sidecar) is not part of the recommended topology below, and no
rewind-specific container or config is proposed. `/Users/artur/supabase/orioledb_postgres`
was not consulted for this document.

## Summary

The existing `test/antithesis/` harness already implements a minimal,
reasonable topology for the properties it targets: one `orioledb` primary
container, a `health-checker`, and workload-specific client containers
(`jepsen-client`, `sk-recovery-race`/`sk-recovery-race-chaos` singleton
drivers). That topology is **reused as-is** below — it doesn't need
reinventing, only one addition.

The property catalog's own file-level Open Questions identify the single
largest gap: **no primary/standby replication topology exists anywhere in the
harness**, yet a large fraction of the highest-priority properties
(`checkpoint-abort-snapshot-standby-panic`, `recovery-finish-abort-livelock`,
`replica-xmin-monotonicity`, `replica-globalxmin-catchup-lag`,
`replica-undo-reclaimed-too-early`, `checkpointer-startup-lock-drain-progress`,
`checkpoint-recovery-lsn-sync-gap`, `malformed-wal-container-double-finish`,
`readiness-gate-standby-recovery-lag`) structurally require a second Postgres
node to be reachable at all. This document's one substantive recommendation
is adding that node — the minimal topology change that unlocks the largest
share of uncovered, high-value properties.

---

## Component Groups

### Dependencies

**None required.** With S3 mode out of scope, OrioleDB has no external
service dependency — it's a single self-contained Postgres extension. This
is a meaningful simplification versus what SUT discovery initially
considered (S3/minio would have been a dependency; it's excluded per scope).

### Services

#### `test.orioledb` (primary) — existing, reuse as-is

| | |
|---|---|
| **Image source** | `test/antithesis/orioledb/Dockerfile` (existing, instrumented with Antithesis C SDK coverage on `orioledb.so`) |
| **Role** | Service — the SUT's primary node |
| **What it runs** | `postgres -D /etc/postgresql` with `shared_preload_libraries = 'orioledb'`, per-workload `postgres.conf` overlay |
| **Network** | Listens on 5432; streamed to by the new standby (below); queried by whichever client containers are active |
| **Replicas** | 1 |

#### `test.orioledb-standby` (NEW) — streaming physical standby

| | |
|---|---|
| **Image source** | Same image as the primary (`test/antithesis/orioledb/Dockerfile`) — no new Dockerfile needed, since it's the identical binary/instrumentation, just started in standby mode. Needs a **new compose service definition and startup command** (not a new image). |
| **Role** | Service — the second SUT node the replication-focused properties need |
| **What it runs** | On first start (empty `PGDATA`): `pg_basebackup -h orioledb -D $PGDATA -U <replication_role> -P -R --slot=<standby_slot_name> -C`, which base-backs-up from the primary and writes `standby.signal` + `primary_conninfo` (the `-R` flag) so the subsequent `postgres` start comes up in standby mode automatically. After that one-time bootstrap, the container's normal command is the same `docker-entrypoint.sh postgres -D /etc/postgresql` as the primary. This needs a small wrapper script or `command:` override in the new compose overlay — not a change to the shared image. |
| **Network** | Connects to `test.orioledb:5432` for streaming replication (and for the initial `pg_basebackup`); exposes its own 5432 to client containers that need to query replica state directly (`orioledb_tbl_check()`, PK/SK counts, `pg_stat_replication`-adjacent checks) |
| **Replicas** | **1.** Per the skill's "Replica Decisions" guidance, OrioleDB's replication is plain physical streaming replication, not a consensus protocol — there's no quorum to exercise, so a single standby is the minimal *meaningful* topology. A second standby would add state space (per the Simplicity Principle) without covering any additional code path any cataloged property needs; if a specific property is later found that requires comparing two standbys against each other, add one then, not preemptively. |

**Primary-side config additions needed** (small, additive to whichever
`postgres.conf` overlay is active): a replication-capable role (`CREATE ROLE
... REPLICATION LOGIN`), a `pg_hba.conf` entry permitting replication
connections from the standby container's address/network, and a replication
slot (`--slot=...` above) so the primary retains WAL the standby hasn't
consumed yet across a standby restart — directly relevant to several
cataloged properties (e.g. `sk-fixup-undo-recycling-drop`'s "widen the
checkpoint-write-to-replay gap" angle explicitly wants the standby to be able
to lag). `wal_level` defaults to `replica` in this Postgres version already
(no override needed); `max_wal_senders` defaults to 10, sufficient for one
standby.

#### `health-checker` — existing, reuse as-is

No change. Still gates on `test.orioledb`'s `service_healthy`. Whether it
should also gate on the new standby depends on the workload — see Open
Questions below.

### Clients

#### `jepsen-client`, `sk-recovery-race`, `sk-recovery-race-chaos` — existing, reuse as-is

All three continue to target the primary only. No changes needed for the
properties they already cover (`sk-fixup-sentinel-spin-livelock`,
`recovery-sk-rebuild-desync`, `sk-extent-leak-after-crash`, and the
oracle-integrity meta-properties `tbl-check-oracle-transient-false-negative`/
`jepsen-verdict-not-sdk-visible`/`chaos-driver-skips-check-on-fault-landing`
all concern single-node behavior or the workload's own verification logic).

#### `standby-consistency-client` (NEW) — the client that actually exercises the new standby

| | |
|---|---|
| **Image source** | New — a Python driver following the existing `sk-recovery-race[-chaos]` pattern (reuse `psycopg2` + `antithesis.assertions`), packaged the same way (`singleton_driver_*` under `/opt/antithesis/test/v1/{name}/`) |
| **Role** | Client — drives DML + checkpoints against the primary while asserting consistency on the standby |
| **What it runs** | Concurrent DML/rollback bursts and explicit `CHECKPOINT`s against `test.orioledb` (reusing the same `o_sk_pending`-style table shape and `orioledb_tbl_check()`/PK-count-vs-SK-count oracle the existing harness already trusts, with the oracle's own known transient-false-negative window from `tbl-check-oracle-transient-false-negative` accounted for — i.e., re-check after a settling checkpoint, not immediately), then connects to `test.orioledb-standby` to assert the same consistency there. This is the workload `checkpoint-abort-snapshot-standby-panic`, `recovery-finish-abort-livelock`, `replica-xmin-monotonicity`, `replica-globalxmin-catchup-lag`, `replica-undo-reclaimed-too-early`, `checkpointer-startup-lock-drain-progress`, `checkpoint-recovery-lsn-sync-gap`, and `malformed-wal-container-double-finish` all need to be exercised at all — none of them can be tested without a client that talks to *both* nodes. Building this driver's specifics (exact assertions, exact stopevent usage if any) is `antithesis-workload`'s job, not this document's; it's named here because the topology needs to account for its existence and its two-node connectivity. |
| **Network** | Connects to both `test.orioledb:5432` and `test.orioledb-standby:5432` |
| **Replicas** | 1 |

---

## Topology Diagram

```text
+---------------------------+      +---------------------------+
| jepsen-client             |----->|                           |
| sk-recovery-race[-chaos]  |----->|  test.orioledb (primary)  |
| (existing, unchanged)     |<-----|                           |
+---------------------------+      +-------------+-------------+
                                                  |
                                                  | streaming replication
                                                  | (pg_basebackup -R bootstrap,
                                                  |  then WAL streaming)
                                                  v
+---------------------------+      +---------------------------+
| standby-consistency-client|<---->| test.orioledb-standby (NEW)|
| (NEW)                     |      |                           |
+---------------------------+      +---------------------------+

+---------------------------+
| health-checker            | ---> depends_on: test.orioledb service_healthy
| (existing, unchanged)     |
+---------------------------+
```

## What This Topology Deliberately Excludes

- **S3/minio (`config/setup/s3`):** out of scope per user decision; not included even though it exists in the current harness as an overlay.
- **A rewind-enabled config or rewind-specific client:** out of scope per user decision; `orioledb.enable_rewind` stays off (its default) in every config.
- **A second standby / multi-standby quorum topology:** not justified — OrioleDB replication isn't a consensus protocol, so there's no quorum behavior to exercise, and every state space Antithesis has to search grows with each added container. Revisit only if a specific cataloged property is found that genuinely needs to compare two replicas against each other.
- **A synchronous-replication (`synchronous_commit`/`synchronous_standby_names`) variant** as a separate config: worth calling out as a plausible follow-up (several properties, e.g. the checkpoint-abort-snapshot race, are sensitive to how far behind a replica can be, and synchronous replication changes that window), but adding it as a *second* topology variant rather than the default keeps the base case minimal. Flagged in Open Questions below rather than included by default.

## Assumptions and Open Questions

- The standby bootstrap mechanism (`pg_basebackup -R` on first start) is a standard pattern, not verified against this specific image/entrypoint — the existing `docker-entrypoint.sh` is the stock docker-library Postgres entrypoint (confirmed by reading it) and doesn't itself special-case standby bootstrap, so the compose `command:` override needs to check for an already-initialized `PGDATA` (to avoid re-running `pg_basebackup` on container restart) before falling through to the normal entrypoint. This is an implementation detail for `antithesis-setup`, not resolved here.
- Whether `health-checker` (and thus Antithesis's `setup_complete` signal) should wait on the standby reaching streaming state, not just the primary being `pg_isready`, is an open design choice: waiting is more correct (avoids injecting faults before the topology is actually in its intended shape) but delays every run's start. Recommend gating on standby `pg_isready` too, given `readiness-gate-standby-recovery-lag`'s finding that `pg_isready` alone doesn't distinguish "caught up" from "far behind" — the health-checker should arguably poll for the standby's replication state (e.g. `pg_stat_wal_receiver` showing `streaming`) rather than just connection-acceptance, but this refinement is left to `antithesis-setup`/`antithesis-workload`.
- Which replication slot / `wal_keep_size` policy the standby's config should use is unresolved — too small risks the primary recycling WAL the standby needs (which is arguably a *feature* for exercising `sk-fixup-undo-recycling-drop`'s undo-recycling angle, but could also cause the standby to fall permanently behind and never re-sync, which would look like a stuck run rather than a useful one). Needs a concrete decision during `antithesis-workload` implementation, informed by how aggressively the chosen DML workload churns undo/WAL.
- A synchronous-replication topology variant (see "What This Topology Deliberately Excludes") is a plausible second config overlay once the async-standby topology above is working, not a day-one requirement.
- **(Added by the evaluation pass, R10.)** A second, smaller topology follow-up: `wal-decode-malformed-container-fails-safe` (and its sibling `wal-decode-rejects-future-version`) need a *live logical-replication consumer* to be exercised at all — today's WAL-format properties only check bytes reaching the decoder via crash recovery, never a real logical-decoding client. This is a separate, smaller addition from the physical standby recommended above: a `logical-subscriber-client` container running a minimal logical-replication subscriber (e.g. `pg_recvlogical` against a slot using the `test_decoding`/`wal2json` output plugin, or a small Python client using `psycopg2`'s replication protocol support) against `test.orioledb`'s existing logical slot machinery — no second full Postgres node needed, unlike the physical standby. Sequencing is not prescribed here: it can be built independently of, and in either order relative to, the physical-standby addition above, since it unblocks a different, smaller set of properties. Left to `antithesis-setup`/`antithesis-workload` to size and implement.
