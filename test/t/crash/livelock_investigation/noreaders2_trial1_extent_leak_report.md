# Error report — `noreaders2` trial 1 (hunt stopped on first BUG)

Build: `f071202a` (= maintainer series through `ca00562d` + conflict resolution),
rebuilt + installed 2026-06-15 05:37. Config: no-readers streaming hunt
(`RR_READERS_*=0`, `RR_REPLICA_MODE=streaming`, `RR_KILL_POSTMASTER=1`,
`RR_KILL_POSTMASTER_INTERVAL=6`, `RR_PANIC_FATAL=0`, `RR_DURATION=60`).
Hunt stopped on first buggy trial per request.

## Verdict

**Pre-existing SK extent leak — NOT a failure of the fix series under test.**
Identical fingerprint to the documented issue in `../extent_leak_issue.md`.

## What the trial reported

```
status=BUG  writes=3322  exit=1  (81s)
invariant violations: ['orioledb structural check returned false']
[diag] tbl_check_ok = False retained_undo = False
```

The violation came solely from `orioledb_tbl_check('o_bank_account')`, which emitted:

```
05:39:30.993 [1871226] NOTICE:  Extent 3 1 is neither free or busy
05:39:30.993 [1871226] NOTICE:  Corrupted index name = o_bank_account_token_uniq
```

i.e. one 8 KB block at offset 3 in the **secondary unique index**
(`o_bank_account_token_uniq`) data file is allocated-but-unreferenced: it is in
neither the tree's busy set nor the `.map` free list. The page allocator lost
track of one block. `Extent 3 1` = start block 3, length 1 — squarely in the
documented `{2,3,4} × length-1` envelope.

## Every logical invariant passed — corruption is bookkeeping-only

| check | result |
|---|---|
| `sum(balance)` | 100000 ✓ |
| `count(*)` / `count(DISTINCT id)` / `count(DISTINCT token)` | 100 / 100 / 100 ✓ |
| `sk_set \ pk_set`, `pk_set \ sk_set` | [] / [] ✓ |
| `sk_ghost_rows`, `sk_missing_rows`, `sk_duplicate_pairs` | [] ✓ |
| universe coverage | 120/120 ✓ |
| PK btree structural check | clean ✓ (only the SK leaks) |

No row lost, duplicated, or mis-indexed. The leak is purely in the SK page
allocator's free/busy accounting (`busy ∪ free ≠ entire data file`), detected by
`check_extents()` (`src/btree/check.c`).

## Context: this is a crash-recovery artifact, not a steady-state bug

- 7 postmaster `kill -9` + crash-recovery cycles in the 60s trial (redo each
  cycle 0–60 ms; cluster healthy after each — start→ready ≈ 0.6 s).
- **No PANIC and no TRAP** on either node (`grep -c "PANIC|TRAP:"` = 0 primary,
  0 replica).
- **Replica is clean** — no extent leak, no crash. The leak is primary-only and
  surfaces at the post-trial `tbl_check`, consistent with the SK split/recovery
  interaction described in `extent_leak_issue.md`, never on the standby's replay.
- The accompanying `[plan-check FAIL]` is the known correlated planner quirk: the
  sk-forced `count(*)` fell back to `Custom Scan (o_scan) ... index only scan of
  o_bank_account_pkey` instead of the SK index. Per the issue doc this is an
  *independent* symptom of SIGKILL+recovery, not caused by (nor causing) the leak.

## Relationship to the fix series

- The fix series (`ca00562d`) touches recovery-side oxid/horizon handling on the
  **standby**; the extent leak is a **primary-side SK page-allocator** accounting
  gap after unclean shutdown. Different subsystem, different node.
- Established rates this build: extent leak ≈ 6–7% of no-readers trials
  (noreaders run #1: 6/100; this trial is an early hit of #2), **0/50** in the
  with-readers runs. Reader absence widens the window (no SK read traffic to
  interleave with the split/recovery), it does not create the bug.
- Three `check_walk_btree` fix attempts (`d2a2723e` / `4da80ba1` / `acc0c70d`,
  detailed in `extent_leak_issue.md`) have not moved its rate; the leak remains
  open and is tracked separately from the deferred-rollback livelock work.

## Bottom line

The first buggy trial of this run is the orthogonal, pre-existing SK extent leak —
expected at single-digit-percent rate in no-readers chaos and unrelated to
`f071202a`. The fix series itself produced no failure here (no livelock, no Assert
TRAP, no undo-read PANIC). Evidence preserved:
`results/20260615_053931_noreaders2_NONE_invariant{,_replica}.log`.
