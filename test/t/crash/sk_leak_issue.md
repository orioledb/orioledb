# Recovery-side SK index desynchronization in chaos-injected crash-recovery tests

## Summary

A fault-injected crash-recovery stress test produces a state in which the primary-key tree of an orioledb table is pristine, but a secondary index over the same table disagrees with it. The failure occurs after one the backend crushed and PG postmaster shut down all the rest backends. After the recovery the PK side is correct in every observed failure; only the SK is wrong. The bug does not reproduce in every test trial. Therefore, it seems that there are specific conditions that the test must meet in order to reproduce the bug.

Current fault-injection occurs in the oriole COMMIT flow immediately after the WAL flush and before the CSN flip. However, some experiments have shown that such fault injection can occur even if the beginning of the orioles commit flow is not the COMMIT.

This scenario is relevant to bank account stress testing, and the rest of the issue is discussed in terms of that test.

## Test mechanic

The workload models a bank with `n_accounts = 100` accounts in one table `o_bank_account(id, balance, token)`. The table has a PK on `id` and a `UNIQUE` constraint on `token`. Initial state: row `i` has `balance = 1000` and `token = i`, for `i ∈ [1, 100]`.

Two invariants must hold at all times and across crash recovery:

1. **Balance conservation:** `sum(balance) == n_accounts * 1000` (PK-side property — money is neither created nor destroyed).
2. **Token uniqueness:** `count(DISTINCT token) == n_accounts` (SK-side property — the `UNIQUE(token)` constraint must remain satisfied).

Each writer thread owns a personal token (`my_token = n_accounts + writer_id`, so initially in `[101, 120]` for 20 writers). In one transaction it picks two random rows `v_from`, `v_to`, then runs:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT balance, token FROM o_bank_account WHERE id = $v_from;
SELECT balance, token FROM o_bank_account WHERE id = $v_to;
UPDATE o_bank_account
   SET balance = balance - $amount, token = $my_token
 WHERE id = $v_from;
UPDATE o_bank_account
   SET balance = balance + $amount, token = $from_token
 WHERE id = $v_to;
COMMIT;
```

After the commit the writer adopts `to_token` as its new `my_token`, so tokens circulate among the 100 rows plus the writers' "pockets". At any instant exactly `n_accounts` distinct token values are present on rows (each writer's pocketed token is *not* on a row). Both invariants are preserved by every successful commit.

All invariants are checked at the end of the test

## Symptom in bank account tests

After a chaos-injected `PANIC` and the subsequent crash recovery, the harness validates two invariants over the bank-account workload table: the sum-of-balances invariant (PK-only) and the token-uniqueness invariant (`count(DISTINCT token) == n_accounts`, which is answered by an Index-Only Scan on the SK alone, without touching the PK). The PK-only invariant always passes: the PK holds exactly 100 rows, 100 distinct IDs, 100 distinct tokens, and the expected total balance. The SK-only invariant fails with a small delta in either direction, +1 to +4 ghost SK entries or -1 to -2 lost SK entries.

Two representative fingerprints from the current hunt:

```
Trial 13: unique_tokens 99 != 100 [SK missing (in PK, not SK): [15]]
Trial 18: unique_tokens 102 != 100 [SK extra (in SK, not PK): [79, 95]]
```

In the first, the PK row whose token is 15 has no matching SK entry. In the second, the SK retains stale entries for tokens 79 and 95 that no PK row currently holds.

## Steps to reproduce

```bash
# 1. Branch under test: add_stress_bank_account_test

# 2. Run the stress test repeatedly (one trial = ~17-22 s):
RR_WRITERS=20 RR_ACCOUNTS=100 RR_DURATION=15 RR_ROLLBACKERS=0 \
  RR_INJECTION_POINTS=NONE \
  RR_ASSERT_POINTS=orioledb-commit-assert RR_ASSERT_FIRINGS=3 \
  RR_PANIC_FATAL=0 RR_SAVE_ALL_LOGS=0 RR_INSTANCE=issue \
  python3 -m unittest test.t.crash.rr_stress_test.RrStressTest.test_bank_account_invariant
```

The harness (`test/t/crash/rr_stress_test.py`) drives a multi-writer workload that runs concurrent `BEGIN; UPDATE; UPDATE; COMMIT;` transactions, swapping balances between accounts and rotating their `token` column. A chaos thread (`assert_chaos_loop`) attaches the `orioledb-commit-assert` injection point with the `'error'` action. The injection sits inside a `START_CRIT_SECTION`, so the attached error escalates to `PANIC`. The cluster runs with `restart_after_crash = on`; once recovery completes, the harness validates the invariants.

A bug-reproducing trial fails with an `AssertionError` ending in:

```
invariant violations: ['unique_tokens N != 100 [SK extra/missing ...: [...] ]']
```

#### Reproduction rate

Per-trial rate is approximately 10-30%. Recent 50-trial hunts observed 6/50 (12%); earlier hunts, before timing instrumentation was added, reached roughly 30%. About 25-30 trials are enough to hit the bug at least once with 95% confidence.
