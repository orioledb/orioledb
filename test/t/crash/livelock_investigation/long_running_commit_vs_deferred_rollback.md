# Why a long-running commit is safe, but a deferred recovery-finish rollback is not

Scope: **streaming physical replication.** Two scenarios that *feel* identical — a
transaction that pins the xid horizon for a long time and then resolves — but one
is safe and the other drives the #876 replica livelock.

- **A (safe):** several txns run on the primary; the *first* one (`T1`, oxid `X1`)
  does heavy DML and stays open while all the others commit; eventually `T1`
  commits normally.
- **B (buggy):** an in-flight txn (`Tb`, oxid `Xb`) is **aborted by the primary's
  crash recovery** after a SIGKILL, and its rollback reaches the replica *late*
  via the #876 deferred `WAL_REC_XID`+`WAL_REC_ROLLBACK` pair.

The question this doc answers (raised during investigation): *if `Tb` was really
in-progress, shouldn't its WAL records pin the replica's horizon, so `writtenXmin`
could never pass it?* The empirical answer from a captured livelock
(`rootcause_evidence/TIMEOUT_6_replica.log`) is the key: **the replica has no in-flight records for
`Tb` at all — only the deferred rollback.**

---

## 0. The invariant that must hold (both nodes)

```
writtenXmin ≤ globalXmin ≤ runXmin ≤ nextXid
```

`writtenXmin` is the **frozen watermark**: oxids `< writtenXmin` have had their
`xidBuffer` circular-buffer slots **recycled and stamped `COMMITSEQNO_FROZEN`**
(oxid.c:1176-1186). Reading `xidBuffer[oxid]` for `oxid < writtenXmin` is therefore
invalid; the only thing that makes `oxid_get_csn` correct for them is the fast path
`oxid < globalXmin → FROZEN`. That is sound **iff `globalXmin ≥ writtenXmin`**
(asserted at oxid.c:1170). If `globalXmin` regresses below `writtenXmin`, oxids in
`[globalXmin, writtenXmin)` take the slow path against a recycled slot, read as
`IN_PROGRESS`, and `o_btree_..._handle_conflicts` spins forever.

**How the invariant is normally preserved:** an in-flight oxid `X` keeps the horizon
pinned at or below itself, so `writtenXmin` can never advance past a *live* oxid:

- `X` enters the leader's `xmin_queue` when its `WAL_REC_XID` is replayed
  (`recovery_switch_to_oxid`, recovery.c:1816 → `pairingheap_add`, ~:1889);
- `update_run_xmin` (recovery.c:2466) sets `runXmin = Min(queue_min, recovery_xmin)`,
  so `runXmin ≤ X`;
- `advance_global_xmin` can only raise `globalXmin` up to `runXmin` and advances
  `writtenXmin` only up to `globalXmin` — so `writtenXmin ≤ globalXmin ≤ runXmin ≤ X`.

The whole machinery assumes **every oxid handed to `recovery_switch_to_oxid` /
`xmin_queue` is ≥ the current `writtenXmin`** (a live oxid is always above the
frozen watermark). Scenario B breaks exactly that assumption.

---

## 1. Scenario A — long-running `T1` commits normally (SAFE)

`T1` does **heavy DML**, so its `local_wal` buffer overflows and its
`WAL_REC_XID(X1)` + modify records **stream to the replica while `T1` runs**.

On the replica, throughout `T1`'s life:
- `X1` is in `xmin_queue` (its XID was replayed), **and** every *other* txn's
  `WAL_REC_COMMIT` carries `finish.xmin = X1` (the primary's `runXmin` is pinned to
  `X1`), folded into `recovery_xmin` (recovery.c:3821). Either way `runXmin = X1`.
- ⇒ `globalXmin = X1`, and `writtenXmin ≤ X1` **always**. The horizon sits *on* `X1`;
  it never passes it.

When `T1` finally commits, its `WAL_REC_COMMIT(X1)` is processed **in WAL order** and
carries the *new* `finish.xmin` (the next oldest in-flight, or `nextXid`). The same
record both **removes `X1`** from the horizon and **delivers the new horizon**. So
`recovery_xmin`, `runXmin`, `globalXmin`, `writtenXmin` all step **up together,
monotonically**. No regression is possible.

**Key property:** the resolution of `T1` and the advance of the horizon past `X1` are
carried by **one in-order record**, while `X1` was continuously represented in the
horizon. `writtenXmin` and `X1` move in lockstep.

---

## 2. Scenario B — `Tb` aborted by crash recovery, deferred rollback (BUGGY)

What the captured livelock shows (`rootcause_evidence/TIMEOUT_6_replica.log`, oxid `Xb = 196`):

- The replica's **only** records mentioning oxid 196 are the deferred rollback's
  abort: `walk_undo_stack … oxid=196 abortTrx=1` and `walk_undo_range exit oxid=196
  **iters=0**` — i.e. **empty undo, no modifies**. There is **no** `WAL_REC_XID(196)`
  from its in-flight life, **no** INSERT/UPDATE/DELETE for 196, **no** `oxid_get_csn`
  on 196. The replica meets oxid 196 *for the first time* in the deferred rollback.

That single fact is the whole bug. Because `Tb`'s in-flight records **never streamed**
(its `local_wal` buffer never flushed before the SIGKILL — it was buffered on the
primary and lost), the replica **never put 196 in `xmin_queue` and never pinned its
horizon on it.** So the horizon was free to advance far past 196:

1. **Primary crash recovery aborts `Tb` and lifts its own horizon past `Xb`.**
   `recovery_finish` aborts the in-flight oxids in memory and calls
   **`free_run_xmin`** (recovery.c:1754, :2509), which sets `runXmin = nextXid` —
   *above* `Xb`. The primary's post-recovery commits then carry a **high**
   `finish.xmin`.
2. **The replica follows the primary past `Xb`.** It folds those high commit xmins:
   `recovery_xmin = Max(recovery_xmin, finish.xmin)` (recovery.c:3821) climbs well
   above `Xb`; with 196 *not* in `xmin_queue`, `runXmin = recovery_xmin`,
   `advance_global_xmin` lifts `globalXmin`, and `writtenXmin` advances to **423**
   (FROZEN-stamping `[…, 423)`, oxid.c:1176-1186). At this point `196 < writtenXmin`
   — 196 is already a frozen slot.
3. **The deferred rollback re-introduces the forgotten oxid below the watermark.**
   `o_emit_recovery_finish_rollbacks` (recovery.c:1786) →
   `wal_emit_recovery_finish_rollback` (wal.c:486) emits `WAL_REC_XID(196)` +
   `WAL_REC_ROLLBACK(196)`. Replaying `WAL_REC_XID(196)` runs
   `recovery_switch_to_oxid(196)` → **`pairingheap_add(xmin_queue, 196)`** even though
   `196 < writtenXmin = 423`. Now `queue_min = 196`, and `update_run_xmin` computes
   `runXmin = Min(196, recovery_xmin=531) = 196` and writes `globalXmin = 196`
   — **below `writtenXmin = 423`**. That is the violation the `LIVELOCK-ROOT` LOG
   caught:
   ```
   update_run_xmin lowering globalXmin to oxid=196 BELOW writtenXmin=423
     (recovery_xmin=531 queue_empty=0 nextXid=537)
   ```
4. **Livelock.** FROZEN slots `[196, 423)` are re-exposed; `oxid_get_csn` reads
   oxids 389/343/388 as `result_csn=0` (IN_PROGRESS); `handle_conflicts` enters
   6.7 M times and never returns.

---

## 3. The precise difference

| | A — long-running commit | B — deferred recovery-finish rollback |
|---|---|---|
| Did the txn's in-flight records reach the replica? | **Yes** (heavy DML overflowed `local_wal`) | **No** — only the deferred rollback (empirically: `iters=0`, no XID/modifies) |
| Was the oxid ever in the replica's `xmin_queue`/horizon during its life? | **Yes, continuously** → horizon pinned ≤ `X1` | **No** → horizon advanced freely past `Xb` |
| How is the resolution communicated? | One **in-order** `WAL_REC_COMMIT` that both removes the oxid *and* carries the new horizon | Two records emitted **late, out of order**, *after* the primary's `free_run_xmin` already lifted the horizon past the oxid |
| Net effect on `writtenXmin` vs the oxid | `writtenXmin ≤ X1` always; both step up together | `writtenXmin` reaches 423 **before** oxid 196 is re-introduced → `globalXmin` dragged to 196 `<` 423 |
| Result | monotonic, safe | **regression below `writtenXmin` → livelock** |

**So your intuition is correct:** a genuinely-streamed in-progress txn *cannot* cause
this — it pins the horizon, so `writtenXmin` can never pass it (Scenario A). The bug
requires the combination unique to B:

1. the txn's in-flight WAL **never reached the replica** (so the replica never knew
   it was in-progress and advanced `writtenXmin` past its oxid), **and**
2. its abort is delivered **later, out of order**, as a deferred `WAL_REC_XID` that
   **re-admits an already-frozen oxid into `xmin_queue`**.

The deferred rollback violates the machinery's core assumption — that any oxid
entering `xmin_queue` is `≥ writtenXmin`. In A that always holds; in B it does not.

---

## 4. Implication for a fix

The re-introduced oxid is, by construction, **already aborted and already frozen**
(`196 < writtenXmin`, and its undo walk is empty — `iters=0`). It carries **no live
state** the horizon needs to protect. The safe rule is therefore: **never let an
oxid below `writtenXmin` enter `xmin_queue` / lower the horizon** — either

- at the consumer, refuse to add a deferred-rollback oxid that is `< writtenXmin`
  (it's a tombstone; its rollback is a no-op), or
- floor `update_run_xmin` at `writtenXmin` so a stale queue entry can never drag
  `globalXmin` below the frozen watermark,

while keeping the normal in-flight path (Scenario A) untouched, where the oxid is
legitimately `≥ writtenXmin` and must continue to pin the horizon.
