# Concurrent Secondary Index Creation in OrioleDB

The procedure is conceptually similar to PostgreSQL’s concurrent index build, but introduces additional mechanisms required by:

* Undo-based MVCC
* MVCC-aware indexes
* Visibility constraints
* Absence of undo logging during validation

The algorithm performs **two passes over the table**, with a validation phase that ensures index consistency without blocking OLTP workloads.

---

## High-Level Algorithm

Concurrent index creation consists of the following stages:

1. **Initial index build using a snapshot**
2. **Collection of index contents**
3. **Validation scan of the table**
4. **Boundary-controlled insertion logic**
5. **Finalization and index activation**

---

## Stage 1 — Initial Build (Snapshot Scan)

During the first pass:

* The table is scanned using a specific snapshot.
* A secondary index is built from tuples visible in that snapshot.

This snapshot guarantees consistency of the initial index contents.

Unlike PostgreSQL, the resulting index is:

* Not immediately valid for queries
* Not fully synchronized with concurrent modifications

At this stage, the index is marked as **ready for inserts with restrictions** (see boundary rules below).

---

## Stage 2 — Collecting Index Contents

After the initial build:

* The system scans the newly created secondary index.
* For each index entry, it collects:

```
(primary_key, secondary_key, xid)
```

Where:

* **primary_key** — identifies the row
* **secondary_key** — index key
* **xid** — transaction ID that committed the tuple

This dataset represents what the index currently contains.

---

## Stage 3 — Validation Scan (Second Table Pass)

A second scan of the table is performed.

Using the collected index contents, the system determines:

### Missing in Index

Tuples present in the table but absent from the index → **must be inserted**

### Stale in Index

Entries present in the index but absent from the table → **must be deleted**

This reconciles the index with the actual table state.

---

## OrioleDB-Specific Behavior (Undo-Based MVCC)

Because OrioleDB uses **undo-based MVCC** and **MVCC-aware indexes**, additional constraints apply.

### No Undo Logging During Validation

Adding undo records to the secondary index during validation would be too complex.

Therefore:

> The validation process inserts only tuples that are **visible to all transactions**.

#### Procedure:

1. When encountering a tuple:

   * Wait until it becomes globally visible
   * Only then insert it into the secondary index

This guarantees:

* No undo logs are needed
* Only final tuple versions are indexed

---

## Validation Boundary Mechanism

### Global Constraint

Only **one concurrent index build** may run per table.

### Boundary Tracking

For each index build, a shared hash table stores:

```
validation_boundary = highest primary key validated
```

As validation progresses, the boundary increases.

### Boundary Stability Guarantee

Boundary correctness relies on page-level synchronization:

> During boundary checking, both the inserting transaction and the validation worker hold a lock on the corresponding page.
> Because of this, tuple boundaries cannot move while the procedure is in progress.

This ensures that:

* The validator observes a stable ordering of tuples
* Inserts cannot shift tuples across the boundary while it is being evaluated
* No race conditions occur between OLTP inserts and validation logic

---

## Insert Behavior During Validation

After the initial build, the index becomes **insert-ready with boundary rules**.

### Rule 1 — Inserts Before Boundary

If a transaction inserts a tuple with:

```
primary_key <= validation_boundary
```

Then:

* The inserting transaction must insert the tuple into the secondary index.

Reason:

The validator has already passed this region and will not revisit it.

---

### Rule 2 — Inserts After Boundary

If:

```
primary_key > validation_boundary
```

Then:

* The transaction does NOT insert into the secondary index.

Reason:

The validator will encounter and insert these tuples later.

---

## Tuple Movement Procedure

When the validator moves a tuple from primary storage to the secondary index:

1. Wait until old transactions that could observe the previous state finish
2. Insert the globally visible tuple into the secondary index

This ensures:

* No inconsistent visibility
* No undo records required
* Correct MVCC semantics

---

## Delete Handling

If an entry exists in the secondary index but not in the table:

* It is deleted directly from the index page.

Safe because:

* The index was built from a valid snapshot
* Each tuple appears only once
* No undo chains exist in the index

---

## Visibility and Consistency Checks

During validation, tuples are compared using:

* Commit sequence numbers (CSN)
* Transaction IDs (XID)
* Tuple identifiers

This guarantees that:

* No modifications occurred unnoticed
* Tuple contents are unchanged

---

## Finalization

After validation completes:

1. The boundary reaches the end of the table
2. All tuples are synchronized
3. Insert restrictions are lifted
4. The index is marked:

```
VALID
```

At this point:

* Queries may use the index
* Normal OLTP operations resume
* Inserts can occur anywhere
