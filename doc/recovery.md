Recovery & replication in OrioleDB
==================================

Splitting work between multiple processes
-----------------------------------------

OrioleDB implements multiprocess recovery and replication (technically, the replication in PostgreSQL is network-based recovery allowing concurrent read-only queries).  The main recovery process reads the WAL stream and distributes the messages via recovery workers via queues.

Unlike other solutions, we do not distribute work between workers transaction-wise.  Instead, each worker is responsible for his own set of keys (values of the primary key for each table).  Therefore, the large transaction will be split into chunks for each worker.  The essential advantage of this approach is the ability to scale the recovery and replication independently on the degree of transaction parallelism.

The picture below illustrates the recovery scheme involving the main recovery process and four recovery workers.  The DMLs in the example are related to some single table, in which the primary key is the `id` column of integer type.  The WAL stream contains transactions 1 comprising insert with `id = 1` and update with `id = 2`, transaction 2  comprising delete with `id = 2` and insert with `id = 3`.  The main recovery process distributes these operations to the queues based on the hash of the `id` column (`id = 1` to queue 1, `id = 2` to queue 2, `id = 3` to queue 3). 

![Distribution of messages to recovery queues](images/recovery_distribute.svg)

Note that the main process does not distribute the transaction begin message because it does not know which workers will be involved in the transaction.  Instead, it attaches the transaction id to the row modification messages.  Also, note that OrioleDB transactions are not necessarily continuous chunks in WAL.  They could be interleaved.  

Main recovery process tracks which worker participates in which transaction.  Once the transaction is confirmed and aborted in the WAL stream, the main process spreads this message to the participating workers.

Recovery workers are not synchronized on each transaction finish.  So, worker #2 can process the `delete` message before worker #2 completes the `commit` message.  It is possible because each worker has his own notion of finished transactions.

Recovery processes store the recovery transaction statuses in `recovery_xid_state_hash`.  When transaction status needs to be clarified, the recovery process first checks `recovery_xid_state_hash` and only then the shared memory.  The main recovery process updates transaction status in the shared memory only when all the worker processes have already processed the transaction finish message.  Consequently, once the transaction status is updated in shared memory, the worker process can remove its entry from the hash.

Given that transaction is effectively split between the main process and multiple workers during recovery, the corresponding undo log is also split.  The picture below illustrates this.  The transaction may have the undo chain in the main process.  That chain reflects transaction undo records that existed during checkpointing as well as undo of actions replayed by the main recovery process (such as DDL).  Simultaneously, the transaction may have one or more undo chains in the recovery workers.

![Undo chains during recovery](images/recovery_undo.svg)

Primary keys, TOAST, and secondary indexes
------------------------------------------

Recovery must bring all the table trees into a consistent state: primary key, TOAST, and secondary indexes.  Primary key and TOAST are the primary information, while secondary indexes could be derived from them.  That is why the OrioleDB WAL log only changes in primary keys and TOAST trees.

The key of OrioleDB TOAST tree contains:
 1.  Value of primary key,
 2.  Attribute number for TOASTed value,
 3.  Offset within the TOASTED value.

 Therefore the single value is represented by one or more leaf tuples in TOAST trees with different offsets (starting from zero).

 There could be multiple versions of the same tuple in the same transaction.  Correspondingly there could be multiple versions of the TOASTed values (if they got updated).  Therefore, we need to correctly match the version of a primary key tuple to that of a TOAST tuple.  We handle this by attaching the version number to the tuple, as depicted below.

![Versions for toast tuples](images/toast_version.svg)

The version number is transaction-wise.  Thus, in each new transaction, the version number starts from zero.  Zero version number is the default.  If the tuple does not contain the version number, then the version is zero.  When the primary key tuple belonging to the in-progress transaction gets updated within the same transaction, its version increases.  The TOASTed fields get updated, and TOAST tuples get the same version as the new primary key tuple.  Therefore when we need to find the TOAST tuple corresponding to the given primary key tuple, we should find the tuple with the greatest version less than equal to the primary key tuple's version.

OrioleDB needs to recover secondary indexes from the TOAST trees and primary key trees.  Secondary indexes might be built on the TOASTed, which complicates the thing. 

Therefore, OrioleDB writes checkpoints in the following order:

 1.  TOAST trees,
 2.  Primary key trees,
 3.  Secondary index trees.

Also, we are writing to the WAL TOAST tuples first and then primary key tuples.

When the checkpointing of the primary key trees is finished, we mark the current WAL position of the "toast consistency point".  See the picture below.

![Recovery of secondary indexes](images/recovery_secondary_indexes.svg)

We only apply WAL records to TOAST and primary key trees during recovery before the toast consistency point.  We cannot "lose" any secondary index changes in that period because secondary index trees were checkpointer later.  Thus, secondary indexes already contain all the changes made before the toast consistency point.

After the toast consistency point, we start to apply changes to the secondary indexes.  Since TOAST WAL records are going first, we can fetch all the TOASTed values we need (if any) and apply the changes to the secondary indexes while applying the primary key WAL record.
