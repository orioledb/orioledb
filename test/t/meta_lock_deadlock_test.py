#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest, ThreadQueryExecutor, wait_stopevent


class MetaLockDeadlockTest(BaseTest):
	"""
	Deterministic reproduction of the standby replay self-deadlock:

	The recovery leader holds oTablesMetaLock SHARED from the replayed
	WAL_REC_O_TABLES_META_LOCK until the matching WAL_REC_O_TABLES_META_UNLOCK.
	If an unrelated dbase_redo (DROP DATABASE, which emits
	PROCSIGNAL_BARRIER_SMGRRELEASE) is interleaved into that WAL window, the
	leader replays it while holding the lock; the held LWLock keeps
	InterruptHoldoffCount > 0, so ProcessProcSignalBarrier() never runs and the
	leader waits for its own slot to accept the barrier -> replay freezes.

	We force the interleaving deterministically on the primary: park a CREATE
	TABLE backend at the `before_o_tables_meta_unlock` stop event (its
	META_LOCK + O_TABLES-modify container is already flushed by
	systrees_modify_end), issue a concurrent DROP DATABASE so its dbase_redo
	lands between META_LOCK and META_UNLOCK, then release the DDL.  A standby
	must replay past all of it without hanging.
	"""

	def test_meta_lock_dbase_redo_no_deadlock(self):
		master = self.node
		master.append_conf("orioledb.enable_stopevents = on\n"
		                   "wal_level = replica\n"
		                   "max_wal_senders = 4\n"
		                   "max_replication_slots = 4\n")
		master.start()

		replica = self.getReplica()
		replica.start()

		master.safe_psql("CREATE EXTENSION orioledb;")
		# A throwaway database whose DROP produces the dbase_redo barrier.
		master.safe_psql("CREATE DATABASE victim;")
		self.catchup_orioledb(replica)

		# Park ONLY the CREATE TABLE backend (filter by application_name, which
		# is a default stop-event param) so DROP DATABASE is free to run.
		con_ddl = master.connect()
		con_ddl.execute("SET application_name = 'ddlparker';")
		ddl_pid = con_ddl.execute("SELECT pg_backend_pid();")[0][0]
		# applicationName is a stop-event *variable* (make_process_params),
		# so reference it as $applicationName, not $.applicationName.
		master.safe_psql(
		    "SELECT pg_stopevent_set('before_o_tables_meta_unlock', "
		    "'$applicationName == \"ddlparker\"');")

		t_ddl = ThreadQueryExecutor(
		    con_ddl,
		    "CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;")
		t_ddl.start()

		# Wait until the DDL is parked inside the meta-lock window.  By now its
		# WAL_REC_O_TABLES_META_LOCK container has been flushed to the stream.
		wait_stopevent(master, ddl_pid)

		# Concurrent DROP DATABASE in its own thread (safe_psql => autocommit,
		# since DROP DATABASE cannot run in a transaction block).  It emits the
		# dbase drop WAL between the META_LOCK container and the still-unwritten
		# META_UNLOCK.  It may block on its foreground SMGRRELEASE barrier
		# (waiting for the parked DDL) -- that's fine, its WAL record is what
		# we need in the stream; releasing the DDL below unblocks it.
		import threading, time
		drop_err = []

		def _do_drop():
			try:
				master.safe_psql("DROP DATABASE victim;")
			except Exception as e:  # noqa: BLE001
				drop_err.append(e)

		t_drop = threading.Thread(target=_do_drop)
		t_drop.start()

		time.sleep(3)  # let DROP DATABASE reach/emit its dbase drop record

		# Release the DDL: WAL_REC_O_TABLES_META_UNLOCK is written; the DDL
		# then absorbs the barrier so DROP DATABASE completes too.
		master.safe_psql(
		    "SELECT pg_stopevent_reset('before_o_tables_meta_unlock');")
		t_ddl.join()
		t_drop.join()
		con_ddl.commit()
		con_ddl.close()
		if drop_err:
			raise drop_err[0]

		# The stream now contains
		#   [META_LOCK + O_TABLES modify] ... [dbase_redo] ... [META_UNLOCK]
		# The standby must replay all of it.  With the bug the recovery leader
		# self-deadlocks in dbase_redo while holding oTablesMetaLock and replay
		# freezes; with the fix it catches up.  Bounded poll so the failure is
		# fast (rather than hanging the whole CI job).
		target = master.execute("SELECT pg_current_wal_lsn();")[0][0]
		deadline = time.time() + 60
		caught = False
		while time.time() < deadline:
			if replica.execute(
			    "SELECT pg_last_wal_replay_lsn() >= '%s'::pg_lsn;" %
			    target)[0][0]:
				caught = True
				break
			time.sleep(1)
		self.assertTrue(
		    caught,
		    "standby replay did not reach %s within 60s -- recovery leader "
		    "self-deadlocked replaying dbase_redo while holding "
		    "oTablesMetaLock" % target)

		# Sanity: the new table and the dropped database replicated.
		self.assertEqual(replica.execute("SELECT count(*) FROM t;")[0][0], 0)
		self.assertEqual(
		    replica.execute(
		        "SELECT count(*) FROM pg_database WHERE datname = 'victim';")
		    [0][0], 0)


if __name__ == '__main__':
	unittest.main()
