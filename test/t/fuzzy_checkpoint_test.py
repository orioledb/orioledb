#!/usr/bin/env python3
# coding: utf-8

import threading
import time
import unittest

from .base_test import (BaseTest, ThreadQueryExecutor, wait_stopevent,
                        wait_checkpointer_stopevent)


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


class SysTreeStageTest(BaseTest):
	"""
	Drive a data transaction's ABORT into the front sys-tree-consistency stage
	window [sysTreesStartPtr, replayStartPtr) by PARKING the checkpointer at the
	new checkpoint_before_replay_start stop event -- which fires after
	sysTreesStartPtr is pinned (and checkpoint_sys_trees has run) but before
	replayStartPtr is pinned.  WAL written while the checkpointer is parked
	therefore lands strictly inside the stage-0 window.

	Two variants of when the aborting txn does its work:
	 A) modifies BEFORE the checkpoint (in-progress at the sysTreesStartPtr pin),
	    ROLLBACK while the checkpointer is parked;
	 B) modifies AND ROLLBACK both while the checkpointer is parked.

	On crash recovery the txn's WAL_REC_ROLLBACK sits in the stage-0 window and
	the leader runs recovery_finish_current_oxid(ABORTED) there.  The txn's
	*current* rebuilt data undo stack is empty in that window (its data modifies
	are < replayStartPtr, so they are skipped and the Regular stack is never
	rebuilt), but its data undo captured in the checkpoint's xids file
	(checkpoint_undo_stacks) is NOT empty.  walk_checkpoint_stacks() therefore
	parks that data-log undo on deferred_checkpoint_undo and orioledb_redo()
	drains it at the replayStartPtr crossing -- applying it earlier would fetch a
	data descriptor before the sys trees are consistent.  Each test asserts
	recovery completes with the aborted work reverted.
	"""

	def _park_checkpoint(self, node):
		node.safe_psql(
		    "SELECT pg_stopevent_set('checkpoint_before_replay_start', 'true');"
		)
		err = []

		def _do_ckpt():
			try:
				node.safe_psql("CHECKPOINT;")
			except Exception as e:  # noqa: BLE001
				err.append(e)

		t = threading.Thread(target=_do_ckpt)
		t.start()
		wait_checkpointer_stopevent(node)
		return t, err

	def _release_and_recover(self, node, t, err):
		node.safe_psql(
		    "SELECT pg_stopevent_reset('checkpoint_before_replay_start');")
		t.join()
		if err:
			raise err[0]
		node.stop(['-m', 'immediate'])
		node.start()

	def test_stage0_abort_inprogress_at_pin(self):
		"""Variant A: modifies before the pin, ROLLBACK during the park."""
		node = self.node
		node.append_conf("orioledb.enable_stopevents = on\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql(
		    "CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;")
		node.safe_psql(
		    "INSERT INTO t SELECT i, i FROM generate_series(1,100) i;")
		node.safe_psql("CHECKPOINT;")

		# In-progress txn: modifies BEFORE the checkpoint (so < sysTreesStartPtr).
		con = node.connect()
		con.begin()
		con.execute(
		    "INSERT INTO t SELECT g, g FROM generate_series(1000,5000) g;")
		con.execute("UPDATE t SET v = v + 1 WHERE id <= 100;")

		t, err = self._park_checkpoint(node)
		# ROLLBACK while parked -> its WAL lands in [sysTreesStartPtr, replayStartPtr).
		con.rollback()
		con.close()
		self._release_and_recover(node, t, err)

		self.assertEqual(node.execute("SELECT count(*) FROM t;")[0][0], 100)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM t WHERE id >= 1000;")[0][0], 0)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM t WHERE v <> id;")[0][0], 0)

	def test_stage0_abort_all_in_window(self):
		"""Variant B: modifies AND ROLLBACK both during the park."""
		node = self.node
		node.append_conf("orioledb.enable_stopevents = on\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql(
		    "CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;")
		node.safe_psql(
		    "INSERT INTO t SELECT i, i FROM generate_series(1,100) i;")
		node.safe_psql("CHECKPOINT;")

		t, err = self._park_checkpoint(node)
		# Whole aborting txn while parked -> modifies + rollback in the window.
		con = node.connect()
		con.begin()
		con.execute(
		    "INSERT INTO t SELECT g, g FROM generate_series(1000,5000) g;")
		con.execute("UPDATE t SET v = v + 1 WHERE id <= 100;")
		con.rollback()
		con.close()
		self._release_and_recover(node, t, err)

		self.assertEqual(node.execute("SELECT count(*) FROM t;")[0][0], 100)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM t WHERE id >= 1000;")[0][0], 0)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM t WHERE v <> id;")[0][0], 0)


class SysTreeXidsGapTest(BaseTest):
	"""
	Repro: checkpoint_sys_trees() (checkpoint.c ~1471) captures the fuzzy
	sys-tree image BEFORE before_writing_xids_file()/start_write_xids()
	(~1511-1512) arm the per-proc xids-queue flush.  apply_undo_stack()
	(transam/undo.c:1459) only records a finishing oxid into the xids file
	when curProcData->flushUndoLocations is already true; before that it's a
	silent no-WAL revert of the live buffer only.

	So a transaction that:
	  1) does a sys-tree write (DDL) whose meta-lock window is already CLOSED
	     well before sysTreesStartPtr is pinned (so oldest_open_ddl_window()
	     does not see it and does not clamp sysTreesStartPtr back for it),
	  2) then ROLLS BACK anywhere in [sysTreesStartPtr, start_write_xids())
	     -- a window spanning the *entire* checkpoint_sys_trees() walk --
	is invisible to both WAL replay (abort commonly takes the no-WAL fast
	path -- no WAL_REC_ROLLBACK) and to the xids file (armed too late). If
	checkpoint_sys_trees() already wrote the dirty (pre-rollback) page to
	disk before the rollback's undo reverted the live page, the on-disk
	sys-tree image keeps the should-have-been-reverted change with no
	reconciliation path left for recovery.

	We park the checkpointer at the existing checkpoint_before_replay_start
	stop event (fires right after checkpoint_sys_trees() returns, before
	start_write_xids()) and roll back an ADD COLUMN done earlier in its own
	still-open transaction, entirely before the checkpoint started.
	"""

	def test_ddl_rollback_in_xids_gap_survives_crash(self):
		node = self.node
		node.append_conf("orioledb.enable_stopevents = on\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql(
		    "CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;")
		node.safe_psql(
		    "INSERT INTO t SELECT i, i FROM generate_series(1,10) i;")
		node.safe_psql("CHECKPOINT;")

		# meta lock is acquired+released synchronously inside the ALTER
		# statement, well before the checkpoint below even starts; the
		# enclosing transaction is left open (no COMMIT/ROLLBACK yet), so
		# oldest_open_ddl_window() will not see/clamp for it.
		con = node.connect()
		con.begin()
		con.execute("ALTER TABLE t ADD COLUMN v2 int DEFAULT 42;")
		self.assertEqual(con.execute("SELECT v2 FROM t LIMIT 1;")[0][0], 42)

		node.safe_psql(
		    "SELECT pg_stopevent_set('checkpoint_before_replay_start', 'true');"
		)
		ckpt_err = []

		def _do_ckpt():
			try:
				node.safe_psql("CHECKPOINT;")
			except Exception as e:  # noqa: BLE001
				ckpt_err.append(e)

		t_ckpt = threading.Thread(target=_do_ckpt)
		t_ckpt.start()
		wait_checkpointer_stopevent(node)

		# checkpoint_sys_trees() has already captured O_TABLES on disk WITH
		# v2 present (T's ADD COLUMN, still uncommitted).  Roll back now,
		# while flushUndoLocations is not yet armed for this proc.
		con.rollback()
		con.close()

		node.safe_psql(
		    "SELECT pg_stopevent_reset('checkpoint_before_replay_start');")
		t_ckpt.join()
		if ckpt_err:
			raise ckpt_err[0]

		node.stop(['-m', 'immediate'])
		node.start()

		# Ground truth (pg_attribute, standard heap MVCC/WAL -- unaffected by
		# the orioledb-internal gap): the ADD COLUMN was rolled back.
		cols = node.execute(
		    "SELECT count(*) FROM information_schema.columns "
		    "WHERE table_name = 't' AND column_name = 'v2';")[0][0]
		self.assertEqual(
		    cols, 0, "pg_attribute unexpectedly kept v2 across the rollback "
		    "(unrelated failure, not this gap)")

		# orioledb's OWN o_tables sys-tree metadata, read directly --
		# independent of pg_attribute.  If the gap leaked the pre-rollback
		# page into the checkpoint image, this will still mention v2 even
		# though pg_attribute (above) correctly does not.
		descr = node.execute(
		    "SELECT orioledb_table_description('t'::regclass);")[0][0]
		self.assertNotIn(
		    'v2', descr,
		    "orioledb o_tables metadata kept the rolled-back ADD COLUMN "
		    "(checkpoint_sys_trees()-before-start_write_xids() ordering "
		    "gap): %r" % (descr, ))

		self.assertEqual(node.execute("SELECT count(*) FROM t;")[0][0], 10)


class SysTreeWalRetentionGapTest(BaseTest):
	"""
	Was a repro for: o_perform_checkpoint() used to clamp sysTreesStartPtr
	back to oldest_open_ddl_window() -- the WAL start of the oldest
	still-open O_TABLES/O_INDICES meta-lock DDL window -- with no lower
	bound.  That clamp could point sysTreesStartPtr at a WAL position
	already removed by this same checkpoint's own ordinary WAL housekeeping
	(KeepLogSeg/RemoveOldXlogFiles, driven by Postgres's own checkPointRedo,
	never consulted by the clamp), and even when the segment survived,
	Postgres's own single WAL redo pass starts at checkPointRedo -- never at
	the clamped-back sysTreesStartPtr -- so the "replay the open DDL window
	forward" reconciliation the clamp existed for silently never engaged
	either way.

	FIX: the clamp/oldest_open_ddl_window() machinery was removed entirely.
	sysTreesStartPtr is now unconditionally get_checkpoint_xlog_ptr(), taken
	right before the sys-tree walk, with before_writing_xids_file()/
	start_write_xids() armed just before that (previously they only preceded
	the PK/data walk).  This gives sys trees the same two mechanisms PK/data
	already relied on: any WAL record before the snapshot is already
	reflected in its page (buffers are modified before their XLogInsert()
	returns), and anything at/after it is covered by forward replay -- plus
	the xids file for anything that finishes (commit or abort) while the
	walk is running.  See the discussion that led here for the full
	reasoning; no separate clamp is needed.

	This test now checks that the correct behavior actually holds: a DDL
	transaction that is still holding oTablesMetaLock while an ordinary
	CHECKPOINT captures the sys trees, and only commits afterwards, still
	recovers correctly across a crash -- with no clamp protecting it at all.
	"""

	def test_open_ddl_window_across_checkpoint_recovers(self):
		node = self.node
		node.append_conf("orioledb.enable_stopevents = on\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql(
		    "CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;")
		node.safe_psql(
		    "INSERT INTO t SELECT i, i FROM generate_series(1,10) i;")
		node.safe_psql("CREATE TABLE filler (id int, v text);")
		node.safe_psql("CHECKPOINT;")

		# Park a DDL backend HOLDING oTablesMetaLock (meta_lock() has run;
		# meta_unlock() -- which would release it -- has not run yet).  Its
		# own O_TABLES modify is already applied to the live page at this
		# point (systrees_modify_end flushed it) even though the SQL
		# statement has not returned.
		con_ddl = node.connect()
		ddl_pid = con_ddl.execute("SELECT pg_backend_pid();")[0][0]
		node.safe_psql(
		    "SELECT pg_stopevent_set('before_o_tables_meta_unlock', 'true');")
		t_ddl = ThreadQueryExecutor(con_ddl,
		                            "ALTER TABLE t ADD COLUMN v2 int;")
		t_ddl.start()
		wait_stopevent(node, ddl_pid)

		# Some concurrent WAL, with the DDL window still open, so the
		# checkpoint below is an otherwise perfectly ordinary one.
		node.safe_psql("INSERT INTO filler SELECT i, repeat('x', 900) "
		               "FROM generate_series(1, 1000) i;")

		# Run the CHECKPOINT in the background, parked at
		# checkpoint_before_replay_start -- right after the (now unclamped)
		# sys-tree walk, before the table walk, which needs oTablesMetaLock
		# EXCLUSIVE (acquire_chkp_lock_drain).  We must release the DDL's
		# SHARED hold before letting the checkpoint proceed past this
		# point, or the checkpoint's own exclusive acquire deadlocks against
		# the DDL we are holding parked -- a test-harness artifact, not
		# anything under test.
		node.safe_psql(
		    "SELECT pg_stopevent_set('checkpoint_before_replay_start', 'true');"
		)
		ckpt_err = []

		def _do_ckpt():
			try:
				node.safe_psql("CHECKPOINT;")
			except Exception as e:  # noqa: BLE001
				ckpt_err.append(e)

		t_ckpt = threading.Thread(target=_do_ckpt)
		t_ckpt.start()
		wait_checkpointer_stopevent(node)

		# The sys-tree walk (and its snapshot of sysTreesStartPtr) already
		# ran with the DDL window open -- exactly the scenario under test.
		# Release the DDL now: it COMMITS, unlike the companion
		# systree_xids_gap_test.py (which rolls back in the same kind of
		# window).  This is the case the old clamp existed to protect and
		# that must still recover correctly with no clamp at all.
		node.safe_psql(
		    "SELECT pg_stopevent_reset('before_o_tables_meta_unlock');")
		t_ddl.join()
		con_ddl.commit()
		con_ddl.close()

		node.safe_psql(
		    "SELECT pg_stopevent_reset('checkpoint_before_replay_start');")
		t_ckpt.join()
		if ckpt_err:
			raise ckpt_err[0]

		node.stop(['-m', 'immediate'])
		node.start()

		# The ADD COLUMN committed after the checkpoint captured its window
		# open; it must be there, correctly, after crash recovery.
		self.assertEqual(
		    node.execute(
		        "SELECT count(*) FROM information_schema.columns "
		        "WHERE table_name = 't' AND column_name = 'v2';")[0][0], 1)
		descr = node.execute(
		    "SELECT orioledb_table_description('t'::regclass);")[0][0]
		self.assertIn('v2', descr)
		self.assertEqual(node.execute("SELECT count(*) FROM t;")[0][0], 10)
		node.safe_psql("INSERT INTO t VALUES (11, 11, 11);")
		self.assertEqual(
		    node.execute("SELECT v2 FROM t WHERE id = 11;")[0][0], 11)


if __name__ == '__main__':
	unittest.main()
