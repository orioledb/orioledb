#!/usr/bin/env python3
# coding: utf-8

import threading
import time
import unittest

from .base_test import (BaseTest, ThreadQueryExecutor, wait_stopevent,
                        wait_checkpointer_stopevent)


class SysTreeStage0AbortUndoTest(BaseTest):
	"""
	Repro: an ABORT whose WAL_REC_ROLLBACK lands inside the front
	sys-tree-consistency window [sysTreesStartPtr, replayStartPtr) can still get
	its DATA (UndoLogRegular) undo stack into the checkpoint's xids file.
	Recovery then replays that ROLLBACK in stage 0 and walk_checkpoint_stacks()
	applies the data undo there -- fetching a data descriptor while the sys
	trees are still being reconciled.

	checkpoint_state->xidsQueueSysTreeOnly is meant to prevent exactly this, but
	it measures the wrong instant.  Its contract (checkpoint.h) is "whatever
	finishes after the flag clears is, by the single WAL stream's own ordering,
	at or after replayStartPtr" -- yet "finishes" is evaluated at two different
	moments:

	  * wal_rollback() writes WAL_REC_ROLLBACK BEFORE apply_undo_stack() is
	    entered (transam/undo.c, XACT_EVENT_ABORT);
	  * walk_undo_stack() queues the xids record AFTER
	    walk_undo_range_with_buf() has finished applying the undo.

	Everything between those two points is unguarded, and an abort can sit there
	for as long as its undo takes.  Two independent leaks follow:

	  (a) the abort is still undoing when the flag clears, so walk_undo_stack()
	      queues its Regular location itself;
	  (b) the abort has not finished undoing by the time the checkpoint closes
	      the xids file, so finish_write_xids() snapshots it -- and that path
	      writes every checkpointable undo log (GetCheckpointableUndoLog: 0 ->
	      UndoLogRegular, 1 -> UndoLogSystem) with no xidsQueueSysTreeOnly check
	      at all.

	Commits are not affected: on_commit_undo_stack() runs before
	wal_after_commit() clears commitInProgressXlogLocation, so the checkpointer's
	drain loop genuinely waits for a commit's queueing.  Aborts never stamp that
	field (flush_local_wal(false, ...)), so nothing waits for them.

	Both leaks are driven here by parking the aborting backend at
	before_apply_undo -- which fires at the top of walk_undo_stack(), i.e. after
	WAL_REC_ROLLBACK is already in the stream and before any undo is applied or
	queued.

	Detected by check_no_stage0_data_undo() in recovery.c, which PANICs when a
	data-log checkpoint undo stack is applied for an oxid finishing below
	replayStartPtr.  Recovery therefore fails to complete and node.start()
	raises.
	"""

	@staticmethod
	def _wait_stopevent_named(node, pid, name):
		"""wait_stopevent(), but pinned to one specific stop event."""
		while node.execute("""SELECT EXISTS(
								 SELECT se.* FROM pg_stopevents() se
								 WHERE se.stopevent = '%s'
								   AND se.waiter_pids @> ARRAY[%d]);""" %
		                   (name, pid))[0][0] is False:
			time.sleep(0.1)

	@staticmethod
	def _checkpointer_pid(node):
		pid = None
		while pid is None:
			rows = node.execute("SELECT pid FROM pg_stat_activity "
			                    "WHERE backend_type = 'checkpointer';")
			if rows and rows[0]:
				pid = rows[0][0]
		return pid

	def _setup(self):
		node = self.node
		node.append_conf("orioledb.enable_stopevents = on\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql(
		    "CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;")
		node.safe_psql(
		    "INSERT INTO t SELECT i, i FROM generate_series(1,100) i;")
		node.safe_psql("CHECKPOINT;")
		return node

	@staticmethod
	def _start_checkpoint(node):
		"""Start a CHECKPOINT parked at checkpoint_before_replay_start."""
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

	@staticmethod
	def _open_aborting_txn(node, park):
		"""
		Open a txn that has dirtied data.  With park=True it will stop inside
		its own abort, right after WAL_REC_ROLLBACK and before any undo runs.
		"""
		con = node.connect()
		con.execute("SET application_name = 'undoparker';")
		pid = con.execute("SELECT pg_backend_pid();")[0][0]
		con.begin()
		con.execute("UPDATE t SET v = v + 1000;")
		con.execute(
		    "INSERT INTO t SELECT g, g FROM generate_series(1000,3000) g;")
		if park:
			node.safe_psql("SELECT pg_stopevent_set('before_apply_undo', "
			               "'$applicationName == \"undoparker\"');")
		return con, pid

	def _assert_rolled_back(self, node):
		self.assertEqual(node.execute("SELECT count(*) FROM t;")[0][0], 100)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM t WHERE v <> id;")[0][0], 0)

	def test_control_abort_completes_inside_window(self):
		"""
		NEGATIVE CONTROL -- the case xidsQueueSysTreeOnly does cover, and the
		one systree_stage_test already exercises.

		The abort runs to completion while the checkpointer is parked, so the
		flag is still set when walk_undo_stack() would have queued the Regular
		location, and the oxid is long gone by finish_write_xids().  No data-log
		stack reaches the xids file and recovery must complete cleanly.
		"""
		node = self._setup()
		con, _ = self._open_aborting_txn(node, park=False)
		t_ckpt, err = self._start_checkpoint(node)

		con.rollback()
		con.close()

		node.safe_psql(
		    "SELECT pg_stopevent_reset('checkpoint_before_replay_start');")
		t_ckpt.join()
		if err:
			raise err[0]

		node.stop(['-m', 'immediate'])
		node.start()
		self._assert_rolled_back(node)

	def test_stage0_data_undo_via_xids_snapshot(self):
		"""
		Leak (b): the abort is still undoing when finish_write_xids() runs, so
		the in-progress snapshot records its Regular stack unconditionally.
		"""
		node = self._setup()
		con, pid = self._open_aborting_txn(node, park=True)
		t_ckpt, err = self._start_checkpoint(node)

		# ROLLBACK inside the window; parks before applying/queueing undo.
		t_rb = ThreadQueryExecutor(con, "ROLLBACK;")
		t_rb.start()
		wait_stopevent(node, pid)

		# Release the checkpointer only.  The txn stays parked mid-abort, so
		# finish_write_xids() snapshots it with its full Regular + System stacks.
		node.safe_psql(
		    "SELECT pg_stopevent_reset('checkpoint_before_replay_start');")
		t_ckpt.join()
		if err:
			raise err[0]

		node.stop(['-m', 'immediate'])
		try:
			t_rb.join()
		except Exception:  # noqa: BLE001 -- connection died with the node
			pass
		con.close()

		node.start()
		self._assert_rolled_back(node)

	def test_stage0_data_undo_queued_after_flag_cleared(self):
		"""
		Leak (a): the abort finishes completely BEFORE finish_write_xids(), so
		the snapshot path cannot be responsible -- walk_undo_stack() queued the
		Regular location itself, after the flag was cleared, for a ROLLBACK
		record that sits below replayStartPtr.  This is precisely what
		xidsQueueSysTreeOnly's comment says cannot happen.
		"""
		node = self._setup()
		con, pid = self._open_aborting_txn(node, park=True)
		t_ckpt, err = self._start_checkpoint(node)
		ckpt_pid = self._checkpointer_pid(node)

		t_rb = ThreadQueryExecutor(con, "ROLLBACK;")
		t_rb.start()
		wait_stopevent(node, pid)

		# Re-park the checkpointer immediately after it clears
		# xidsQueueSysTreeOnly, so it cannot reach finish_write_xids() before
		# the aborting txn is completely done.
		node.safe_psql(
		    "SELECT pg_stopevent_set('checkpoint_table_start', 'true');")
		node.safe_psql(
		    "SELECT pg_stopevent_reset('checkpoint_before_replay_start');")
		self._wait_stopevent_named(node, ckpt_pid, 'checkpoint_table_start')

		# Let the abort apply and QUEUE its undo -- the flag is already clear.
		node.safe_psql("SELECT pg_stopevent_reset('before_apply_undo');")
		t_rb.join()
		con.close()

		node.safe_psql("SELECT pg_stopevent_reset('checkpoint_table_start');")
		t_ckpt.join()
		if err:
			raise err[0]

		node.stop(['-m', 'immediate'])
		node.start()
		self._assert_rolled_back(node)


if __name__ == '__main__':
	unittest.main()
