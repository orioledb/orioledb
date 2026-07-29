#!/usr/bin/env python3
# coding: utf-8

import threading
import unittest

from .base_test import BaseTest, wait_checkpointer_stopevent


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
	the leader runs recovery_finish_current_oxid(ABORTED) there (confirmed via
	diagnostics: aborted=1 while !systrees_consistent).  Its data (UndoLogRegular)
	undo is empty in that window -- the data modifies are < replayStartPtr, so
	they are skipped and the Regular undo stack is never rebuilt -- so the
	stage-0 abort undo is a no-op and needs no deferral.  Each test asserts
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


if __name__ == '__main__':
	unittest.main()
