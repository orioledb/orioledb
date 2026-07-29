#!/usr/bin/env python3
# coding: utf-8

import threading
import unittest

from .base_test import BaseTest, wait_checkpointer_stopevent


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


if __name__ == '__main__':
	unittest.main()
