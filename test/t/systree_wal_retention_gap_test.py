#!/usr/bin/env python3
# coding: utf-8

import threading
import unittest

from .base_test import (BaseTest, ThreadQueryExecutor, wait_stopevent,
                        wait_checkpointer_stopevent)


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
