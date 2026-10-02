#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class CommitErrorCheckpointTest(BaseTest):
	"""
	An ERROR raised late in XACT_EVENT_COMMIT must not block checkpoints.

	flush_local_wal() publishes the LSN of the commit record in
	commitInProgressXlogLocation, and wal_after_commit() clears it.  Between
	the end of the commit critical section and wal_after_commit() an ERROR is
	an ordinary ERROR.  The transaction is already committed and its oxid is
	already cleared, so the abort that follows takes the branch without
	wal_after_commit() and the slot keeps the commit LSN.  Every later
	checkpoint then waits on that slot in wait_finish_active_commits().
	"""

	def test_checkpoint_after_commit_error(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_test (id integer PRIMARY KEY) USING orioledb;\n")

		try:
			con = node.connect()
			node.safe_psql(
			    "SELECT pg_stopevent_set('before_on_commit_undo_stack', 'true');"
			)
			con.begin()
			con.execute("INSERT INTO o_test VALUES (1);")
			with self.assertRaises(Exception) as e:
				con.commit()
			self.assertIn("error before on-commit undo", str(e.exception))
			con.rollback()
			con.close()
			node.safe_psql(
			    "SELECT pg_stopevent_reset('before_on_commit_undo_stack');")

			# The client got an ERROR, but the transaction is committed.
			self.assertEqual(node.execute("SELECT id FROM o_test;"), [(1, )])

			# Hangs while the slot of the closed session is stale.
			with node.connect(autocommit=True) as con:
				con.execute("SET statement_timeout = '5s';")
				con.execute("CHECKPOINT;")
		finally:
			# The blocked checkpoint blocks a fast shutdown as well.
			node.stop(['-m', 'immediate'])
