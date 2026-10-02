#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest


class CommitErrorCheckpointTest(BaseTest):
	"""
	An ERROR raised late in XACT_EVENT_COMMIT, after the commit is durable and
	visible, must not turn into an abort.

	The transaction can no longer be aborted, so the ERROR becomes a PANIC,
	just like PostgreSQL's "cannot abort transaction, it was already
	committed".  Before that the abort quietly skipped the on-commit undo,
	reported the committed transaction as failed, and left the commit
	position flush_local_wal() published in commitInProgressXlogLocation, so
	every later checkpoint waited for it in wait_finish_active_commits().
	"""

	def wait_for_restart(self, node, timeout=60):
		deadline = time.time() + timeout
		while True:
			try:
				node.execute("SELECT 1;")
				return
			except Exception:
				if time.time() > deadline:
					raise
				time.sleep(0.2)

	def test_checkpoint_after_commit_error(self):
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n"
		                 "restart_after_crash = on\n")
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
			with self.assertRaises(Exception):
				con.commit()
			con.close()

			# The PANIC restarts the cluster, and crash recovery keeps the
			# committed row.
			self.wait_for_restart(node)
			with open(node.pg_log_file, errors='replace') as f:
				self.assertRegex(
				    f.read(), "PANIC:  cannot abort transaction [0-9]+, "
				    "it was already committed")
			self.assertEqual(node.execute("SELECT id FROM o_test;"), [(1, )])

			with node.connect(autocommit=True) as con:
				con.execute("SET statement_timeout = '30s';")
				con.execute("CHECKPOINT;")
				con.execute("INSERT INTO o_test VALUES (2);")
				con.execute("CHECKPOINT;")

			node.stop()
			node.start()
			self.assertEqual(node.execute("SELECT id FROM o_test ORDER BY id;"),
			                 [(1, ), (2, )])
		finally:
			node.stop(['-m', 'immediate'])
