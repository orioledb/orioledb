#!/usr/bin/env python3
# coding: utf-8

import time

from testgres.enums import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class CheckpointErrorTest(BaseTest):
	"""
	An error raised in the middle of an OrioleDB checkpoint (issue #1053).

	PostgreSQL's checkpointer catches an ERROR and simply tries the checkpoint
	again a second later, with the very same checkpoint number.  OrioleDB has
	already rotated part of its per-tree state to that number by then -- the
	seq buf slot a tree writes its *.map / *.tmp file through is picked as
	`checkpoint number % 2`, and its pages are allocated up front -- and
	nothing rolls that back, so the retry walks into state the failed attempt
	built and asserts on an already allocated slot.
	"""

	def create_table(self, node):
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(
		    'postgres', "CREATE TABLE o_test (\n"
		    "	id integer NOT NULL,\n"
		    "	val text,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n")
		node.safe_psql(
		    'postgres', "INSERT INTO o_test\n"
		    "	(SELECT id, id || 'val' FROM generate_series(1, 2000) id);")

	def test_checkpoint_error_does_not_retry(self):
		"""
		Whatever else goes wrong, the checkpoint must not be tried again.

		orioledb.debug_checkpoint_error stands in for the error the fuzzer hit,
		and raises it in the same place: with the system trees already rotated
		onto this checkpoint number, and no user table touched yet.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.debug_checkpoint_error = on\n")
		node.start()
		self.create_table(node)

		with self.assertRaises(Exception):
			node.safe_psql('postgres', "CHECKPOINT;")

		# The checkpointer took the cluster down with it instead of coming
		# back for another go at the same checkpoint number.
		deadline = time.time() + 30
		while node.status() == NodeStatus.Running and time.time() < deadline:
			time.sleep(0.1)
		self.assertNotEqual(node.status(), NodeStatus.Running)
		node.is_started = False

		with open(node.pg_log_file) as f:
			log = f.read()
		self.assertIn("OrioleDB checkpoint 1 failed", log)
		self.assertNotIn("TRAP", log)
		self.assertEqual(log.count("orioledb checkpoint 1 started"), 1)

		node.append_conf('postgresql.conf',
		                 "orioledb.debug_checkpoint_error = off\n")
		node.start()
		self.assertEqual(
		    node.execute('postgres', "SELECT count(*) FROM o_test;")[0][0],
		    2000)
		node.safe_psql('postgres', "CHECKPOINT;")
		node.stop()

	def fill_lock_table(self, con):
		"""
		Leave the shared lock table with no room left.

		Session-level advisory locks live in the same table as the per-tree
		userlocks the checkpointer takes, and are not released when the
		transaction that ran out of room aborts, so they stay for as long as
		the connection does.
		"""
		with self.assertRaises(Exception):
			con.execute("SELECT pg_advisory_lock(i) "
			            "FROM generate_series(1, 1000000) i;")
		con.rollback()
		self.assertGreater(
		    con.execute(
		        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';")
		    [0][0], 0)

	def test_checkpoint_with_no_lock_table_room(self):
		"""
		A full lock table is what the checkpointer ran into in the wild, and
		it is transient -- it is other backends\' locks that filled it.  So it
		has to wait for room rather than fail.
		"""
		node = self.node
		# The smallest lock table the server accepts, so that a single session
		# can fill it below.
		node.append_conf(
		    'postgresql.conf', "max_connections = 10\n"
		    "max_locks_per_transaction = 10\n")
		node.start()
		self.create_table(node)
		node.safe_psql('postgres', "CHECKPOINT;")

		# Both connections have to be open before the lock table fills up: a
		# fresh backend cannot lock its own database to log in afterwards.
		con_hold = node.connect()
		con_chkp = node.connect()

		self.fill_lock_table(con_hold)

		# The checkpointer gets through the system trees, which take no
		# heavyweight locks, and then has to wait for room for the lock on the
		# first user table.
		t = ThreadQueryExecutor(con_chkp, "CHECKPOINT;")
		t.start()
		t.join(2)
		self.assertTrue(t.is_alive(),
		                "the checkpoint did not wait for the lock table")

		con_hold.execute("SELECT pg_advisory_unlock_all();")
		con_hold.commit()
		t.join()

		# A second one, to show the first left nothing behind for it.
		con_chkp.execute("CHECKPOINT;")
		con_hold.close()
		con_chkp.close()

		self.assertEqual(
		    node.execute('postgres', "SELECT count(*) FROM o_test;")[0][0],
		    2000)

		# And what it wrote must still come back after a crash.
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(
		    node.execute('postgres', "SELECT count(*) FROM o_test;")[0][0],
		    2000)
		node.stop()
