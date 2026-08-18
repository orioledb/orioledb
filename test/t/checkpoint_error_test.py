#!/usr/bin/env python3
# coding: utf-8

import time

from testgres.enums import NodeStatus

from .base_test import BaseTest


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
