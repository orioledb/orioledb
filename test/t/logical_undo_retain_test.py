#!/usr/bin/env python3
# coding: utf-8

import unittest

from threading import Thread

from testgres.enums import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

INVALID_UNDO_LOCATION = 1 << 61
SLOT = 'regression_slot'

# A small system undo log, so that a moderate amount of DDL is enough to push
# the retain floor forward.
SMALL_SYSTEM_UNDO_CONF = """
wal_level = logical
max_wal_senders = 4
max_replication_slots = 4
orioledb.main_buffers = 8MB
orioledb.undo_buffers = 128
orioledb.system_undo_circular_buffer_fraction = 0.05
"""


class LogicalUndoRetainTest(BaseTest):
	"""
	Tests for the system undo log retained for logical decoding: the
	SYS_TREES_CATALOG_XID_UNDO_LOCATION mapping that says how far back it has
	to be kept, and the places that have to honour it.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', SMALL_SYSTEM_UNDO_CONF)

	def start_node(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_decoded (id int PRIMARY KEY, v text) USING orioledb;
		""")
		return node

	def churn(self, con, tag, count, cols=8):
		"""Write system undo: every CREATE TABLE updates o_tables."""
		body = ", ".join(f"c{j} text" for j in range(cols))
		for i in range(count):
			con.execute(f"CREATE TABLE churn_{tag}_{i} "
			            f"(id int PRIMARY KEY, {body}) USING orioledb;")

	def create_slot(self, node):
		node.safe_psql(
		    'postgres', f"SELECT pg_create_logical_replication_slot("
		    f"'{SLOT}', 'test_decoding');")

	def decoded(self, node):
		return [
		    row[0] for row in node.execute(
		        f"SELECT data FROM pg_logical_slot_get_changes("
		        f"'{SLOT}', NULL, NULL);")
		]

	def read_mapping(self, node, xmin):
		return node.execute(
		    f"SELECT orioledb_read_sys_xid_undo_location({xmin});")[0][0]

	def test_mapping_scan_terminates(self):
		"""
		The scan has to walk the mapping page by page.  Restarting it from the
		leftmost leaf, as it used to, never gets past the first page once that
		page has been drained.
		"""
		node = self.start_node()
		node.safe_psql(
		    'postgres',
		    "SELECT orioledb_insert_sys_xid_undo_location(i, i * 1000) "
		    "FROM generate_series(1, 5000) i;")

		# The least location sits on the last page, so answering correctly
		# takes walking all of them.
		node.safe_psql(
		    'postgres',
		    "SELECT orioledb_insert_sys_xid_undo_location(5001, 1);")

		thread = ThreadQueryExecutor(
		    node.connect(autocommit=True),
		    "SELECT orioledb_read_sys_xid_undo_location(2500);")
		thread.start()
		Thread.join(thread, 60)
		if thread.is_alive():
			node.stop(['-m', 'immediate'])
			node.is_started = False
			thread.join(30)
			self.fail("the mapping scan did not terminate")

		self.assertEqual(thread.join()[0][0], 1)

		# Everything below 2500 is gone now, and nothing is at or above 6000.
		self.assertEqual(self.read_mapping(node, 6000), INVALID_UNDO_LOCATION)

		# And again, now that the mapping is empty.
		self.assertEqual(self.read_mapping(node, 6000), INVALID_UNDO_LOCATION)

	def write_records_for_an_idle_slot(self, node):
		self.create_slot(node)
		node.safe_psql('postgres',
		               "INSERT INTO o_decoded VALUES (1, 'before-restart');")
		# o_tables has to move on after that INSERT's CSN, so that reading it
		# back at that CSN means walking the system undo.
		with node.connect(autocommit=True) as con:
			self.churn(con, 'r', 40)

	def check_survived_restart(self, node):
		self.assertEqual(node.status(), NodeStatus.Running,
		                 "node did not come back up")
		self.assertIn(
		    "table public.o_decoded: INSERT: id[integer]:1 v[text]:'before-restart'",
		    self.decoded(node))

	def test_idle_slot_survives_restart(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		node.restart()

		self.check_survived_restart(node)

	@unittest.skip("a page undo chain can still lead below the retained floor "
               "after a crash: see issue #1081")
	def test_idle_slot_survives_crash_restart(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		node.stop(['-m', 'immediate'])
		node.start()

		self.check_survived_restart(node)

	@unittest.skip("a page undo chain can still lead below the retained floor "
               "after a crash: see issue #1081")
	def test_idle_slot_survives_crash_restart_after_checkpoint(self):
		node = self.start_node()
		self.write_records_for_an_idle_slot(node)

		# The undo the slot needs is now behind the checkpoint the crash will
		# recover from, so keeping it takes the checkpoint having recorded it.
		node.safe_psql('postgres', "CHECKPOINT;")
		with node.connect(autocommit=True) as con:
			self.churn(con, 'after_chkp', 40)

		node.stop(['-m', 'immediate'])
		node.start()

		self.check_survived_restart(node)
