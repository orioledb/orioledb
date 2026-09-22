#!/usr/bin/env python3
# coding: utf-8

from threading import Thread

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

INVALID_UNDO_LOCATION = 1 << 61


class LogicalUndoRetainTest(BaseTest):
	"""
	Tests for the SYS_TREES_CATALOG_XID_UNDO_LOCATION mapping, which tells how
	far the system undo log has to be retained for logical decoding.
	"""

	def start_node(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		return node

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
