#!/usr/bin/env python3
# coding: utf-8

import unittest

from testgres.connection import DatabaseError
from testgres.enums import NodeStatus

from .base_test import BaseTest


class SeqScanResownerTest(BaseTest):
	"""
    Tests setup some edge cases for seq scan and transaction aborts and
    asserts they are handled cleanly.

	A sequential scan falling back to iterator mode (scan_make_iterator())
	pins the index descriptor on the ResourceOwner that is current while the
	iterator is built. Only the owner should ever drop the pin.

	  * on transaction abort the scans are cleaned up from undo_xact_callback()
	    after AtAbort_ResourceOwner() has already switched CurrentResourceOwner
	    to TopTransactionResourceOwner. Dropping the pin with
        CurrentResourceOwner raises an error
	    "OrioleDB OIndexDescr...is not owned by resource owner TopTransaction"

	  * on subtransaction abort the scan is released by the pinning owner's own
	    release callback instead, where a forget is rejected

	Both errors are raised inside the critical section of
	free_btree_seq_scan_internal(), which promotes them to PANIC.
	"""

	N = 4000
	BOOM_ID = 500

	def setUp(self):
		super().setUp()
		node = self.node
		node.append_conf(
		    'postgresql.conf', "shared_preload_libraries = orioledb\n"
		    "orioledb.main_buffers = 8MB\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.debug_disable_bgwriter = true\n")
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("""
			CREATE TABLE o_scan_resowner (
				id int PRIMARY KEY,
				payload text
			) USING orioledb;

			INSERT INTO o_scan_resowner
				SELECT g, repeat('x', 100) FROM generate_series(1, %d) g;

			CREATE FUNCTION scan_probe(id int) RETURNS int AS $$
			BEGIN
				IF id = 1 THEN
					-- The scan already holds its image of the internal page:
					-- evicting the leaves invalidates every downlink it is
					-- about to follow, so the remaining leaves are read
					-- through fallback iterators.
					PERFORM orioledb_evict_pages('o_scan_resowner'::regclass, 0);
				ELSIF id = %d THEN
					RAISE EXCEPTION 'boom';
				END IF;
				RETURN id;
			END $$ LANGUAGE plpgsql;
		""" % (self.N, self.BOOM_ID))

	def _log(self):
		with open(self.node.pg_log_file, errors='replace') as f:
			return f.read()

	def assertScanFellBackToIterator(self):
		self.assertIn('scan_make_iterator', self._log(),
		              "the sequential scan never fell back to an iterator")

	def assertNoPanic(self):
		self.assertEqual(self.node.status(), NodeStatus.Running,
		                 "the backend did not survive the cleanup")
		log = self._log()
		for mark in ('PANIC', 'is not owned by resource owner',
		             'after release started'):
			self.assertNotIn(mark, log)

	def test_xact_abort_with_fallback_iterator(self):
		"""Transaction abort for seq scan with fallback iterator."""
		with self.node.connect() as con:
			con.execute("SET log_min_messages = 'debug3';")
			with self.assertRaises(DatabaseError):
				con.execute("""
					DO $$
					BEGIN
						PERFORM scan_probe(id) FROM o_scan_resowner;
					END $$;
				""")
			con.rollback()
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_scan_resowner;"),
			    [(self.N, )])
		self.assertScanFellBackToIterator()
		self.assertNoPanic()

	def test_plain_statement_abort_with_fallback_iterator(self):
		"""
        Transaction abort for seq scan with fallback iterator
        while the scan is running.
		"""
		with self.node.connect() as con:
			con.execute("SET log_min_messages = 'debug3';")
			with self.assertRaises(DatabaseError):
				con.execute("SELECT scan_probe(id) FROM o_scan_resowner;")
			con.rollback()
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_scan_resowner;"),
			    [(self.N, )])
		self.assertScanFellBackToIterator()
		self.assertNoPanic()

	def test_subxact_abort_with_fallback_iterator(self):
		"""
		Subtransaction abort for a seq scan with fallback iterator: the
		scan must ONLY be freed by the release callback of the owner
        that pinned the descriptor.
		"""
		with self.node.connect() as con:
			con.execute("SET log_min_messages = 'debug3';")
			con.execute("""
				DO $$
				BEGIN
					BEGIN
						PERFORM scan_probe(id) FROM o_scan_resowner;
					EXCEPTION WHEN others THEN
						RAISE NOTICE 'boom caught';
					END;
				END $$;
			""")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_scan_resowner;"),
			    [(self.N, )])
			con.commit()
		self.assertScanFellBackToIterator()
		self.assertNoPanic()


if __name__ == "__main__":
	unittest.main()
