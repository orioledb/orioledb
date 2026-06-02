#!/usr/bin/env python3
# coding: utf-8

import time
import unittest

from .base_test import BaseTest, ThreadQueryExecutor


class ConcurrentIndexTest(BaseTest):
	"""
	Coverage for CREATE INDEX CONCURRENTLY on orioledb tables.

	Current status:

	  - Non-unique, no-concurrent-DML: works (test_cic_basic).
	  - UNIQUE: rejected at the planner gate (test_cic_rejects_unique).
	  - Concurrent DML during build: NOT YET CORRECT.  Writers during
	    the build snapshot do not yet land in the new index because
	    multi-transaction orchestration is incomplete -- their writes
	    miss both the build snapshot and the spool (the OIndex
	    sys-tree row is not yet visible to them during ambuild's
	    transaction).  The infrastructure (state field, spool, capture
	    hooks, REVERSE-op via undo, phase WAL records, drain helper)
	    is in place; what is missing is the multi-txn driver that
	    commits the OIndex with state=BUILDING before the build scan
	    starts.  See test_cic_concurrent_writes_TODO.
	"""

	def test_cic_basic(self):
		"""
		Smoke test: CREATE INDEX CONCURRENTLY on an orioledb table
		without concurrent DML.  Should produce a valid index.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_cic_basic (
					id int NOT NULL,
					val text NOT NULL,
					PRIMARY KEY (id)
				) USING orioledb;
				INSERT INTO o_cic_basic
				SELECT g, 'v' || g FROM generate_series(1, 1000) g;
			""")
			node.safe_psql("CREATE INDEX CONCURRENTLY o_cic_basic_val_idx "
			               "ON o_cic_basic (val);")
			cnt = node.execute("SET enable_seqscan = off; "
			                   "SELECT count(*) FROM o_cic_basic "
			                   "WHERE val = 'v500';")[0][0]
			self.assertEqual(cnt, 1)
			cnt = node.execute("SELECT count(*) FROM o_cic_basic;")[0][0]
			self.assertEqual(cnt, 1000)
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_cic_rejects_unique(self):
		"""
		CREATE UNIQUE INDEX CONCURRENTLY must be rejected when UNIQUE CIC is not yet supported.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_cic_uniq (id int NOT NULL,
					val text NOT NULL,
					PRIMARY KEY (id)) USING orioledb;
			""")
			with self.assertRaises(Exception):
				node.safe_psql(
				    "CREATE UNIQUE INDEX CONCURRENTLY o_cic_uniq_val_uidx "
				    "ON o_cic_uniq (val);")
		finally:
			try:
				node.stop()
			except Exception:
				pass

	@unittest.skip(
	    "CIC multi-txn driver pending; writers during build snapshot are "
	    "not yet captured.  Skeleton kept for the follow-up that wires the "
	    "phased OIndex.state transitions through a separate orchestrator.")
	def test_cic_concurrent_writes_TODO(self):
		"""
		Once the multi-txn driver lands, writers during the build phase
		should be captured to the spool and replayed at phase 4 drain.
		The check: every row visible via PK equals one row visible via
		the new index.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_cic_conc (id int NOT NULL,
					val text NOT NULL, PRIMARY KEY (id)) USING orioledb;
				INSERT INTO o_cic_conc
				SELECT g, 'v' || g FROM generate_series(1, 5000) g;
			""")

			writer = ThreadQueryExecutor(
			    node.connect(), "INSERT INTO o_cic_conc "
			    "SELECT g, 'v' || g FROM generate_series(5001, 10000) g;")
			writer.start()

			node.safe_psql("CREATE INDEX CONCURRENTLY o_cic_conc_val_idx "
			               "ON o_cic_conc (val);")
			writer.join()

			pk_cnt = node.execute("SELECT count(*) FROM o_cic_conc;")[0][0]
			idx_cnt = node.execute("SET enable_seqscan = off; "
			                       "SELECT count(*) FROM o_cic_conc "
			                       "WHERE val LIKE 'v%';")[0][0]
			self.assertEqual(pk_cnt, idx_cnt)
			self.assertEqual(pk_cnt, 10000)
		finally:
			try:
				node.stop()
			except Exception:
				pass
