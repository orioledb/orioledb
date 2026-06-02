#!/usr/bin/env python3
# coding: utf-8

import time
import unittest

from .base_test import BaseTest, ThreadQueryExecutor


class ConcurrentIndexTest(BaseTest):
	"""
	Coverage for CREATE INDEX CONCURRENTLY on orioledb tables.

	Native btree CIC path:
	  - PG's DefineIndex(concurrent=true) drives the standard CIC
	    orchestration.
	  - orioledb_ambuild installs OIndex with
	    state=BUILDING_PHASE_2 and skips the actual build.  Other
	    backends see BUILDING via the in-progress snapshot the
	    OIndex sys-tree uses by default; their DML routes through
	    the spool.
	  - PG's phase-3 validate_index calls
	    orioledb_index_validate_scan, which runs the real build,
	    drains the spool under a brief ShareLock, and flips state
	    to VALID.

		"""

	def test_cic_basic(self):
		"""
		CIC on a small table without concurrent DML.  Should produce
		a queryable, complete index.
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

	def test_cic_unique_strict_rejects(self):
		"""
		CREATE UNIQUE INDEX CONCURRENTLY is rejected for orioledb
		when orioledb.use_orioledb_strict_mode is on.
		"""
		node = self.node
		node.append_conf("orioledb.strict_mode = on")
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

	def test_cic_unique_compat_downgrades(self):
		"""
		CREATE UNIQUE INDEX CONCURRENTLY is downgraded to a plain
		CREATE UNIQUE INDEX with a WARNING in compat mode (default).
		Uniqueness must still be enforced.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_cic_uniq_compat (
					id int NOT NULL,
					val text NOT NULL,
					PRIMARY KEY (id)
				) USING orioledb;
				INSERT INTO o_cic_uniq_compat VALUES (1, 'a'), (2, 'b');
			""")
			_, _, err = node.psql(
			    "CREATE UNIQUE INDEX CONCURRENTLY o_cic_uniq_compat_val_uidx "
			    "ON o_cic_uniq_compat (val);")
			self.assertIn(
			    b"WARNING:  CREATE UNIQUE INDEX CONCURRENTLY is not yet "
			    b"supported for orioledb tables, using a plain "
			    b"CREATE UNIQUE INDEX instead", err)
			# Uniqueness is enforced by the plain index that was built.
			with self.assertRaises(Exception):
				node.safe_psql(
				    "INSERT INTO o_cic_uniq_compat VALUES (3, 'a');")
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_cic_concurrent_writes(self):
		"""
		Writers during the build snapshot should be captured to the
		spool and applied at validate-scan drain.  Acceptance: every
		row visible via PK equals one row visible via the new index.
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

			# Implicit autocommit on testgres connections is False, so
			# wrap the INSERT in BEGIN/COMMIT to make the writes
			# visible after the thread exits.
			writer_conn = node.connect()
			writer = ThreadQueryExecutor(
			    writer_conn, "BEGIN;"
			    "INSERT INTO o_cic_conc "
			    "SELECT g, 'v' || g FROM generate_series(5001, 10000) g;"
			    "COMMIT;")
			writer.start()

			node.safe_psql("CREATE INDEX CONCURRENTLY o_cic_conc_val_idx "
			               "ON o_cic_conc (val);")
			writer.join()
			writer_conn.close()

			pk_cnt = node.execute("SELECT count(*) FROM o_cic_conc;")[0][0]
			with node.connect() as c:
				c.execute("SET enable_seqscan = off")
				# Use a range predicate (avoids psycopg2's '%' format-arg
				# parsing, which trips on 'v%').
				idx_cnt = c.execute("SELECT count(*) FROM o_cic_conc "
				                    "WHERE val >= 'v' AND val < 'w';")[0][0]
			self.assertEqual(pk_cnt, idx_cnt)
			self.assertEqual(pk_cnt, 10000)
		finally:
			try:
				node.stop()
			except Exception:
				pass
