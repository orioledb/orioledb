#!/usr/bin/env python3
# coding: utf-8

import os
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

	def test_cic_writer_spans_all_phases(self):
		"""
		Stress test for the would-be micro-window between PG's phase-1
		commit and our ambuild's OIndex insert.  A writer connection is
		BEGIN'd before CIC starts and continues inserting throughout
		CIC's life.  Expectation: every row visible via PK is also
		visible via the new index, even if the writer commits at any
		point during phases 1/2/3.
		"""
		import threading

		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_cic_span (
					id int NOT NULL,
					val text NOT NULL,
					PRIMARY KEY (id)
				) USING orioledb;
				INSERT INTO o_cic_span
				SELECT g, 'v' || g FROM generate_series(1, 1000) g;
			""")

			# Writer in a separate thread that streams INSERTs in tiny
			# transactions for 3 seconds.  Some commits will land in
			# pre-CIC, some in phase 1, some in phase 2 (ambuild), some
			# in phase 3 (validate_scan build+drain), some after VALID.
			stop = threading.Event()
			inserted = [1000]  # next id

			def writer():
				with node.connect() as c:
					while not stop.is_set():
						i = inserted[0]
						inserted[0] = i + 1
						c.begin()
						c.execute("INSERT INTO o_cic_span(id, val) "
						          "VALUES (" + str(i + 1) + ", 'v" +
						          str(i + 1) + "')")
						c.commit()

			t = threading.Thread(target=writer)
			t.start()
			try:
				# Let some writes accumulate then run CIC.
				time.sleep(0.5)
				node.safe_psql("CREATE INDEX CONCURRENTLY o_cic_span_val_idx "
				               "ON o_cic_span (val);")
				time.sleep(0.5)
			finally:
				stop.set()
				t.join()

			pk_cnt = node.execute("SELECT count(*) FROM o_cic_span;")[0][0]
			with node.connect() as c:
				c.execute("SET enable_seqscan = off")
				idx_cnt = c.execute("SELECT count(*) FROM o_cic_span "
				                    "WHERE val >= 'v' AND val < 'w';")[0][0]
			self.assertEqual(
			    pk_cnt, idx_cnt,
			    f"pk={pk_cnt} idx={idx_cnt}: rows missing from "
			    f"index built concurrently with writer")
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_cic_crash_orphan_cleanup(self):
		"""
		A stale cic_<...>/spool_<...>.bin left in orioledb_data from a
		crashed CIC must be removed by the WAL recovery cleanup hook on
		next startup.  Simulated here by planting the dir while the node
		is stopped, then starting it.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_cic_orphan (
					id int NOT NULL,
					val text NOT NULL,
					PRIMARY KEY (id)
				) USING orioledb;
				INSERT INTO o_cic_orphan
				SELECT g, 'v' || g FROM generate_series(1, 10) g;
				CHECKPOINT;
			""")
		finally:
			# SIGKILL: ensures actual WAL replay is needed on next start so
			# the rmgr cleanup callback (orphan dropper) fires.
			node.stop(['-m', 'immediate'])

		data_dir = os.path.join(node.data_dir, "orioledb_data")
		orphan_dir = os.path.join(data_dir, "cic_99999_99999_99999")
		os.makedirs(orphan_dir, exist_ok=True)
		spool_path = os.path.join(orphan_dir, "spool_0.bin")
		with open(spool_path, "wb") as f:
			f.write(b"\x00" * 64)
		self.assertTrue(os.path.isdir(orphan_dir))

		node.start()
		try:
			node.execute("SELECT 1")
			self.assertFalse(os.path.exists(orphan_dir),
			                 "orphan CIC spool dir not cleaned up at startup")
		finally:
			try:
				node.stop()
			except Exception:
				pass

	def test_cic_streaming_replica(self):
		"""
		CIC on master must produce a queryable index on a streaming
		standby.  All the data-changing work goes through standard WAL
		(o_tables_update, o_indices update, build_secondary_index's
		bulk-load, drain's per-row modifies), so the standby converges without any phase-record-specific redo logic.
		"""
		master = self.node
		master.start()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_cic_repl (
						id int NOT NULL,
						val text NOT NULL,
						PRIMARY KEY (id)
					) USING orioledb;
					INSERT INTO o_cic_repl
					SELECT g, 'v' || g FROM generate_series(1, 2000) g;
				""")
				self.catchup_orioledb(replica)

				master.safe_psql(
				    "CREATE INDEX CONCURRENTLY o_cic_repl_val_idx "
				    "ON o_cic_repl (val);")

				self.catchup_orioledb(replica)

				with replica.connect() as c:
					c.execute("SET enable_seqscan = off")
					cnt = c.execute("SELECT count(*) FROM o_cic_repl "
					                "WHERE val >= 'v' AND val < 'w';")[0][0]
				self.assertEqual(cnt, 2000)

				with replica.connect() as c:
					c.execute("SET enable_seqscan = off")
					one = c.execute("SELECT id FROM o_cic_repl "
					                "WHERE val = 'v1234';")[0][0]
				self.assertEqual(one, 1234)
		finally:
			try:
				master.stop()
			except Exception:
				pass

	def test_cic_streaming_replica_failing_expr(self):
		"""
		CIC on the primary may fail at validate-scan when the index
		expression raises on some row (e.g. `a/b` with b=0).  On
		the primary the index is left in OINDEX_STATE_BUILDING_PHASE_2
		and PG marks pg_index.indisvalid=false.  The replica must
		mirror that state without aborting recovery.
		"""
		master = self.node
		master.start()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_cic_bad (a int, b int) USING orioledb;
					INSERT INTO o_cic_bad VALUES (1, 0);
				""")
				self.catchup_orioledb(replica)

				# CIC fails on master; ignore the expected ERROR.
				_, _, err = master.psql(
				    "CREATE INDEX CONCURRENTLY o_cic_bad_idx "
				    "ON o_cic_bad ((a/b));")
				self.assertIn(b"division by zero", err)

				# After more DML, the replica must still catch up
				# (i.e. recovery did not stall on the failed build).
				master.safe_psql("INSERT INTO o_cic_bad VALUES (5, 1);")
				self.catchup_orioledb(replica)

				cnt = replica.execute("SELECT count(*) FROM o_cic_bad;")[0][0]
				self.assertEqual(cnt, 2)
		finally:
			try:
				master.stop()
			except Exception:
				pass
