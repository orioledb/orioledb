#!/usr/bin/env python3
# coding: utf-8

# Parallel ordered (key-order) index scan under concurrency, driven with stop
# events for precise timing.
#
# Each scenario parks a parallel ordered scan in the middle of the tree walk
# (reading an on-disk leaf, or descending during the backward re-descent) and,
# while it is parked, mutates the exact range being scanned from another
# connection -- page splits, merges, updates, checkpoints, re-eviction -- before
# releasing it.  The resumed scan must return its snapshot's rows in the exact
# key order, compared element-for-element against a reference vector captured
# before the mutation.  This exercises the backward re-descent by low key, the
# cooperative double-buffered internal-page walk and undo-based snapshot reads
# while the physical tree changes shape underneath the scan.

import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

# An always-true but per-row-expensive filter makes the parallel ordered path
# cheaper than the naturally-ordered serial index scan, so the planner picks
# Gather Merge deterministically.
FILTER = "sqrt(val::float8) >= 0"
LO, HI = 2000, 8000
NROWS = 15000


class ParallelOrderedScanTest(BaseTest):

	def setUp(self):
		super().setUp()
		# NB: stop events are enabled per-connection in the scanner only, so
		# that the mutating connection never parks on the same event.
		self.node.append_conf(
		    'postgresql.conf', "max_worker_processes = 16\n"
		    "max_parallel_workers = 16\n"
		    "orioledb.main_buffers = 8MB\n")
		self.node.start()
		self.node.safe_psql('postgres',
		                    "CREATE EXTENSION IF NOT EXISTS orioledb;")

	def _load(self):
		self.node.safe_psql(
		    'postgres', f"""
			DROP TABLE IF EXISTS o_ord;
			CREATE TABLE o_ord (
				id int8 NOT NULL,
				val int8 NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
			INSERT INTO o_ord SELECT i, i % 97 FROM generate_series(1, {NROWS}) i;
			ANALYZE o_ord;
			CHECKPOINT;
		""")

	def _evict(self):
		# Push all leaves to disk so the scan reads them (and hits
		# scan_disk_page).  Must run right before the scan: reading the table
		# for any reason (ANALYZE, the reference query) reloads pages into the
		# in-memory pool.
		self.node.safe_psql(
		    'postgres',
		    "SELECT orioledb_evict_pages('o_ord'::regclass::oid, 0);")

	def _scan_setup(self, con):
		con.execute("SET orioledb.enable_stopevents = true;")
		con.execute("SET enable_seqscan = off;")
		con.execute("SET enable_bitmapscan = off;")
		con.execute("SET parallel_setup_cost = 0;")
		con.execute("SET parallel_tuple_cost = 0;")
		con.execute("SET min_parallel_table_scan_size = 0;")
		con.execute("SET min_parallel_index_scan_size = 0;")
		con.execute("SET max_parallel_workers_per_gather = 3;")

	def _scan_sql(self, desc):
		return (f"SELECT id FROM o_ord "
		        f"WHERE id BETWEEN {LO} AND {HI} AND {FILTER} "
		        f"ORDER BY id %s" % ("DESC" if desc else "ASC"))

	def _reference(self, desc):
		# Ordered id vector of the pre-mutation committed state.
		rows = self.node.execute(
		    'postgres', f"SELECT id FROM o_ord WHERE id BETWEEN {LO} AND {HI} "
		    f"AND {FILTER} ORDER BY id %s" % ("DESC" if desc else "ASC"))
		return [r[0] for r in rows]

	def _assert_parallel_plan(self, con, desc):
		plan = "\n".join(r[0] for r in con.execute("EXPLAIN (COSTS OFF) " +
		                                           self._scan_sql(desc)))
		self.assertIn("Gather Merge", plan)
		self.assertIn("Parallel Custom Scan", plan)

	def _wait_any_parked(self, ctrl_con, event, timeout=60):
		import time
		deadline = time.time() + timeout
		while True:
			r = ctrl_con.execute(
			    "SELECT coalesce(array_length(waiter_pids, 1), 0) "
			    "FROM pg_stopevents() WHERE stopevent = '%s';" % event)
			if r and r[0][0] and r[0][0] > 0:
				return
			if time.time() > deadline:
				raise AssertionError("no backend parked on %s" % event)
			time.sleep(0.05)

	# Core driver: park an ordered scan at `event`, run `mutate(dml_con)` while
	# it is parked, release it, and assert the result equals the pre-mutation
	# ordered reference vector.
	def _run_parked(self, desc, event, condition, mutate):
		node = self.node
		self._load()
		reference = self._reference(desc)
		self.assertGreater(len(reference), 3000)

		scan_con = node.connect()
		ctrl_con = node.connect()
		dml_con = node.connect()
		try:
			self._scan_setup(scan_con)
			self._assert_parallel_plan(scan_con, desc)

			# Re-evict AFTER the reference query / plan check reloaded pages,
			# so the parked scan actually reads from disk.
			self._evict()
			ctrl_con.execute("SELECT pg_stopevent_set('%s', '%s');" %
			                 (event, condition))

			scan = ThreadQueryExecutor(scan_con, self._scan_sql(desc))
			scan.start()
			try:
				self._wait_any_parked(ctrl_con, event)
				# The scan must still be running (genuinely parked), otherwise
				# the mutation would not be concurrent with it.
				self.assertTrue(scan.is_alive())
				mutate(dml_con)
			finally:
				ctrl_con.execute("SELECT pg_stopevent_reset('%s');" % event)

			rows = scan.join()
			result = [r[0] for r in rows]
			self.assertEqual(result, reference)
			# Persist the mutations so the post-run integrity check sees the
			# mutated tree (testgres connections are not autocommit).
			dml_con.commit()
		finally:
			scan_con.close()
			ctrl_con.close()
			dml_con.close()

		# Independent structural check, on a fresh connection after the working
		# transactions have released their locks.
		self.assertEqual(
		    node.execute('postgres',
		                 "SELECT orioledb_tbl_check('o_ord'::regclass)")[0][0],
		    True)

	# --- scenarios ----------------------------------------------------------

	# Backward scan parked on the first on-disk leaf; concurrent inserts into the
	# scanned range split pages, then a checkpoint + re-eviction rewrites them.
	def test_backward_survives_splits(self):

		def mutate(con):
			con.execute(
			    "INSERT INTO o_ord SELECT i, i %% 97 FROM "
			    "generate_series(1000000, 1020000) i WHERE i %% 2 = 0;")
			con.execute("CHECKPOINT;")
			con.execute(
			    "SELECT orioledb_evict_pages('o_ord'::regclass::oid, 0);")

		self._run_parked(True, 'scan_disk_page', 'true', mutate)

	# Backward scan parked; concurrent deletes across the scanned range merge
	# pages under it.
	def test_backward_survives_merges(self):

		def mutate(con):
			con.execute("DELETE FROM o_ord WHERE id BETWEEN 3000 AND 6000;")
			con.execute("CHECKPOINT;")

		self._run_parked(True, 'scan_disk_page', 'true', mutate)

	# Forward scan parked; a mix of insert (splits), delete (merges), update and
	# checkpoint all land while it is parked.
	def test_forward_survives_mixed(self):

		def mutate(con):
			con.execute("INSERT INTO o_ord SELECT i, i %% 97 FROM "
			            "generate_series(1000000, 1020000) i;")
			con.execute("DELETE FROM o_ord WHERE id BETWEEN 3000 AND 4000;")
			con.execute("UPDATE o_ord SET val = val + 1 "
			            "WHERE id BETWEEN 5000 AND 6000;")
			con.execute("CHECKPOINT;")
			con.execute(
			    "SELECT orioledb_evict_pages('o_ord'::regclass::oid, 0);")

		self._run_parked(False, 'scan_disk_page', 'true', mutate)

	# Two parallel ordered scans (ascending and descending) parked at once;
	# cross DML lands while both are parked, then both are released together.
	def test_two_concurrent_scans(self):
		node = self.node
		self._load()
		ref_desc = self._reference(True)
		ref_asc = self._reference(False)

		desc_con = node.connect()
		asc_con = node.connect()
		ctrl_con = node.connect()
		dml_con = node.connect()
		try:
			self._scan_setup(desc_con)
			self._scan_setup(asc_con)
			self._evict()
			ctrl_con.execute(
			    "SELECT pg_stopevent_set('scan_disk_page', 'true');")

			desc_scan = ThreadQueryExecutor(desc_con, self._scan_sql(True))
			asc_scan = ThreadQueryExecutor(asc_con, self._scan_sql(False))
			desc_scan.start()
			asc_scan.start()
			try:
				# Wait until both scans have parked (>= 2 distinct waiters).
				import time
				deadline = time.time() + 60
				while True:
					n = ctrl_con.execute(
					    "SELECT coalesce(array_length(waiter_pids, 1), 0) "
					    "FROM pg_stopevents() "
					    "WHERE stopevent = 'scan_disk_page';")[0][0]
					if n and n >= 2:
						break
					self.assertLess(time.time(), deadline,
					                "scans did not both park")
					time.sleep(0.05)
				self.assertTrue(desc_scan.is_alive() and asc_scan.is_alive())
				dml_con.execute("INSERT INTO o_ord SELECT i, i %% 97 FROM "
				                "generate_series(1000000, 1020000) i;")
				dml_con.execute(
				    "DELETE FROM o_ord WHERE id BETWEEN 3000 AND 6000;")
				dml_con.execute("CHECKPOINT;")
			finally:
				ctrl_con.execute(
				    "SELECT pg_stopevent_reset('scan_disk_page');")

			got_desc = [r[0] for r in desc_scan.join()]
			got_asc = [r[0] for r in asc_scan.join()]
			self.assertEqual(got_desc, ref_desc)
			self.assertEqual(got_asc, ref_asc)
		finally:
			desc_con.close()
			asc_con.close()
			ctrl_con.close()
			dml_con.close()


if __name__ == "__main__":
	unittest.main()
