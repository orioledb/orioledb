#!/usr/bin/env python3
# coding: utf-8

# Parallel bitmap heap scan under concurrency, driven with stop events.
#
# Each scenario parks a parallel bitmap scan in the middle of reading an
# on-disk leaf (the scan_disk_page stop event, hit while the first worker
# builds the key bitmap or while the primary tree is fetched cooperatively) and,
# while it is parked, mutates the matching range from another connection -- page
# splits, merges, updates, checkpoints, re-eviction -- before releasing it.  The
# resumed scan must return exactly its snapshot's matching rows (compared as a
# sorted set, since the parallel bitmap output is unordered), proving the shared
# read-only bitmap and the cooperative primary fetch stay correct while the tree
# changes shape underneath the scan.

import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

# An always-true but per-row-expensive filter makes the parallel bitmap path
# win, so the planner picks Gather over a parallel custom bitmap scan.
FILTER = "sqrt(id::float8) >= 0"
NROWS = 20000


class ParallelBitmapScanTest(BaseTest):

	def setUp(self):
		super().setUp()
		# Stop events are enabled per-connection in the scanner only, so the
		# mutating connection never parks on the same event.
		self.node.append_conf(
		    'postgresql.conf', "max_worker_processes = 16\n"
		    "max_parallel_workers = 16\n"
		    "orioledb.main_buffers = 8MB\n")
		self.node.start()
		self.node.safe_psql('postgres',
		                    "CREATE EXTENSION IF NOT EXISTS orioledb;")

	def _load(self, bridged=False):
		# A bridged secondary index builds a shared DSA TIDBitmap; a native one
		# builds a shared key bitmap.  Both run the same cooperative fetch.
		table_opts = " WITH (index_bridging)" if bridged else ""
		index_sql = ("CREATE INDEX o_bm_val ON o_bm USING btree (val) "
		             "WITH (orioledb_index = off);"
		             if bridged else "CREATE INDEX o_bm_val ON o_bm (val);")
		self.node.safe_psql(
		    'postgres', f"""
			DROP TABLE IF EXISTS o_bm;
			CREATE TABLE o_bm (
				id int8 PRIMARY KEY,
				val int8
			) USING orioledb{table_opts};
			INSERT INTO o_bm SELECT i, i % 1000 FROM generate_series(1, {NROWS}) i;
			{index_sql}
			ANALYZE o_bm;
			CHECKPOINT;
		""")

	def _evict(self):
		self.node.safe_psql(
		    'postgres',
		    "SELECT orioledb_evict_pages('o_bm'::regclass::oid, 0);")

	def _scan_setup(self, con):
		con.execute("SET orioledb.enable_stopevents = true;")
		con.execute("SET enable_seqscan = off;")
		con.execute("SET enable_indexscan = off;")
		con.execute("SET enable_indexonlyscan = off;")
		con.execute("SET parallel_setup_cost = 0;")
		con.execute("SET parallel_tuple_cost = 0;")
		con.execute("SET min_parallel_table_scan_size = 0;")
		con.execute("SET min_parallel_index_scan_size = 0;")
		con.execute("SET max_parallel_workers_per_gather = 3;")

	def _scan_sql(self, cond):
		return f"SELECT id FROM o_bm WHERE ({cond}) AND {FILTER}"

	def _reference(self, cond):
		rows = self.node.execute(
		    'postgres', f"SELECT id FROM o_bm WHERE ({cond}) AND {FILTER}")
		return sorted(r[0] for r in rows)

	def _assert_parallel_plan(self, con, cond):
		plan = "\n".join(r[0] for r in con.execute("EXPLAIN (COSTS OFF) " +
		                                           self._scan_sql(cond)))
		self.assertIn("Gather", plan)
		self.assertIn("Parallel Custom Scan", plan)
		self.assertIn("Bitmap", plan)

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

	# Core driver: park a parallel bitmap scan at scan_disk_page, run
	# mutate(dml_con) while it is parked, release it, and assert the result set
	# equals the pre-mutation reference.
	def _run_parked(self, cond, mutate, bridged=False):
		node = self.node
		self._load(bridged=bridged)
		reference = self._reference(cond)
		self.assertGreater(len(reference), 500)

		scan_con = node.connect()
		ctrl_con = node.connect()
		dml_con = node.connect()
		try:
			self._scan_setup(scan_con)
			self._assert_parallel_plan(scan_con, cond)

			self._evict()
			ctrl_con.execute(
			    "SELECT pg_stopevent_set('scan_disk_page', 'true');")

			scan = ThreadQueryExecutor(scan_con, self._scan_sql(cond))
			scan.start()
			try:
				self._wait_any_parked(ctrl_con, 'scan_disk_page')
				self.assertTrue(scan.is_alive())
				mutate(dml_con)
			finally:
				ctrl_con.execute(
				    "SELECT pg_stopevent_reset('scan_disk_page');")

			result = sorted(r[0] for r in scan.join())
			self.assertEqual(result, reference)
			dml_con.commit()
		finally:
			scan_con.close()
			ctrl_con.close()
			dml_con.close()

		self.assertEqual(
		    node.execute('postgres',
		                 "SELECT orioledb_tbl_check('o_bm'::regclass)")[0][0],
		    True)

	# --- scenarios ----------------------------------------------------------

	# Parked bitmap scan; concurrent inserts split pages, then checkpoint +
	# re-eviction rewrite them.
	def test_bitmap_survives_splits(self):

		def mutate(con):
			con.execute("INSERT INTO o_bm SELECT i, i %% 1000 FROM "
			            "generate_series(1000000, 1030000) i;")
			con.execute("CHECKPOINT;")
			con.execute(
			    "SELECT orioledb_evict_pages('o_bm'::regclass::oid, 0);")

		self._run_parked("val < 50", mutate)

	# Parked bitmap scan; concurrent deletes across the matching range merge
	# pages under it.
	def test_bitmap_survives_merges(self):

		def mutate(con):
			con.execute("DELETE FROM o_bm WHERE id BETWEEN 3000 AND 12000;")
			con.execute("CHECKPOINT;")

		self._run_parked("val < 50", mutate)

	# BitmapOr qual; a mix of insert, delete, update and checkpoint land while
	# the scan is parked.
	def test_bitmap_or_under_mutation(self):

		def mutate(con):
			con.execute("INSERT INTO o_bm SELECT i, i %% 1000 FROM "
			            "generate_series(1000000, 1020000) i;")
			con.execute("DELETE FROM o_bm WHERE id BETWEEN 5000 AND 8000;")
			con.execute("UPDATE o_bm SET val = val + 1 "
			            "WHERE id BETWEEN 9000 AND 11000;")
			con.execute("CHECKPOINT;")

		self._run_parked("val < 30 OR val > 970", mutate)

	# Bridged (non-orioledb) index: the shared DSA TIDBitmap is consumed
	# cooperatively while the primary tree is mutated (page splits from inserts,
	# checkpoint, re-eviction) under the parked scan.  Row visibility resolves
	# from the primary key (the per-page primary scan uses the scan snapshot),
	# while the bridge index is a separate mapping; to keep the compared result
	# set deterministic the mutation inserts only non-matching rows (val = 500),
	# reshaping the tree without changing the val < 50 result.
	def test_bridged_survives_mutation(self):

		def mutate(con):
			con.execute("INSERT INTO o_bm SELECT i, 500 FROM "
			            "generate_series(1000000, 1040000) i;")
			con.execute("CHECKPOINT;")
			con.execute(
			    "SELECT orioledb_evict_pages('o_bm'::regclass::oid, 0);")

		self._run_parked("val < 50", mutate, bridged=True)

	# Two parallel bitmap scans parked at once; cross DML lands while both are
	# parked, then both are released together.
	def test_two_concurrent_bitmaps(self):
		import time
		node = self.node
		self._load()
		ref_a = self._reference("val < 50")
		ref_b = self._reference("val >= 900")

		a_con = node.connect()
		b_con = node.connect()
		ctrl_con = node.connect()
		dml_con = node.connect()
		try:
			self._scan_setup(a_con)
			self._scan_setup(b_con)
			self._evict()
			ctrl_con.execute(
			    "SELECT pg_stopevent_set('scan_disk_page', 'true');")

			a_scan = ThreadQueryExecutor(a_con, self._scan_sql("val < 50"))
			b_scan = ThreadQueryExecutor(b_con, self._scan_sql("val >= 900"))
			a_scan.start()
			b_scan.start()
			try:
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
				self.assertTrue(a_scan.is_alive() and b_scan.is_alive())
				dml_con.execute("INSERT INTO o_bm SELECT i, i %% 1000 FROM "
				                "generate_series(1000000, 1020000) i;")
				dml_con.execute(
				    "DELETE FROM o_bm WHERE id BETWEEN 5000 AND 8000;")
				dml_con.execute("CHECKPOINT;")
			finally:
				ctrl_con.execute(
				    "SELECT pg_stopevent_reset('scan_disk_page');")

			got_a = sorted(r[0] for r in a_scan.join())
			got_b = sorted(r[0] for r in b_scan.join())
			self.assertEqual(got_a, ref_a)
			self.assertEqual(got_b, ref_b)
		finally:
			a_con.close()
			b_con.close()
			ctrl_con.close()
			dml_con.close()


if __name__ == "__main__":
	unittest.main()
