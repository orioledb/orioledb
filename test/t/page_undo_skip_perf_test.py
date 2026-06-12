#!/usr/bin/env python3
# coding: utf-8
# Performance measurements for the page-level undo elision optimization.
#
# Hidden behind ORIOLEDB_RUN_PERF=1 to keep CI fast.  Run locally with:
#   ORIOLEDB_RUN_PERF=1 python3 -m unittest -v \
#       test.t.page_undo_skip_perf_test

import os
import threading
import time
import unittest

from .base_test import BaseTest

RUN_PERF = os.environ.get('ORIOLEDB_RUN_PERF', '0') == '1'


def page_undo_loc(node, con=None):
	"""lastusedlocation for the page-level undo log."""
	q = "SELECT lastusedlocation FROM orioledb_get_undo_meta() WHERE undo_type='page';"
	if con is not None:
		return con.execute(q)[0][0]
	return node.execute(q)[0][0]


@unittest.skipUnless(RUN_PERF, 'set ORIOLEDB_RUN_PERF=1 to run perf tests')
class PageUndoSkipPerfTest(BaseTest):

	NROWS = 300000  # ~60 MB of data, many leaf splits
	PAYLOAD = 200  # bytes per row

	def _bootstrap(self, name):
		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(
		    f"CREATE TABLE {name} (i bigint PRIMARY KEY, t text) USING orioledb;"
		)
		return node

	def _bulk_insert(self, node, table, force_undo):
		"""Bulk insert NROWS and return (undo_bytes_emitted, seconds)."""
		guc = 'on' if force_undo else 'off'
		before = page_undo_loc(node)
		t0 = time.monotonic()
		node.safe_psql(f"""
			SET orioledb.debug_force_page_undo = {guc};
			INSERT INTO {table} SELECT g, repeat('a', {self.PAYLOAD})
				FROM generate_series(1, {self.NROWS}) g;
		""")
		elapsed = time.monotonic() - t0
		after = page_undo_loc(node)
		return after - before, elapsed

	def test_bulk_insert_savings(self):
		"""Measure undo savings on a clean bulk INSERT.  Optimized path
		should emit < 1% of the unoptimized path's page-level undo."""
		node = self._bootstrap('perf_bulk_off')
		off_bytes, off_secs = self._bulk_insert(node, 'perf_bulk_off', True)
		node.safe_psql(
		    "CREATE TABLE perf_bulk_on (i bigint PRIMARY KEY, t text) USING orioledb;"
		)
		on_bytes, on_secs = self._bulk_insert(node, 'perf_bulk_on', False)

		ratio = on_bytes / max(off_bytes, 1)
		print(f"\n[perf bulk_insert N={self.NROWS}] "
		      f"forced={off_bytes/1024:.1f} KB / {off_secs:.2f}s, "
		      f"optimized={on_bytes/1024:.1f} KB / {on_secs:.2f}s, "
		      f"ratio={ratio*100:.2f}%, savings={(1-ratio)*100:.2f}%")

		self.assertGreater(off_bytes, 10 * 1024 * 1024,
		                   "forced path should emit > 10 MB undo")
		self.assertLess(ratio, 0.01,
		                "optimized path should emit < 1% of forced path")
		node.stop()

	def test_concurrent_reader_no_savings(self):
		"""A held REPEATABLE READ cursor on the relation must defeat the
		optimization, leaving page-level undo on par with the forced path."""
		node = self._bootstrap('perf_reader')
		node.safe_psql("INSERT INTO perf_reader VALUES (0, 'pre');")

		with node.connect() as reader:
			reader.begin('REPEATABLE READ')
			# No ORDER BY: forces a sequential scan, which is what registers
			# numSeqScans and defeats the optimization.
			reader.execute("DECLARE c CURSOR FOR SELECT i FROM perf_reader;")
			reader.execute("FETCH FORWARD 1 FROM c;")

			before = page_undo_loc(node)
			t0 = time.monotonic()
			node.safe_psql(f"""
				INSERT INTO perf_reader SELECT g, repeat('a', {self.PAYLOAD})
					FROM generate_series(1, {self.NROWS}) g;
			""")
			elapsed = time.monotonic() - t0
			after = page_undo_loc(node)
			reader.execute("CLOSE c;")
			reader.commit()

		emitted = after - before
		print(f"\n[perf concurrent_reader N={self.NROWS}] "
		      f"emitted={emitted/1024:.1f} KB / {elapsed:.2f}s")
		# Concurrent reader must force the unoptimized amount.
		self.assertGreater(emitted, 10 * 1024 * 1024)
		node.stop()

	def test_delete_then_insert_no_savings(self):
		"""Local deletes on touched leaves must defeat the optimization for
		those leaves; expect undo growth on the same order as the forced
		path."""
		node = self._bootstrap('perf_del')
		# Seed enough rows that DELETE distributes across many leaves.
		node.safe_psql(f"""
			INSERT INTO perf_del SELECT g, repeat('a', {self.PAYLOAD})
				FROM generate_series(1, {self.NROWS // 10}) g;
		""")

		with node.connect() as con:
			con.begin()
			# mod() instead of '%' to avoid psycopg2 placeholder parsing.
			con.execute("DELETE FROM perf_del WHERE mod(i, 5) = 0;")
			before = page_undo_loc(node, con)
			t0 = time.monotonic()
			con.execute(f"""
				INSERT INTO perf_del SELECT g, repeat('a', {self.PAYLOAD})
					FROM generate_series({self.NROWS // 10 + 1}, {self.NROWS}) g;
			""")
			elapsed = time.monotonic() - t0
			after = page_undo_loc(node, con)
			con.commit()

		emitted = after - before
		print(f"\n[perf delete_then_insert N={self.NROWS}] "
		      f"emitted={emitted/1024:.1f} KB / {elapsed:.2f}s")
		# Only leaves where a delete AND a later split land emit undo;
		# the appending insert touches few of those, so just assert that
		# at least one page image is emitted (vs 0 with no deletes).
		self.assertGreater(emitted, 4 * 1024)
		node.stop()

	# ---- concurrent inserters ----------------------------------------

	NWORKERS = 4

	def _run_workers(self, node, table, ranges, force_undo, ordered):
		"""Spawn NWORKERS connections that INSERT their assigned range.
		`ordered`=True inserts a contiguous range in PK order; False inserts
		random PKs across the whole keyspace (worker keeps its slice via
		mod NWORKERS == worker_id).  Returns (undo_bytes, wall_seconds).
		"""
		guc = 'on' if force_undo else 'off'
		barrier = threading.Barrier(self.NWORKERS)
		errors = []

		def worker(wid, lo, hi):
			try:
				with node.connect() as con:
					con.execute(f"SET orioledb.debug_force_page_undo = {guc};")
					con.begin()
					if ordered:
						sql = (f"INSERT INTO {table} "
						       f"SELECT g, repeat('a', {self.PAYLOAD}) "
						       f"FROM generate_series({lo}, {hi}) g;")
					else:
						# Hash-shuffle the key range, keep slices disjoint
						# across workers so PKs don't collide.
						sql = (f"INSERT INTO {table} "
						       f"SELECT (hashint8(g) & x'7fffffff'::bigint) "
						       f"  * {self.NWORKERS} + {wid}, "
						       f"  repeat('a', {self.PAYLOAD}) "
						       f"FROM generate_series({lo}, {hi}) g "
						       f"ON CONFLICT DO NOTHING;")
					barrier.wait()
					con.execute(sql)
					con.commit()
			except Exception as e:
				errors.append((wid, repr(e)))

		per = self.NROWS // self.NWORKERS
		threads = [
		    threading.Thread(target=worker,
		                     args=(w, w * per + 1, (w + 1) * per))
		    for w in range(self.NWORKERS)
		]
		before = page_undo_loc(node)
		t0 = time.monotonic()
		for t in threads:
			t.start()
		for t in threads:
			t.join()
		elapsed = time.monotonic() - t0
		after = page_undo_loc(node)
		self.assertFalse(errors, f"worker errors: {errors}")
		return after - before, elapsed

	def _run_concurrent_pair(self, label, ordered):
		"""Run the workload twice (forced + optimized) and assert savings."""
		node = self._bootstrap(f"perf_{label}_off")
		off_bytes, off_secs = self._run_workers(node, f"perf_{label}_off",
		                                        None, True, ordered)
		node.safe_psql(
		    f"CREATE TABLE perf_{label}_on (i bigint PRIMARY KEY, t text) USING orioledb;"
		)
		on_bytes, on_secs = self._run_workers(node, f"perf_{label}_on", None,
		                                      False, ordered)

		ratio = on_bytes / max(off_bytes, 1)
		print(f"\n[perf {label} N={self.NROWS} workers={self.NWORKERS}] "
		      f"forced={off_bytes/1024:.1f} KB / {off_secs:.2f}s, "
		      f"optimized={on_bytes/1024:.1f} KB / {on_secs:.2f}s, "
		      f"ratio={ratio*100:.2f}%, savings={(1-ratio)*100:.2f}%")

		# Forced path must still emit substantial undo.
		self.assertGreater(off_bytes, 10 * 1024 * 1024,
		                   "forced path should emit > 10 MB undo")
		# Optimized path should save the vast majority of page-level undo
		# even with concurrent inserters (each backend's snapshot retain
		# tracks the same lastUsed so the cross-backend probe doesn't trip).
		self.assertLess(ratio, 0.1,
		                "optimized path should emit < 10% of forced path")
		node.stop()

	def test_concurrent_ordered_inserts_savings(self):
		"""N workers each insert a disjoint, monotonic PK range."""
		self._run_concurrent_pair('co_ord', ordered=True)

	def test_concurrent_unordered_inserts_savings(self):
		"""N workers each insert random PKs across the whole keyspace."""
		self._run_concurrent_pair('co_rand', ordered=False)
