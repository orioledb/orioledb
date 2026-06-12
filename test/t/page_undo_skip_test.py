#!/usr/bin/env python3
# coding: utf-8
# Tests for the page-level undo elision optimization.

import threading

from .base_test import BaseTest


def page_undo_loc(node, con=None):
	"""lastusedlocation for the page-level undo log."""
	q = "SELECT lastusedlocation FROM orioledb_get_undo_meta() WHERE undo_type='page';"
	if con is not None:
		return con.execute(q)[0][0]
	return node.execute(q)[0][0]


class PageUndoSkipTest(BaseTest):

	# Approximate per-row payload: text repeated 200 chars + tuple overhead.
	# 30k rows is comfortably > one leaf page so splits are guaranteed.
	NROWS = 30000

	def _setup_oriole(self):
		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		return node

	def test_bulk_load_no_concurrent_readers_skips_page_undo(self):
		"""Pre-committed table + bulk insert in a fresh transaction emits
		no page-level undo when no other backends are active."""
		node = self._setup_oriole()

		# Defeat the self-create shortcut: CREATE commits in its own txn.
		node.safe_psql(
		    "CREATE TABLE bulk (i bigint PRIMARY KEY, t text) USING orioledb;")
		before = page_undo_loc(node)
		node.safe_psql(
		    f"INSERT INTO bulk SELECT g, repeat('a', 200) FROM generate_series(1, {self.NROWS}) g;"
		)
		after = page_undo_loc(node)

		# Tolerate a small constant for unrelated activity (system trees,
		# meta updates).  Optimization is working iff growth << one undo
		# image (~8KB).
		self.assertLess(after - before, 16 * 1024)

		node.stop()

	def test_bulk_load_disabled_emits_full_page_undo(self):
		"""With the optimization disabled, the same workload emits the
		expected page-level undo (sanity-check the GUC and the baseline)."""
		node = self._setup_oriole()

		node.safe_psql(
		    "CREATE TABLE bulk_off (i bigint PRIMARY KEY, t text) USING orioledb;"
		)
		before = page_undo_loc(node)
		node.safe_psql(f"""
			SET orioledb.debug_force_page_undo = on;
			INSERT INTO bulk_off SELECT g, repeat('a', 200)
				FROM generate_series(1, {self.NROWS}) g;
		""")
		after = page_undo_loc(node)

		# Each split emits ~8KB; 30k rows split many leaves.  Expect at
		# least 100 KB.
		self.assertGreater(after - before, 100 * 1024)

		node.stop()

	def test_bulk_load_with_concurrent_reader_keeps_page_undo(self):
		"""A concurrent open scan on the same relation must force page
		undo to be emitted, and the scanner must see only the pre-bulk
		row set after each FETCH."""
		node = self._setup_oriole()

		node.safe_psql("""
			CREATE TABLE r1 (i bigint PRIMARY KEY, t text) USING orioledb;
			INSERT INTO r1 VALUES (0, 'pre');
		""")

		# Reader: REPEATABLE READ + open cursor pins the snapshot and
		# bumps numSeqScans for the duration of FETCH iteration.  Omit
		# ORDER BY: with ORDER BY on the PK, the planner picks a Custom
		# Scan / index scan that does NOT bump numSeqScans, and the
		# optimization would not see this reader.
		with node.connect() as reader:
			reader.begin('REPEATABLE READ')
			reader.execute("DECLARE c CURSOR FOR SELECT i, t FROM r1;")
			# Force the cursor to start streaming so a scan is registered.
			first = reader.execute("FETCH FORWARD 1 FROM c;")
			self.assertEqual(first, [(0, 'pre')])

			with node.connect() as writer:
				writer.execute("SET orioledb.debug_force_page_undo = off;")
				before = page_undo_loc(node, writer)
				writer.execute(f"""
					INSERT INTO r1 SELECT g, repeat('a', 200)
						FROM generate_series(1, {self.NROWS}) g;
				""")
				writer.commit()
				after = page_undo_loc(node, writer)

			# Concurrent reader must have prevented elision: expect
			# substantial page undo emission.
			self.assertGreater(after - before, 100 * 1024)

			# Reader still sees only the pre-bulk row.
			remaining = reader.execute("FETCH FORWARD 10 FROM c;")
			self.assertEqual(remaining, [])
			reader.execute("CLOSE c;")
			reader.commit()

		# Outside the reader's transaction the new rows are visible.
		count = node.execute("SELECT count(*) FROM r1;")[0][0]
		self.assertEqual(count, self.NROWS + 1)

		node.stop()

	def test_delete_then_bulk_insert_keeps_page_undo(self):
		"""A delete by the same backend on a leaf forces page undo for
		subsequent split/compact on that leaf, even with no concurrent
		readers."""
		node = self._setup_oriole()

		# Seed enough rows to fill many leaves so deletes spread across them.
		node.safe_psql(f"""
			CREATE TABLE d1 (i bigint PRIMARY KEY, t text) USING orioledb;
			INSERT INTO d1 SELECT g, repeat('a', 200)
				FROM generate_series(1, {self.NROWS // 3}) g;
		""")

		# In a single transaction: delete scattered rows then bulk insert.
		with node.connect() as con:
			con.begin()
			# mod() instead of '%' to avoid psycopg2 placeholder parsing.
			con.execute("DELETE FROM d1 WHERE mod(i, 5) = 0;")
			before = page_undo_loc(node, con)
			con.execute(f"""
				INSERT INTO d1 SELECT g, repeat('a', 200)
					FROM generate_series({self.NROWS // 3 + 1}, {self.NROWS}) g;
			""")
			after = page_undo_loc(node, con)
			con.commit()

		# Pages with local deletes must take page-level undo on subsequent
		# splits/compactions.  Only leaves where both a delete and a later
		# split land emit undo, so we expect at least one page image
		# (vs 0 in the fully optimized path).
		self.assertGreater(after - before, 4 * 1024)

		node.stop()

	def test_abort_after_optimized_bulk_load(self):
		"""When the optimization fired and the transaction aborts, the
		original row set must remain visible."""
		node = self._setup_oriole()

		node.safe_psql("""
			CREATE TABLE a1 (i bigint PRIMARY KEY, t text) USING orioledb;
			INSERT INTO a1 SELECT g, 'orig'
				FROM generate_series(1, 10) g;
		""")

		with node.connect() as con:
			con.begin()
			con.execute(f"""
				INSERT INTO a1 SELECT g, repeat('a', 200)
					FROM generate_series(100, {self.NROWS + 100}) g;
			""")
			con.rollback()

		# Original ten rows must remain.
		rows = node.execute("SELECT count(*), max(t) FROM a1;")
		self.assertEqual(rows[0][0], 10)
		self.assertEqual(rows[0][1], 'orig')

		node.stop()

	# ---- concurrent correctness ---------------------------------------

	NWORKERS = 4

	def _run_concurrent_inserts(self, node, table, ordered):
		"""Spawn NWORKERS connections that each INSERT a disjoint slice of
		[1..NROWS].  ordered=True: each worker's slice is a contiguous,
		monotonic PK range.  ordered=False: each worker's slice is a hash-
		shuffled set of PKs (disjoint via mod NWORKERS == worker id)."""
		barrier = threading.Barrier(self.NWORKERS)
		errors = []

		def worker(wid, lo, hi):
			try:
				with node.connect() as con:
					con.begin()
					if ordered:
						sql = (f"INSERT INTO {table} "
						       f"SELECT g, 'w{wid}' "
						       f"FROM generate_series({lo}, {hi}) g;")
					else:
						sql = (f"INSERT INTO {table} "
						       f"SELECT (hashint8(g) & x'7fffffff'::bigint) "
						       f"  * {self.NWORKERS} + {wid}, 'w{wid}' "
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
		for t in threads:
			t.start()
		for t in threads:
			t.join()
		self.assertFalse(errors, f"worker errors: {errors}")

	def test_concurrent_ordered_inserts_correctness(self):
		"""N workers insert disjoint monotonic PK ranges.  Final table must
		contain every PK exactly once and per-worker row counts must match."""
		node = self._setup_oriole()
		node.safe_psql(
		    "CREATE TABLE co_ord (i bigint PRIMARY KEY, t text) USING orioledb;"
		)
		self._run_concurrent_inserts(node, 'co_ord', ordered=True)

		total = node.execute("SELECT count(*) FROM co_ord;")[0][0]
		# Each worker writes NROWS//NWORKERS rows; total may be slightly
		# below NROWS if NROWS isn't divisible by NWORKERS.
		expected = (self.NROWS // self.NWORKERS) * self.NWORKERS
		self.assertEqual(total, expected)

		# No duplicates and every PK is in the expected interval.
		dup = node.execute(
		    "SELECT i FROM co_ord GROUP BY i HAVING count(*) > 1 LIMIT 1;")
		self.assertEqual(dup, [])
		out_of_range = node.execute(
		    f"SELECT 1 FROM co_ord WHERE i < 1 OR i > {expected} LIMIT 1;")
		self.assertEqual(out_of_range, [])

		# Each worker's slice has the right count.
		per = self.NROWS // self.NWORKERS
		for w in range(self.NWORKERS):
			lo, hi = w * per + 1, (w + 1) * per
			cnt = node.execute(
			    f"SELECT count(*) FROM co_ord WHERE i BETWEEN {lo} AND {hi};"
			)[0][0]
			self.assertEqual(cnt, per, f"worker {w} slice mismatch")

		node.stop()

	def test_concurrent_unordered_inserts_correctness(self):
		"""N workers insert hash-shuffled PKs across the keyspace.  Final
		table must satisfy: row count == sum of per-worker rows, PK
		uniqueness, and each worker's tagged rows survive intact."""
		node = self._setup_oriole()
		node.safe_psql(
		    "CREATE TABLE co_rand (i bigint PRIMARY KEY, t text) USING orioledb;"
		)
		self._run_concurrent_inserts(node, 'co_rand', ordered=False)

		# Sum across workers (worker tag = 't' column) equals row count.
		total = node.execute("SELECT count(*) FROM co_rand;")[0][0]
		per_worker = node.execute(
		    "SELECT t, count(*) FROM co_rand GROUP BY t ORDER BY t;")
		self.assertEqual(sum(c for _, c in per_worker), total)
		self.assertEqual(len(per_worker), self.NWORKERS)

		# PK uniqueness is implicit (PRIMARY KEY) but double-check.
		dup = node.execute(
		    "SELECT i FROM co_rand GROUP BY i HAVING count(*) > 1 LIMIT 1;")
		self.assertEqual(dup, [])

		# Each worker's rows must carry that worker's tag and PK mod
		# NWORKERS must match the worker id.
		for w, cnt in per_worker:
			wid = int(w[1:])
			mismatched = node.execute(
			    f"SELECT 1 FROM co_rand WHERE t = '{w}' "
			    f"AND mod(i, {self.NWORKERS}) <> {wid} LIMIT 1;")
			self.assertEqual(mismatched, [],
			                 f"worker {w} has rows with wrong PK residue")

		node.stop()

	def test_reader_during_uncommitted_bulk_insert_sees_pre_state(self):
		"""A reader that takes its snapshot AFTER an optimized bulk insert
		has run (and triggered splits) but BEFORE the writing transaction
		commits must see only the pre-insert state.  This exercises the
		case where the skip optimization fires (no concurrent seq scan
		during the writer's INSERT) yet MVCC visibility must still hide
		uncommitted rows from a later snapshot."""
		node = self._setup_oriole()
		node.safe_psql("""
			CREATE TABLE u1 (i bigint PRIMARY KEY, t text) USING orioledb;
			INSERT INTO u1 SELECT g, 'pre' FROM generate_series(1, 50) g;
		""")
		pre_rows = node.execute("SELECT i, t FROM u1 ORDER BY i;")
		self.assertEqual(len(pre_rows), 50)

		with node.connect() as writer:
			writer.begin()
			# Bulk insert under the skip optimization (no concurrent scan).
			writer.execute(f"""
				INSERT INTO u1 SELECT g, 'new' FROM
					generate_series(100, {self.NROWS + 100}) g;
			""")
			# Writer NOT yet committed.

			with node.connect() as reader:
				reader.begin('REPEATABLE READ')
				# Pre-insert visibility under MVCC despite splits with no
				# page-level undo.
				cnt = reader.execute("SELECT count(*) FROM u1;")[0][0]
				self.assertEqual(cnt, 50)
				seen = reader.execute("SELECT i, t FROM u1 ORDER BY i;")
				self.assertEqual(seen, pre_rows)
				# 'new' rows must be entirely invisible.
				new_seen = reader.execute(
				    "SELECT count(*) FROM u1 WHERE t = 'new';")[0][0]
				self.assertEqual(new_seen, 0)

				writer.commit()

				# Reader still in its RR snapshot: post-commit, still
				# pre-state.
				cnt = reader.execute("SELECT count(*) FROM u1;")[0][0]
				self.assertEqual(cnt, 50)
				reader.commit()

		# A fresh connection now sees both pre and new.
		total = node.execute("SELECT count(*) FROM u1;")[0][0]
		self.assertEqual(total, 50 + self.NROWS + 1)

		node.stop()
