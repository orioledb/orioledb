#!/usr/bin/env python3
# coding: utf-8
"""
Bench: cost of reading an old snapshot after many updates, heap vs OrioleDB.
Prints EXP_S, EXP_S_CTX, EXP_S_EXPLAIN, EXP_D and EXP_D_SAMPLE lines to stdout.
Run with the number of updates and the engine in the environment:

    EXP_N=100000 EXP_ENGINE=orioledb|heap python3 -m unittest \\
        test.t.exp_old_snapshot_read_test

The only assertions are that every reader still sees the rows of its own
snapshot and that every plan is the expected single-process scan.
"""

import os
import statistics
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from .base_test import BaseTest, tbl_structure_to_json

# Top-level data directory entries that get their own EXP_D line.
DATA_DIRS = ('base', 'pg_wal', 'orioledb_data', 'orioledb_undo')
# st_blocks counts blocks of this size, whatever the file system block is.
ST_BLOCK_SIZE = 512


def dir_sizes(data_dir):
	"""{category: [apparent, allocated]} bytes, with 'other' and 'total'."""
	sizes = {name: [0, 0] for name in DATA_DIRS + ('other', )}
	for root, _, files in os.walk(data_dir):
		top = os.path.relpath(root, data_dir).split(os.sep)[0]
		key = top if top in DATA_DIRS else 'other'
		for name in files:
			try:
				st = os.lstat(os.path.join(root, name))
			except FileNotFoundError:
				# The server removes temporary files while we walk.
				continue
			sizes[key][0] += st.st_size
			sizes[key][1] += st.st_blocks * ST_BLOCK_SIZE
	sizes['total'] = [sum(v[i] for v in sizes.values()) for i in (0, 1)]
	return sizes


# Experiment, not a regression test: runs only when asked for explicitly.
@unittest.skipUnless('EXP_N' in os.environ and 'EXP_ENGINE' in os.environ,
                     "set EXP_N and EXP_ENGINE to run the bench")
class ExpOldSnapshotReadTest(BaseTest):
	ROWS = 1000
	PAD = 100
	SIZE_BASE = 20
	SIZE_STRIDE = 37
	SIZE_RANGE = 130
	# Runs of each query: the first one is the cold run, the rest are warm.
	# 20 warm runs x 5 benchmark runs give 100 samples, enough for p1 and p99.
	REPEATS = 21
	# 5 readers x 20 warm repeats give 100 samples per cell from one run.
	READERS_PER_PROBE = 5
	# Bounds the point-query time at high ROWS.
	POINT_SAMPLE = 50
	# Bounds sampling cost; the peak is accurate to this many updates.
	DISK_SAMPLE_EVERY = 1000
	# The bgwriter runs undo cleanup every bgwriter_delay (200 ms default).
	CLEANUP_WAIT_S = 1
	MS = 1000.0

	def setUp(self):
		super().setUp()
		# Same single-process plans for both engines.
		self.node.append_conf('postgresql.conf',
		                      "max_parallel_workers_per_gather = 0\n")
		self.node.start()
		self.node.safe_psql('postgres',
		                    "CREATE EXTENSION IF NOT EXISTS orioledb;")

	def _reader(self, table):
		con = self.node.connect()
		con.begin()
		con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		view = con.execute("SELECT id, payload FROM %s ORDER BY id;" % table)
		self.assertEqual(len(view), self.ROWS)
		return con, view

	def _update(self, writer, table, i):
		writer.execute(
		    "UPDATE %s SET payload = repeat('b', %d + mod(%d * %d, %d)) "
		    "WHERE id = %d;" % (table, self.SIZE_BASE, i, self.SIZE_STRIDE,
		                        self.SIZE_RANGE, 1 + i % self.ROWS))

	def _undo_used(self, con, undo_type):
		return con.execute(
		    "SELECT lastUsedLocation FROM orioledb_get_undo_meta() "
		    "WHERE undo_type = '%s';" % undo_type)[0][0]

	def _undo_dir_bytes(self):
		total = 0
		for root, _, files in os.walk(
		    os.path.join(self.node.data_dir, 'orioledb_undo')):
			for name in files:
				total += os.lstat(os.path.join(root, name)).st_size
		return total

	def _timed(self, con, sql):
		start = time.perf_counter()
		res = con.execute(sql)
		return (time.perf_counter() - start) * self.MS, res

	def _runs(self, n, engine, reader, inst, query, times):
		cold, warm = times[0], times[1:]
		print(
		    "EXP_S N=%d engine=%s rows=%d reader=%s inst=%d query=%s cold_ms=%.3f "
		    "warm_median_ms=%.3f warm_min_ms=%.3f warm_max_ms=%.3f" %
		    (n, engine, self.ROWS, reader, inst, query, cold,
		     statistics.median(warm), min(warm), max(warm)),
		    flush=True)

	def _requests(self, n, engine, reader, inst, query, warm):
		print("EXP_R N=%d engine=%s rows=%d reader=%s inst=%d query=%s ms=%s" %
		      (n, engine, self.ROWS, reader, inst, query, ','.join(
		          "%.3f" % t for t in warm)),
		      flush=True)

	def _check_plan(self, engine, table, query, plan):
		text = ' | '.join(r[0] for r in plan)
		for node in ('Gather', 'Parallel'):
			self.assertNotIn(node, text)
		if query == 'seq':
			self.assertIn("Seq Scan on %s" % table, text)
		elif engine == 'heap':
			self.assertIn("Index Scan using %s_pkey" % table, text)
		else:
			self.assertIn("Forward index scan of: %s_pkey" % table, text)

	def _readers(self, table):
		"""Readers opened one after another, so they share one snapshot point."""
		return [self._reader(table) for _ in range(self.READERS_PER_PROBE)]

	def _read_bench(self, n, engine, table, readers):
		for name, instances in readers.items():
			barrier = threading.Barrier(len(instances))
			with ThreadPoolExecutor(len(instances)) as pool:
				futures = [
				    pool.submit(self._read_instance, n, engine, table, name,
				                inst, con, view, barrier)
				    for inst, (con, view) in enumerate(instances)
				]
			errors = [f.exception() for f in futures]
			real = [
			    e for e in errors
			    if e and not isinstance(e, threading.BrokenBarrierError)
			]
			if real:
				raise real[0]

	def _read_instance(self, n, engine, table, name, inst, con, view, barrier):
		try:
			self._read_queries(n, engine, table, name, inst, con, view,
			                   barrier)
		except BaseException:
			barrier.abort()
			raise

	def _read_queries(self, n, engine, table, name, inst, con, view, barrier):
		seq_sql = ("SELECT count(*), sum(length(payload)) FROM %s;" % table)
		idx_sql = ("SELECT count(*), sum(length(payload)) FROM "
		           "(SELECT payload FROM %s ORDER BY id) s;" % table)
		plans = (
		    ('seq', seq_sql, "SET enable_indexscan = off; "
		     "SET enable_bitmapscan = off; SET enable_seqscan = on;"),
		    ('index', idx_sql, "SET enable_seqscan = off; "
		     "SET enable_bitmapscan = off; SET enable_indexscan = on;"),
		)
		for query, sql, prep in plans:
			for stmt in prep.split(';'):
				if stmt.strip():
					con.execute(stmt + ';')
			barrier.wait()
			times = [self._timed(con, sql)[0] for _ in range(self.REPEATS)]
			self._runs(n, engine, name, inst, query, times)
			self._requests(n, engine, name, inst, query, times[1:])
			plan = con.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
			self._check_plan(engine, table, query, plan)
			if inst == 0:
				print("EXP_S_EXPLAIN N=%d engine=%s rows=%d reader=%s inst=%d "
				      "query=%s %s" % (n, engine, self.ROWS, name, inst, query,
				                       ' | '.join(r[0] for r in plan)),
				      flush=True)
		con.execute("SET enable_indexscan = on;")
		point_times = []
		lookups = []
		sample = min(self.POINT_SAMPLE, self.ROWS)
		ids = [1 + k * self.ROWS // sample for k in range(sample)]
		barrier.wait()
		for _ in range(self.REPEATS):
			one = []
			for k in ids:
				one.append(
				    self._timed(
				        con, "SELECT payload FROM %s WHERE id = %d;" %
				        (table, k))[0])
			lookups.append(one)
			point_times.append(statistics.median(one))
		self._runs(n, engine, name, inst, 'point', point_times)
		self._requests(n, engine, name, inst, 'point',
		               [t for one in lookups[1:] for t in one])
		plan = con.execute(
		    "EXPLAIN (ANALYZE, BUFFERS) SELECT payload FROM %s WHERE id = 1;" %
		    table)
		self._check_plan(engine, table, 'point', plan)
		if inst == 0:
			print(
			    "EXP_S_EXPLAIN N=%d engine=%s rows=%d reader=%s inst=%d "
			    "query=point %s" %
			    (n, engine, self.ROWS, name, inst, ' | '.join(r[0]
			                                                  for r in plan)),
			    flush=True)
		self.assertEqual(
		    con.execute("SELECT id, payload FROM %s ORDER BY id;" % table),
		    view)

	def _heap_ctx(self, writer, n, table, write_s):
		# Pending stats are flushed at the end of a statement, so force first.
		writer.execute("SELECT pg_stat_force_next_flush();")
		dead = writer.execute(
		    "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = '%s';"
		    % table)[0][0]
		rel, total, idx = writer.execute(
		    "SELECT pg_relation_size('%s'), pg_total_relation_size('%s'), "
		    "pg_relation_size('%s_pkey');" % (table, table, table))[0]
		print(
		    "EXP_S_CTX N=%d engine=heap rows=%d write_s=%.1f relation_size=%d "
		    "total_relation_size=%d n_dead_tup=%d index_size=%d" %
		    (n, self.ROWS, write_s, rel, total, dead, idx))

	def _bench(self, n, engine='orioledb'):
		table = ('o_' if engine == 'orioledb' else 'h_') + 'exp_oldsnap'
		self.node.safe_psql(
		    'postgres', "CREATE TABLE %s (id int PRIMARY KEY, payload text) "
		    "USING %s;"
		    "INSERT INTO %s (SELECT id, repeat('a', %d) "
		    "FROM generate_series(1, %d) id);" %
		    (table, engine, table, self.PAD, self.ROWS))
		peak = {}

		def disk(label, full=True):
			sizes = dir_sizes(self.node.data_dir)
			for key, (apparent, allocated) in sizes.items():
				old = peak.get(key, (0, 0))
				peak[key] = (max(old[0], apparent), max(old[1], allocated))
				if full:
					self._disk_line(n, engine, label, key, apparent, allocated)
			return sizes

		disk('before_updates')
		readers = {'R0': self._readers(table)}
		writer = self.node.connect(autocommit=True)
		write_s = 0.0
		for i in range(1, n + 1):
			t = time.perf_counter()
			self._update(writer, table, i)
			write_s += time.perf_counter() - t
			if i == n // 2:
				readers['R50'] = self._readers(table)
			if i % self.DISK_SAMPLE_EVERY == 0:
				sizes = disk('sample', full=False)
				print("EXP_D_SAMPLE N=%d engine=%s rows=%d i=%d %s" %
				      (n, engine, self.ROWS, i, ' '.join(
				          "%s=%d" % (k, v[1]) for k, v in sizes.items())))
		disk('after_updates')
		readers['RNEW'] = self._readers(table)
		if engine == 'heap':
			self._heap_ctx(writer, n, table, write_s)
		else:
			self._orioledb_ctx(writer, n, table, write_s)
		self._read_bench(n, engine, table, readers)
		writer.execute("CHECKPOINT;")
		disk('after_checkpoint')
		readers['RCKPT'] = self._readers(table)
		self._read_bench(n, engine, table, {'RCKPT': readers['RCKPT']})
		for key, (apparent, allocated) in peak.items():
			self._disk_line(n, engine, 'peak', key, apparent, allocated)
		for con, _ in sum(readers.values(), []):
			con.rollback()
			con.close()
		disk('after_close')
		time.sleep(self.CLEANUP_WAIT_S)
		disk('after_close_wait')
		writer.execute("CHECKPOINT;")
		disk('after_close_checkpoint')
		clean = self._readers(table)
		self._read_bench(n, engine, table, {'RCLEAN': clean})
		for con, _ in clean:
			con.rollback()
			con.close()
		writer.close()

	def _disk_line(self, n, engine, label, key, apparent, allocated):
		print(
		    "EXP_D %s N=%d engine=%s rows=%d dir=%s apparent=%d allocated=%d" %
		    (label, n, engine, self.ROWS, key, apparent, allocated))

	def _orioledb_ctx(self, writer, n, table, write_s):
		structure = tbl_structure_to_json(
		    writer.execute(
		        "SELECT orioledb_tbl_structure('%s'::regclass, 'nue', 4);" %
		        table)[0][0])
		print(
		    "EXP_S_CTX N=%d engine=orioledb rows=%d write_s=%.1f page_undo=%d row_undo=%d "
		    "undo_dir_bytes=%d undo_buffers=%s leaves=%d" %
		    (n, self.ROWS, write_s, self._undo_used(writer, 'page'),
		     self._undo_used(writer, 'row'), self._undo_dir_bytes(),
		     writer.execute("SHOW orioledb.undo_buffers;")[0][0],
		     len(structure[table + '_pkey'].get(0, {}))))

	def test_bench(self):
		self._bench(int(os.environ['EXP_N']), os.environ['EXP_ENGINE'])


if __name__ == '__main__':
	unittest.main()
