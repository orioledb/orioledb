#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent


class SeqScanCacheTest(BaseTest):
	"""
	A sequential scan loads the on-disk leaves of a tree that takes at most a
	quarter of the page pool into the pool, so that the next scan reads them
	from memory; a larger tree is left on disk.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "max_parallel_workers_per_gather = 0\n")

	def make_table(self, name, rows):
		self.node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE {name} (id int PRIMARY KEY, v text NOT NULL)
				USING orioledb;
			INSERT INTO {name}
				SELECT i, repeat(md5(i::text), 2)
				FROM generate_series(1, {rows}) i;
			CHECKPOINT;
		""")

	def scan_reads_pages(self, con, name, rows):
		"""Scans the table and tells whether that read any page from disk."""
		con.execute("SELECT reset_read_page_checkpoint_stats();")
		self.assertEqual(
		    con.execute(f"SELECT count(*), sum(id::bigint) FROM {name};")[0],
		    (rows, rows * (rows + 1) // 2))
		_, max_chkp = con.execute(
		    "SELECT * FROM fetch_read_page_checkpoint_stats();")[0]
		return max_chkp != 0

	def test_small_table_cached(self):
		node = self.node
		node.start()
		self.make_table('o_small', 5000)
		node.restart()
		with node.connect() as con:
			self.assertTrue(self.scan_reads_pages(con, 'o_small', 5000))
			self.assertFalse(self.scan_reads_pages(con, 'o_small', 5000))
		node.stop()

	def test_large_table_not_cached(self):
		node = self.node
		node.start()
		self.make_table('o_large', 60000)
		node.restart()
		with node.connect() as con:
			self.assertTrue(self.scan_reads_pages(con, 'o_large', 60000))
			self.assertTrue(self.scan_reads_pages(con, 'o_large', 60000))
		node.stop()

	def test_leaf_split_before_load(self):
		"""
		The scan reads the downlinks of an internal page first and loads the
		leaves later.  Leaves split in between must still give every row of
		the scan's snapshot exactly once.
		"""
		node = self.node
		node.start()
		self.make_table('o_split', 5000)
		node.restart()

		con = node.connect()
		con.execute("SET orioledb.enable_stopevents = true;")
		pid = con.pid
		with node.connect() as ctl:
			ctl.execute("SELECT pg_stopevent_set('step_down', 'true');")
			ctl.commit()
			scanner = ThreadQueryExecutor(
			    con, "SELECT count(*), sum(id::bigint), count(DISTINCT id) "
			    "FROM o_split;")
			scanner.start()
			wait_stopevent(node, pid)
			ctl.execute("SELECT pg_stopevent_reset('step_down');")
			# split leaves all over the key range while the scan waits
			ctl.execute("INSERT INTO o_split "
			            "SELECT i + 100000, repeat('y', 200) "
			            "FROM generate_series(1, 5000) i;")
			ctl.execute("UPDATE o_split SET v = repeat('z', 300) "
			            "WHERE id %% 3 = 0;")
			ctl.commit()
			result = scanner.join()
		self.assertEqual(result[0], (5000, 5000 * 5001 // 2, 5000))
		con.close()
		node.stop()

	def test_rows_survive_the_scan_moving_on(self):
		"""
		Rows a cached scan hands out refer to its page image until the next
		row; whatever keeps one longer has to have copied it.
		"""
		node = self.node
		node.start()
		self.make_table('o_src', 5000)
		node.restart()
		node.safe_psql(
		    'postgres', """
			CREATE TABLE o_dst (id int PRIMARY KEY, v text NOT NULL)
				USING orioledb;
			INSERT INTO o_dst SELECT * FROM o_src;
		""")
		self.assertEqual(
		    node.execute("""
			SELECT count(*) FROM o_src s
				FULL JOIN o_dst d USING (id)
			WHERE s.v IS DISTINCT FROM d.v;
		""")[0][0], 0)
		self.assertEqual(
		    node.execute("""
			WITH m AS MATERIALIZED (SELECT * FROM o_src)
			SELECT count(*), sum(length(v)) FROM m;
		""")[0], (5000, 5000 * 64))
		self.assertEqual(
		    node.execute("""
			WITH u AS (UPDATE o_src SET v = v || 'x' WHERE id %% 100 = 0
			           RETURNING id, v)
			SELECT count(*), min(length(v)) FROM u;
		""")[0], (50, 65))
		node.stop()
