#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import struct
import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent

ROWS = 150000


@unittest.skipIf(BaseTest.get_pg_version() < 18,
                 'AIO prefetch needs PostgreSQL 18')
class BtreePrefetchTest(BaseTest):
	"""
	Leaf prefetch of a sequential scan through the AIO subsystem with
	io_method = worker, where the reads go into shared buffers of a pool so
	that an io worker can perform them.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "io_method = worker\n"
		    "orioledb.main_buffers = 8MB\n"
		    "orioledb.btree_prefetch_buffers = 16\n"
		    "max_parallel_workers_per_gather = 0\n")

	def make_table(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_prefetch (id int PRIMARY KEY, v text NOT NULL)
				USING orioledb;
			INSERT INTO o_prefetch
				SELECT i, repeat(md5(i::text), 4)
				FROM generate_series(1, {ROWS}) i;
			CHECKPOINT;
		""")

	def scan(self, con, depth):
		con.execute(f"SET orioledb.btree_prefetch_depth = {depth};")
		return con.execute("SELECT count(*), sum(id::bigint), sum(length(v)) "
		                   "FROM o_prefetch;")[0]

	def held(self):
		return self.node.execute(
		    "SELECT orioledb_prefetch_buffers_held();")[0][0]

	def test_prefetch_matches_plain_scan(self):
		"""
		A prefetching scan returns what a plain one does, also from an older
		snapshot that needs the pages' undo, and gives its buffers back.
		Adjacent pages are read together, several per read.
		"""
		node = self.node
		self.make_table()
		expected = (ROWS, ROWS * (ROWS + 1) // 2, ROWS * 128)
		with node.connect() as con:
			self.assertEqual(self.scan(con, 0), expected)
			self.assertEqual(self.scan(con, 16), expected)
			# deeper than the pool: some pages get read synchronously
			self.assertEqual(self.scan(con, 64), expected)

			con.commit()
			con.begin()
			con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
			self.assertEqual(self.scan(con, 16), expected)
			node.safe_psql(
			    'postgres', "UPDATE o_prefetch SET v = 'x' "
			    "WHERE id % 1000 = 0; CHECKPOINT;")
			self.assertEqual(self.scan(con, 16), expected)
			con.commit()

			self.assertEqual(self.scan(con, 16),
			                 (ROWS, expected[1], expected[2] - 150 * 127))
		self.assertEqual(self.held(), 0)
		node.stop()

	def test_prefetch_error_releases_buffers(self):
		"""
		A scan that fails with reads still in flight waits for them and
		gives the buffers they target back.
		"""
		node = self.node
		self.make_table()
		with node.connect() as con:
			con.execute("SET orioledb.btree_prefetch_depth = 16;")
			with self.assertRaises(Exception) as cm:
				con.execute("SELECT sum(1 / (id - 50000)) FROM o_prefetch;")
			self.assertIn("division by zero", str(cm.exception))
			con.rollback()
		self.assertEqual(self.held(), 0)
		node.stop()

	def interrupt_parked_scan(self, signal_function):
		"""
		Parks a prefetching scan on a disk page, when its ring holds pool
		buffers, signals it and lets it go.  A parked backend does not act on
		the signal, so the interrupt lands in the middle of the scan.
		"""
		node = self.node
		self.make_table()
		con = node.connect()
		con.execute("SET orioledb.enable_stopevents = true;")
		con.execute("SET orioledb.btree_prefetch_depth = 16;")
		pid = con.pid
		with node.connect() as ctl:
			ctl.execute("SELECT pg_stopevent_set('scan_disk_page', 'true');")
			ctl.commit()
			scanner = ThreadQueryExecutor(
			    con, "SELECT count(*) FROM o_prefetch WHERE v = 'none';")
			scanner.start()
			wait_stopevent(node, pid)
			self.assertGreater(self.held(), 0)
			ctl.execute(f"SELECT {signal_function}({pid});")
			ctl.execute("SELECT pg_stopevent_reset('scan_disk_page');")
			ctl.commit()
			with self.assertRaises(Exception):
				scanner.join()
		node.poll_query_until("SELECT orioledb_prefetch_buffers_held() = 0;")
		try:
			con.close()
		except Exception:
			pass
		node.stop()

	def test_prefetch_cancel_releases_buffers(self):
		self.interrupt_parked_scan('pg_cancel_backend')

	def test_prefetch_backend_exit_releases_buffers(self):
		self.interrupt_parked_scan('pg_terminate_backend')

	def test_prefetch_checksum_failure(self):
		"""
		A page read through an io worker gets its checksum verified like a
		page read synchronously.
		"""
		node = self.node
		self.make_table()
		datoid, relnode = node.execute("""
			SELECT (SELECT oid FROM pg_database WHERE datname = 'postgres'),
			       (SELECT relfilenode FROM pg_class
			        WHERE relname = 'o_prefetch_pkey');
		""")[0]
		node.stop()

		path = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                    str(relnode))
		files = sorted(glob.glob(path))
		self.assertTrue(files, f"no data file matched {path}")
		with open(files[-1], 'r+b') as f:
			f.seek(16000)
			f.write(struct.pack('<Q', 0xDEADBEEF))

		node.start()
		with node.connect() as con:
			con.execute("SET orioledb.btree_prefetch_depth = 16;")
			with self.assertRaises(Exception) as cm:
				con.execute("SELECT count(*), sum(length(v)) FROM o_prefetch;")
			self.assertIn("invalid leaf page", str(cm.exception))
			con.rollback()
		with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
			self.assertIn("page checksum mismatch", f.read())
		self.assertEqual(self.held(), 0)
		node.stop()
