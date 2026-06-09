#!/usr/bin/env python3
# coding: utf-8
"""Perf tests for keeping the ring buffer populated across checkpoints.

Reports timings; loose assertions only.  Gated by ORIOLEDB_RUN_PERF=1.
"""

import os
import time
import unittest

from .base_test import BaseTest

SMALL_UNDO_CONF = """
orioledb.main_buffers = 16MB
orioledb.undo_buffers = 256
orioledb.xid_buffers = 128
"""


def _skip_unless_perf():
	return unittest.skipUnless(
	    os.environ.get('ORIOLEDB_RUN_PERF'),
	    'set ORIOLEDB_RUN_PERF=1 to run performance tests')


@_skip_unless_perf()
class UndoRingKeepCheckpointPerfTest(BaseTest):

	def _create_table(self):
		self.node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_perf (
				id integer PRIMARY KEY,
				v integer NOT NULL
			) USING orioledb;
		""")

	def test_post_checkpoint_hot_ring(self):
		"""CSN read latency before vs after CHECKPOINT."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()

		node.safe_psql(
		    'postgres', """
			DO $$ BEGIN
				FOR i IN 1..5000 LOOP
					INSERT INTO o_perf VALUES (i, i);
				END LOOP;
			END $$;
		""")

		t0 = time.time()
		node.execute('postgres', "SELECT count(*) FROM o_perf WHERE v < 1000;")
		t_before = time.time() - t0

		node.safe_psql('postgres', 'CHECKPOINT;')

		t0 = time.time()
		node.execute('postgres', "SELECT count(*) FROM o_perf WHERE v < 1000;")
		t_after = time.time() - t0

		print(f"\nt_before={t_before*1000:.2f}ms "
		      f"t_after={t_after*1000:.2f}ms "
		      f"ratio={t_after/t_before:.2f}x")
		# Pre-patch slow disk path was ~20x; 5x gate catches regressions.
		self.assertLess(
		    t_after, max(t_before * 5, 0.5),
		    f"post-CHECKPOINT read regressed: {t_after*1000:.1f}ms vs "
		    f"baseline {t_before*1000:.1f}ms")
		node.stop()

	def test_dirty_bit_writer_overhead(self):
		"""Wall time for a fixed amount of undo traffic."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()

		t0 = time.time()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_perf SELECT i, i FROM generate_series(1, 200000) i;"
		)
		elapsed = time.time() - t0
		print(f"\n200k INSERT = {elapsed:.2f}s")
		self.assertLess(elapsed, 30)
		node.stop()

	def test_concurrent_eviction_flush_throughput(self):
		"""Heavy writer + long retention txn + CHECKPOINTs."""
		import threading
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_perf SELECT i, i FROM generate_series(1, 50000) i;")

		ret_con = node.connect()
		ret_con.begin()
		ret_con.execute("SELECT count(*) FROM o_perf;")

		stop = [False]
		updates = [0]

		def writer():
			con = node.connect()
			i = 0
			while not stop[0]:
				con.execute(
				    "UPDATE o_perf SET v = v + 1 WHERE id BETWEEN %d AND %d;" %
				    (i % 50 * 1000 + 1, i % 50 * 1000 + 1000))
				updates[0] += 1
				i += 1
			con.close()

		def chkp():
			con = node.connect()
			while not stop[0]:
				con.execute("CHECKPOINT;")
				time.sleep(0.05)
			con.close()

		tw = threading.Thread(target=writer)
		tc = threading.Thread(target=chkp)
		tw.start()
		tc.start()
		time.sleep(5)
		stop[0] = True
		tw.join()
		tc.join()
		ret_con.commit()
		ret_con.close()

		print(f"\n{updates[0]} batched UPDATEs in 5s")
		self.assertGreater(updates[0], 10,
		                   "writer was starved by checkpointer")
		node.stop()

	def test_rll_checkpoint_stress(self):
		"""Sustained SELECT FOR UPDATE + UPDATE + CHECKPOINT mix."""
		import threading
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_perf SELECT i, 0 FROM generate_series(1, 500) i;")

		stop = [False]
		txns = [0]
		n_workers = 4

		def worker(wid):
			con = node.connect()
			i = 0
			while not stop[0]:
				k = (wid * 91 + i * 7) % 500 + 1
				con.execute("BEGIN; "
				            f"SELECT v FROM o_perf WHERE id = {k} FOR UPDATE; "
				            f"UPDATE o_perf SET v = v + 1 WHERE id = {k}; "
				            "COMMIT;")
				txns[0] += 1
				i += 1
			con.close()

		def chkp():
			con = node.connect()
			while not stop[0]:
				con.execute("CHECKPOINT;")
				time.sleep(0.02)
			con.close()

		threads = [
		    threading.Thread(target=worker, args=(w, ))
		    for w in range(n_workers)
		]
		tc = threading.Thread(target=chkp)
		for t in threads:
			t.start()
		tc.start()
		time.sleep(10)
		stop[0] = True
		for t in threads:
			t.join()
		tc.join()

		got = node.execute('postgres', "SELECT SUM(v) FROM o_perf;")[0][0]
		print(f"\n{txns[0]} txns in 10s; SUM(v)={got}")
		self.assertEqual(got, txns[0])
		node.stop()


if __name__ == '__main__':
	unittest.main()
