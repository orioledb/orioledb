#!/usr/bin/env python3
# coding: utf-8
"""Tests for keeping the ring buffer populated across checkpoints."""

import threading
import time
import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

SMALL_UNDO_CONF = """
orioledb.main_buffers = 8MB
orioledb.undo_buffers = 128
orioledb.xid_buffers = 128
"""


class UndoRingKeepCheckpointTest(BaseTest):

	def _create_table(self, name='o_ring'):
		self.node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE IF NOT EXISTS {name} (
				id integer PRIMARY KEY,
				v  integer NOT NULL
			) USING orioledb;
		""")

	def _amcheck(self, name='o_ring'):
		rows = self.node.execute(
		    'postgres',
		    f"SELECT * FROM verify_orioledb('{name}'::regclass, true);")
		self.assertEqual(rows, [], f"verify_orioledb({name}) reported issues")

	def test_concurrent_rll_with_checkpoint(self):
		"""Row-level locks (in-place undo writes) racing with CHECKPOINT."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_ring SELECT i, 0 FROM generate_series(1, 200) i;")

		# FOR UPDATE installs a lock-only undo record; COMMIT triggers
		# cleanChainHasLocks() -> in-place tuphdr writes.
		n_workers = 8
		iters = 80
		worker_cons = [node.connect() for _ in range(n_workers)]
		workers = []
		for w, con in enumerate(worker_cons):
			q = ";".join([
			    f"BEGIN; "
			    f"SELECT v FROM o_ring WHERE id = {(w * 13 + i) % 200 + 1} FOR UPDATE; "
			    f"UPDATE o_ring SET v = v + 1 WHERE id = {(w * 13 + i) % 200 + 1}; "
			    f"COMMIT" for i in range(iters)
			]) + ";"
			workers.append(ThreadQueryExecutor(con, q))

		chkp_con = node.connect()
		stop = [False]

		def chkp_loop():
			while not stop[0]:
				chkp_con.execute("CHECKPOINT;")
				time.sleep(0.05)

		chkp_thread = threading.Thread(target=chkp_loop)
		chkp_thread.start()

		for t in workers:
			t.start()
		for t in workers:
			t.join()
		stop[0] = True
		chkp_thread.join()

		expected = n_workers * iters
		got = node.execute('postgres', "SELECT SUM(v) FROM o_ring;")[0][0]
		self.assertEqual(
		    got, expected,
		    f"lost or duplicated updates: got {got}, expected {expected}")
		self._amcheck()
		chkp_con.close()
		for c in worker_cons:
			c.close()
		node.stop()

	def test_undo_wraparound_in_fsync(self):
		"""Force more undo than the ring holds and call CHECKPOINT."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		# UPDATE-all produces > ring_size undo, wrapping repeatedly.
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_ring SELECT i, i FROM generate_series(1, 100000) i;"
		)
		node.safe_psql('postgres', 'CHECKPOINT;')

		con = node.connect()
		con.begin()
		con.execute("UPDATE o_ring SET v = v + 1;")

		chkp_con = node.connect()
		chkp_con.execute('CHECKPOINT;')
		con.commit()

		got = node.execute('postgres', "SELECT SUM(v) FROM o_ring;")[0][0]
		# sum(1..100000) + 100000
		self.assertEqual(got, 5000150000)
		self._amcheck()
		con.close()
		chkp_con.close()
		node.stop()

	def test_eviction_pressure_during_checkpoint(self):
		"""Long-retention txn + writer in another backend + CHECKPOINTs."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_ring SELECT i, i FROM generate_series(1, 50000) i;")

		# Held snapshot pins undo retention -> writer hits slot-pressure
		# eviction path.
		ret_con = node.connect()
		ret_con.begin()
		ret_con.execute("SELECT count(*) FROM o_ring;")

		writer_con = node.connect()
		chkp_con = node.connect()

		def writer():
			for i in range(40):
				writer_con.execute(
				    "UPDATE o_ring SET v = v + 1 WHERE id BETWEEN %d AND %d;" %
				    (i * 1000 + 1, i * 1000 + 1000))

		def checkpointer():
			for _ in range(20):
				chkp_con.execute("CHECKPOINT;")
				time.sleep(0.02)

		tw = threading.Thread(target=writer)
		tc = threading.Thread(target=checkpointer)
		tw.start()
		tc.start()
		tw.join()
		tc.join()

		ret_con.commit()
		ret_con.close()
		writer_con.close()
		chkp_con.close()

		self._amcheck()
		node.stop()

	def test_crash_recovery_with_hot_ring(self):
		"""CHECKPOINT then more txns; SIGKILL; verify recovered state."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_ring SELECT i, 1 FROM generate_series(1, 1000) i;")
		node.safe_psql('postgres', 'CHECKPOINT;')

		# Post-checkpoint mix: new appends + in-place writes (row lock).
		con = node.connect()
		con.execute("INSERT INTO o_ring SELECT i, 2 FROM "
		            "generate_series(1001, 2000) i;")
		con.begin()
		con.execute(
		    "SELECT v FROM o_ring WHERE id BETWEEN 1 AND 100 FOR UPDATE;")
		con.execute("UPDATE o_ring SET v = v + 10 WHERE id BETWEEN 1 AND 100;")
		con.commit()
		con.close()

		self.crash_with_os_buffer_loss()
		node.start()

		got = node.execute('postgres', "SELECT COUNT(*) FROM o_ring;")[0][0]
		self.assertEqual(got, 2000)
		got_sum = node.execute('postgres', "SELECT SUM(v) FROM o_ring;")[0][0]
		# 1000*1 + 1000*2 + 100*10
		self.assertEqual(got_sum, 4000)
		self._amcheck()
		node.stop()

	def test_undo_flush_race_via_stopevent(self):
		"""Park flush mid-page; row-lock COMMIT mutates the same page;
		resume.  Asserts result is consistent and amcheck clean."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		self._create_table()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_ring SELECT i, 0 FROM generate_series(1, 200) i;")

		worker_con = node.connect()
		ctrl_con = node.connect()
		ctrl_con.execute("SELECT pg_stopevent_set('undo_flush', 'true');")

		chkp_con = node.connect()
		t_chkp = ThreadQueryExecutor(chkp_con, "CHECKPOINT;")
		t_chkp.start()
		wait_checkpointer_stopevent(node)

		# Mid-flush: row-lock COMMIT fires in-place undo write.
		worker_con.begin()
		worker_con.execute(
		    "SELECT v FROM o_ring WHERE id BETWEEN 1 AND 50 FOR UPDATE;")
		worker_con.execute(
		    "UPDATE o_ring SET v = v + 1 WHERE id BETWEEN 1 AND 50;")
		worker_con.commit()

		ctrl_con.execute("SELECT pg_stopevent_reset('undo_flush');")
		t_chkp.join()

		got = node.execute('postgres', "SELECT SUM(v) FROM o_ring;")[0][0]
		self.assertEqual(got, 50)
		self._amcheck()
		worker_con.close()
		ctrl_con.close()
		chkp_con.close()
		node.stop()

	def test_dirty_bit_overhead_smoke(self):
		"""Bulk write under small buffers; no PANIC, no hang."""
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		self._create_table()
		t0 = time.time()
		node.safe_psql(
		    'postgres',
		    "INSERT INTO o_ring SELECT i, i FROM generate_series(1, 50000) i;")
		node.safe_psql('postgres', "UPDATE o_ring SET v = v + 1;")
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertLess(time.time() - t0, 120)
		self._amcheck()
		node.stop()


if __name__ == '__main__':
	unittest.main()
