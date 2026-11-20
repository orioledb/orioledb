#!/usr/bin/env python3
# coding: utf-8

import random
import threading
import time

from .base_test import BaseTest


class UndoNumaChunksTest(BaseTest):
	"""Row-level undo handed out in chunks per NUMA node.

	The tests pretend there are three nodes and make the chunks and the undo
	ring small, so that records of one row's versions and of one transaction
	come from different chunks of different nodes all the time.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "orioledb.debug_undo_numa_nodes = 3\n"
		    "orioledb.undo_chunk_size = 16kB\n"
		    "orioledb.undo_buffers = 128\n")

	def test_idle_chunk_does_not_hold_checkpoint(self):
		# A checkpoint waits for the undo up to lastUsedLocation to be
		# written.  The current chunk of a node nobody writes to any more
		# must not keep it waiting.
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		               "CREATE TABLE o_test (id int PRIMARY KEY, v int)"
		               " USING orioledb;\n"
		               "INSERT INTO o_test SELECT g, 0"
		               " FROM generate_series(1, 100) g;")
		for i in range(10):
			node.safe_psql(f"UPDATE o_test SET v = v + 1 WHERE id = {i + 1};")
		node.safe_psql("SET statement_timeout = '60s'; CHECKPOINT;")
		node.safe_psql("UPDATE o_test SET v = v + 1;")
		node.safe_psql("SET statement_timeout = '60s'; CHECKPOINT;")
		self.assertEqual(node.execute("SELECT sum(v) FROM o_test;")[0][0], 110)
		node.stop()

	def test_hot_rows_across_nodes(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		               "CREATE TABLE o_hot (id int PRIMARY KEY, v int,"
		               " pad text) USING orioledb;\n"
		               "INSERT INTO o_hot SELECT g, 0, repeat('x', 100)"
		               " FROM generate_series(1, 10) g;")

		# A snapshot taken before all the updates must keep seeing the
		# rows as they were, through version chains whose records come
		# from many chunks.
		reader = node.connect()
		reader.execute("BEGIN ISOLATION LEVEL REPEATABLE READ;")
		self.assertEqual(reader.execute("SELECT sum(v) FROM o_hot;")[0][0], 0)

		nthreads = 8
		iterations = 300
		committed = [0] * nthreads
		errors = []

		def worker(n):
			rnd = random.Random(n)
			try:
				con = node.connect()
				for _ in range(iterations):
					# Rows in ascending order, so that no deadlocks come up.
					ids = sorted(rnd.sample(range(1, 11), rnd.randint(1, 4)))
					con.begin()
					for i in ids:
						con.execute(
						    f"UPDATE o_hot SET v = v + 1, pad = repeat('y', "
						    f"{rnd.randint(1, 200)}) WHERE id = {i};")
					if rnd.random() < 0.3:
						con.execute("SAVEPOINT s;")
						con.execute("UPDATE o_hot SET v = v + 1000 "
						            f"WHERE id = {ids[-1]};")
						con.execute("ROLLBACK TO SAVEPOINT s;")
					if rnd.random() < 0.2:
						con.rollback()
					else:
						con.commit()
						committed[n] += len(ids)
				con.close()
			except Exception as e:
				errors.append(e)

		threads = [
		    threading.Thread(target=worker, args=(n, ))
		    for n in range(nthreads)
		]
		for t in threads:
			t.start()
		for t in threads:
			t.join()
		self.assertEqual(errors, [])

		self.assertEqual(reader.execute("SELECT sum(v) FROM o_hot;")[0][0], 0)
		reader.commit()
		reader.close()

		expected = sum(committed)
		self.assertEqual(
		    node.execute("SELECT sum(v) FROM o_hot;")[0][0], expected)

		node.safe_psql("SET statement_timeout = '60s'; CHECKPOINT;")
		node.safe_psql("UPDATE o_hot SET v = v + 1;")
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(
		    node.execute("SELECT sum(v) FROM o_hot;")[0][0], expected + 10)
		node.stop()

	def test_rollback_across_chunks(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		               "CREATE TABLE o_test (id int PRIMARY KEY, v text)"
		               " USING orioledb;\n"
		               "INSERT INTO o_test SELECT g, 'a'"
		               " FROM generate_series(1, 2000) g;\n"
		               "CREATE TABLE o_other (id int PRIMARY KEY, v int)"
		               " USING orioledb;\n"
		               "INSERT INTO o_other SELECT g, 0"
		               " FROM generate_series(1, 20) g;")

		other = node.connect()
		con = node.connect()
		con.begin()
		for i in range(20):
			con.execute(f"UPDATE o_test SET v = repeat('{i % 10}', 50)"
			            f" WHERE mod(id, 20) = {i};")
			# Another transaction keeps moving the chunks on.
			other.execute(f"UPDATE o_other SET v = v + 1 WHERE id = {i + 1};")
			other.commit()
			if i == 10:
				con.execute("SAVEPOINT s;")
		con.execute("ROLLBACK TO SAVEPOINT s;")
		self.assertEqual(
		    con.execute("SELECT count(*) FROM o_test WHERE v = 'a';")[0][0],
		    2000 - 11 * 100)
		con.rollback()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_test WHERE v = 'a';")[0][0],
		    2000)
		con.close()
		other.close()
		node.stop()

	def test_churn_through_small_ring(self):
		# Updates of wide rows go round the small undo ring many times over,
		# and the readers walk back through the older versions, which by then
		# have been written out of the ring.
		node = self.node
		node.append_conf('postgresql.conf', "statement_timeout = '60s'\n")
		node.start()
		rows = 2000
		workers = 4
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		               "CREATE TABLE o_churn (id int PRIMARY KEY, pad text)"
		               " USING orioledb;\n"
		               "INSERT INTO o_churn SELECT g, repeat('x', 2000)"
		               f" FROM generate_series(1, {rows}) g;")

		stop = threading.Event()
		failures = []

		def churn(worker):
			low = 1 + worker * (rows // workers)
			high = low + rows // workers - 1
			try:
				con = node.connect()
				round = 0
				while not stop.is_set():
					round += 1
					con.begin('repeatable read')
					count = con.execute(
					    "SELECT count(*) FROM o_churn "
					    f"WHERE id BETWEEN {low} AND {high};")[0][0]
					if count != rows // workers:
						failures.append(f"worker {worker}: {count} rows")
					con.execute(
					    f"UPDATE o_churn SET pad = repeat('{round % 10}', 2000) "
					    f"WHERE id BETWEEN {low} AND {high};")
					con.commit()
				con.close()
			except Exception as e:
				failures.append(f"worker {worker}: {e}")

		threads = [
		    threading.Thread(target=churn, args=(worker, ))
		    for worker in range(workers)
		]
		for t in threads:
			t.start()
		try:
			for _ in range(5):
				time.sleep(1)
				node.safe_psql("CHECKPOINT;")
		finally:
			stop.set()
			for t in threads:
				t.join()
		self.assertEqual(failures, [])
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_churn;")[0][0], rows)
		node.stop()
