#!/usr/bin/env python3
# coding: utf-8

import threading
import time

from testgres.enums import IsolationLevel

from .base_test import BaseTest


class RowLockRRTest(BaseTest):

	def test_concurrent_for_update_no_serialization_error(self):
		"""
		REPEATABLE READ transactions that only lock a few hot rows must wait
		for each other, never fail: nobody modifies the rows.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				v int NOT NULL
			) USING orioledb;
			INSERT INTO o_test SELECT i, 0 FROM generate_series(1, 5) i;
		""")

		stop = threading.Event()
		errors = []
		counts = []

		def worker(n):
			done = 0
			with node.connect() as con:
				while not stop.is_set():
					con.begin(IsolationLevel.RepeatableRead)
					try:
						con.execute("SELECT v FROM o_test "
						            f"WHERE id = {done % 5 + 1} FOR UPDATE")
						con.commit()
						done += 1
					except Exception as e:
						con.rollback()
						errors.append(str(e))
			counts.append(done)

		threads = [
		    threading.Thread(target=worker, args=(n, )) for n in range(8)
		]
		for t in threads:
			t.start()
		time.sleep(3)
		stop.set()
		for t in threads:
			t.join()

		self.assertEqual(errors[:3], [])
		self.assertGreater(sum(counts), 0)
		node.stop()
