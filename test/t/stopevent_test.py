#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest, ThreadQueryExecutor, wait_stopevent


class StopEventTest(BaseTest):
	"""
	The `$hits` jsonpath variable.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf("orioledb.enable_stopevents = true\n")
		self.node.start()
		self.node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE t (id int PRIMARY KEY, v int) USING orioledb;
		""")

	def tearDown(self):
		try:
			self.node.stop()
		except Exception:
			pass
		super().tearDown()

	def waiters(self, event):
		return self.node.execute("SELECT waiter_pids FROM pg_stopevents() "
		                         "WHERE stopevent = '%s'" % event)[0][0]

	def arm(self, condition):
		self.node.safe_psql("SELECT pg_stopevent_set('modify_start', '%s');" %
		                    condition)

	def start_insert(self, con, id):
		t = ThreadQueryExecutor(con, "INSERT INTO t VALUES (%d, 1);" % id)
		t.start()
		return t

	def test_hits_limit_and_reset(self):
		"""
		`$hits < N` stops exactly N arrivals; the next pg_stopevent_set()
		starts counting again.
		"""
		node = self.node
		cons = [node.connect() for _ in range(4)]
		pids = [c.pid for c in cons]
		cond = "$pid == %d || $pid == %d || $pid == %d || $pid == %d" % tuple(
		    pids)
		self.arm("($hits < 2) && (%s)" % cond)

		threads = [self.start_insert(cons[0], 1)]
		wait_stopevent(node, pids[0])
		threads.append(self.start_insert(cons[1], 2))
		wait_stopevent(node, pids[1])
		# the third arrival is past the limit and goes through
		cons[2].execute("INSERT INTO t VALUES (3, 1);")
		cons[2].commit()
		self.assertEqual(sorted(self.waiters('modify_start')),
		                 sorted(pids[:2]))

		# pg_stopevent_set() resets the count
		node.safe_psql("SELECT pg_stopevent_reset('modify_start');")
		for t in threads:
			t.join()
		self.arm("($hits < 1) && (%s)" % cond)
		t = self.start_insert(cons[3], 4)
		wait_stopevent(node, pids[3])
		node.safe_psql("SELECT pg_stopevent_reset('modify_start');")
		t.join()

	def test_hits_waiter_rechecks_do_not_count(self):
		"""
		A parked process re-evaluates the condition every second.  Those
		re-checks neither add hits nor release it.
		"""
		node = self.node
		c1, c2 = node.connect(), node.connect()
		p1, p2 = c1.pid, c2.pid
		self.arm("$hits < 1 && ($pid == %d || $pid == %d)" % (p1, p2))
		t = self.start_insert(c1, 1)
		wait_stopevent(node, p1)
		time.sleep(3)
		self.assertEqual(self.waiters('modify_start'), [p1])
		# the second arrival is the first one past the limit
		c2.execute("INSERT INTO t VALUES (2, 1);")
		c2.commit()
		node.safe_psql("SELECT pg_stopevent_reset('modify_start');")
		t.join()

	def test_condition_without_hits(self):
		"""A condition that does not mention $hits stops every arrival."""
		node = self.node
		for id in (1, 2):
			con = node.connect()
			pid = con.pid
			self.arm("$pid == %d" % pid)
			t = self.start_insert(con, id)
			wait_stopevent(node, pid)
			node.safe_psql("SELECT pg_stopevent_reset('modify_start');")
			t.join()
