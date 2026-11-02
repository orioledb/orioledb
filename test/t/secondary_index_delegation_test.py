#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest, ThreadQueryExecutor, wait_for_wait_event


class SecondaryIndexDelegationTest(BaseTest):
	"""
	A table with secondary indexes may have its updates done by the holder of
	the leaf page, or taken over by a batch, when the update changes no
	column any index uses: the secondary indexes then stay as they are.  An
	update that changes an indexed column is done the ordinary way.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf("orioledb.enable_stopevents = true\n"
		                      "orioledb.delegate_updates = on\n")
		self.node.start()
		self.node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE t (
				id int NOT NULL PRIMARY KEY,
				v bigint NOT NULL,
				s int NOT NULL
			) USING orioledb;
			INSERT INTO t SELECT g, 0, g * 1000 FROM generate_series(1, 10) g;
			CREATE INDEX t_s ON t (s);
		""")
		self.ctrl = self.node.connect(autocommit=True)

	def tearDown(self):
		try:
			self.node.stop()
		except Exception:
			pass
		super().tearDown()

	# --------------------------- helpers ---------------------------

	def _session(self, name, combine=False):
		conn = self.node.connect(autocommit=True)
		conn.execute("SET application_name = '%s'" % name)
		if combine:
			conn.execute("SET orioledb.combine_updates = on")
		pid = conn.execute("SELECT pg_backend_pid()")[0][0]
		return conn, pid

	def _arm(self, event, name):
		self.ctrl.execute("SELECT pg_stopevent_set('%s', "
		                  "'$applicationName == \"%s\"')" % (event, name))

	def _release(self, event):
		self.ctrl.execute("SELECT pg_stopevent_reset('%s')" % event)

	def _parked_at(self, event, pid):
		return self.ctrl.execute(
		    "SELECT EXISTS(SELECT 1 FROM pg_stopevents() "
		    "WHERE stopevent = '%s' AND waiter_pids @> ARRAY[%d])" %
		    (event, pid))[0][0]

	def _wait_at(self, event, pid):
		while not self._parked_at(event, pid):
			time.sleep(0.1)

	def _row(self, id):
		return self.ctrl.execute("SELECT v, s FROM t WHERE id = %d" % id)[0]

	def _assertIndexConsistent(self):
		by_index = self.ctrl.execute("""
			SET enable_seqscan = off;
			SET enable_bitmapscan = off;
			SELECT count(*), sum(s) FROM t WHERE s >= 0;
		""")
		by_seq = self.ctrl.execute("""
			SET enable_indexscan = off;
			SET enable_indexonlyscan = off;
			SET enable_bitmapscan = off;
			SELECT count(*), sum(s) FROM t WHERE s >= 0;
		""")
		self.ctrl.execute("RESET ALL")
		self.assertEqual(by_index, by_seq)

	def _queue_update_behind_holder(self, update):
		"""
		Park a holder of the leaf, queue the update on it, and let the holder
		look at it.  Returns whether the holder did the update.
		"""
		waiter, waiter_pid = self._session('waiter')
		holder, holder_pid = self._session('holder')

		self._arm('modify_start', 'waiter')
		waiter_thread = ThreadQueryExecutor(waiter, update)
		waiter_thread.start()
		self._wait_at('modify_start', waiter_pid)

		self._arm('before_get_waiters_with_tuples', 'holder')
		holder_thread = ThreadQueryExecutor(holder,
		                                    "INSERT INTO t VALUES (11, 0, 0)")
		holder_thread.start()
		self._wait_at('before_get_waiters_with_tuples', holder_pid)

		self._release('modify_start')
		time.sleep(1)

		self._arm('waiter_op_applied', 'holder')
		self._release('before_get_waiters_with_tuples')
		delegated = False
		deadline = time.time() + 10
		while waiter_thread.is_alive() and time.time() < deadline:
			if self._parked_at('waiter_op_applied', holder_pid):
				delegated = True
				break
			time.sleep(0.1)
		self._release('waiter_op_applied')
		holder_thread.join(30)
		waiter_thread.join(30)
		return delegated

	# ---------------------------- tests ----------------------------

	def test_unindexed_update_is_delegated(self):
		# "v * 1 + 1" compiles into no program: the holder writes the tuple
		# the waiter made
		self.assertTrue(
		    self._queue_update_behind_holder(
		        "UPDATE t SET v = v * 1 + 1 WHERE id = 1"))
		self.assertEqual(self._row(1), (1, 1000))
		self._assertIndexConsistent()

	def test_indexed_update_is_not_delegated(self):
		self.assertFalse(
		    self._queue_update_behind_holder(
		        "UPDATE t SET s = s * 1 + 1 WHERE id = 1"))
		self.assertEqual(self._row(1), (0, 1001))
		self.assertEqual(
		    self.ctrl.execute("""
				SET enable_seqscan = off;
				SET enable_bitmapscan = off;
				SELECT id FROM t WHERE s = 1001;
			""")[0][0], 1)
		self.ctrl.execute("RESET ALL")
		self._assertIndexConsistent()

	def test_batch_takes_unindexed_update(self):
		owner, owner_pid = self._session('owner', combine=True)
		waiter, waiter_pid = self._session('waiter', combine=True)

		self._arm('batch_commit_start', 'owner')
		owner_thread = ThreadQueryExecutor(
		    owner, "UPDATE t SET v = v + 1 WHERE id = 1")
		owner_thread.start()
		self._wait_at('batch_commit_start', owner_pid)

		waiter_thread = ThreadQueryExecutor(
		    waiter, "UPDATE t SET v = v + 10 WHERE id = 1")
		waiter_thread.start()
		wait_for_wait_event(self.node, waiter_pid, 'OrioleBatchQueue')

		self._release('batch_commit_start')
		owner_thread.join(30)
		waiter_thread.join(30)
		self.assertEqual(self._row(1), (11, 1000))
		self._assertIndexConsistent()

	def test_batch_leaves_indexed_update(self):
		owner, owner_pid = self._session('owner', combine=True)
		waiter, waiter_pid = self._session('waiter', combine=True)

		self._arm('batch_commit_start', 'owner')
		owner_thread = ThreadQueryExecutor(
		    owner, "UPDATE t SET v = v + 1 WHERE id = 1")
		owner_thread.start()
		self._wait_at('batch_commit_start', owner_pid)

		# Assigning an indexed column: no queue to join, an ordinary wait
		waiter_thread = ThreadQueryExecutor(
		    waiter, "UPDATE t SET s = s + 1 WHERE id = 1")
		waiter_thread.start()
		while self.ctrl.execute(
		    "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %d" %
		    waiter_pid)[0][0] != 'Lock':
			time.sleep(0.1)

		self._release('batch_commit_start')
		owner_thread.join(30)
		waiter_thread.join(30)
		self.assertEqual(self._row(1), (1, 1001))
		self._assertIndexConsistent()
