#!/usr/bin/env python3
# coding: utf-8

import time
from threading import Thread

from .base_test import BaseTest, ThreadQueryExecutor, wait_for_wait_event


class BatchInterruptTest(BaseTest):
	"""
	A batch transaction commits a statement's update before the statement
	returns, and the updates of the statements it takes over before those
	return too.  From that point on the outcome an ordinary transaction would
	give is "done": a cancel or a statement timeout that arrives later may
	cost a WARNING, never an ERROR, and a cancel in one backend must not cut
	the synchronous replication wait of another.  Until a batch has taken a
	waiter's update, the waiter's wait must be cancellable, as a row lock
	wait is.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf("orioledb.enable_stopevents = true\n")
		self.node.start()
		self.node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE t (
				id int NOT NULL PRIMARY KEY,
				v bigint NOT NULL
			) USING orioledb;
			INSERT INTO t SELECT g, 0 FROM generate_series(1, 10) g;
		""")
		self.ctrl = self.node.connect(autocommit=True)

	def tearDown(self):
		try:
			self.ctrl.execute("ALTER SYSTEM RESET synchronous_standby_names")
			self.ctrl.execute("SELECT pg_reload_conf()")
		except Exception:
			pass
		try:
			self.node.stop()
		except Exception:
			pass
		super().tearDown()

	# --------------------------- helpers ---------------------------

	def _session(self, statement_timeout=None):
		conn = self.node.connect(autocommit=True)
		conn.execute("SET orioledb.combine_updates = on")
		if statement_timeout is not None:
			conn.execute("SET statement_timeout = '%s'" % statement_timeout)
		pid = conn.execute("SELECT pg_backend_pid()")[0][0]
		return conn, pid

	def _start(self, conn, sql):
		thread = ThreadQueryExecutor(conn, sql)
		thread.start()
		return thread

	def _arm(self, event):
		self.ctrl.execute("SELECT pg_stopevent_set('%s', 'true')" % event)

	def _release(self, event):
		self.ctrl.execute("SELECT pg_stopevent_reset('%s')" % event)

	def _wait_at(self, event, pid):
		"""Wait until backend pid is parked at the given stop event."""
		while not self.ctrl.execute(
		    "SELECT EXISTS(SELECT 1 FROM pg_stopevents() "
		    "WHERE stopevent = '%s' AND waiter_pids @> ARRAY[%d])" %
		    (event, pid))[0][0]:
			time.sleep(0.1)

	def _value(self, id):
		return self.ctrl.execute("SELECT v FROM t WHERE id = %d" % id)[0][0]

	def _wait_finished(self, thread, timeout):
		Thread.join(thread, timeout)
		return not thread.is_alive()

	def _statement_result(self, thread, timeout=30):
		"""The statement's result or the exception it raised, not re-raised."""
		self.assertTrue(self._wait_finished(thread, timeout),
		                "the statement did not finish")
		return thread._return

	def _assertSucceeded(self, outcome, what):
		self.assertNotIsInstance(
		    outcome, Exception,
		    "%s is committed, but its statement reported: %s" %
		    (what, outcome))

	def _sync_rep_unreachable(self, on):
		if on:
			self.ctrl.execute(
			    "ALTER SYSTEM SET synchronous_standby_names = 'nosuch'")
		else:
			self.ctrl.execute("ALTER SYSTEM RESET synchronous_standby_names")
		self.ctrl.execute("SELECT pg_reload_conf()")
		time.sleep(0.5)

	# ---------------------------- owner ----------------------------

	def _owner_interrupted_after_commit(self, interrupt):
		owner, owner_pid = self._session(
		    statement_timeout='1s' if interrupt == 'timeout' else None)
		self._arm('batch_after_commit')
		t = self._start(owner, "UPDATE t SET v = v + 1 WHERE id = 1")
		self._wait_at('batch_after_commit', owner_pid)

		if interrupt == 'timeout':
			time.sleep(2)
		else:
			self.ctrl.execute("SELECT pg_cancel_backend(%d)" % owner_pid)
			time.sleep(0.5)

		self._release('batch_after_commit')
		outcome = self._statement_result(t)

		# The update is committed, so the statement must not report an error
		self.assertEqual(self._value(1), 1)
		self._assertSucceeded(outcome, "the update")

	def test_owner_statement_timeout_after_commit(self):
		self._owner_interrupted_after_commit('timeout')

	def test_owner_cancel_after_commit(self):
		self._owner_interrupted_after_commit('cancel')

	# ---------------------------- waiter ---------------------------

	def _queue_waiter_behind_owner(self, event):
		"""
		Park the owner's batch at the stop event, after it has written row 1,
		and queue a second update of row 1 in the owner's batch queue.
		"""
		owner, owner_pid = self._session()
		waiter, waiter_pid = self._session()
		self._arm(event)
		owner_thread = self._start(owner,
		                           "UPDATE t SET v = v + 1 WHERE id = 1")
		self._wait_at(event, owner_pid)
		self.waiter_conn = waiter
		waiter_thread = self._start(waiter,
		                            "UPDATE t SET v = v + 10 WHERE id = 1")
		wait_for_wait_event(self.node, waiter_pid, 'OrioleBatchQueue')
		return owner_thread, owner_pid, waiter_thread, waiter_pid

	def test_waiter_waits_for_its_own_sync_rep(self):
		"""
		The owner gives up waiting for the synchronous standby.  A waiter
		whose update went into the same batch has not, and must go on
		waiting, as its own commit would.
		"""
		self._sync_rep_unreachable(True)
		owner_thread, owner_pid, waiter_thread, waiter_pid = \
            self._queue_waiter_behind_owner('batch_commit_start')

		self._release('batch_commit_start')
		wait_for_wait_event(self.node, owner_pid, 'SyncRep')
		self.ctrl.execute("SELECT pg_cancel_backend(%d)" % owner_pid)
		self._assertSucceeded(self._statement_result(owner_thread),
		                      "the owner's update")

		# Still waiting for the standby, as it was never told to stop
		self.assertFalse(self._wait_finished(waiter_thread, 2))
		wait_for_wait_event(self.node, waiter_pid, 'SyncRep')

		self._sync_rep_unreachable(False)
		outcome = self._statement_result(waiter_thread)
		self.assertEqual(self._value(1), 11)
		self._assertSucceeded(outcome, "the waiter's update")

	def test_waiter_cancel_in_queue(self):
		"""
		A waiter the batch has not taken yet can be cancelled, and its
		update is then not done by anybody.
		"""
		owner_thread, owner_pid, waiter_thread, waiter_pid = \
            self._queue_waiter_behind_owner('batch_commit_start')

		self.ctrl.execute("SELECT pg_cancel_backend(%d)" % waiter_pid)
		self.assertTrue(self._wait_finished(waiter_thread, 10),
		                "the waiter did not react to the cancel")
		outcome = waiter_thread._return
		self.assertIsInstance(outcome, Exception)
		self.assertIn("canceling statement due to user request", str(outcome))

		# The owner has not gone past the cancelled entry yet, so the same
		# backend must not queue again: it waits the ordinary way instead
		retry_thread = self._start(self.waiter_conn,
		                           "UPDATE t SET v = v + 100 WHERE id = 1")
		while self.ctrl.execute(
		    "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %d" %
		    waiter_pid)[0][0] != 'Lock':
			time.sleep(0.1)

		self._release('batch_commit_start')
		self._assertSucceeded(self._statement_result(owner_thread),
		                      "the owner's update")
		self._assertSucceeded(self._statement_result(retry_thread),
		                      "the retried update")
		self.assertEqual(self._value(1), 101)

	def test_waiter_cancel_after_taken(self):
		"""
		Once the batch has taken the waiter's update, the cancel has to wait
		for the batch, and since the batch commits it, the waiter's statement
		succeeds.
		"""
		owner_thread, owner_pid, waiter_thread, waiter_pid = \
            self._queue_waiter_behind_owner('batch_commit_start')

		# Let the owner take the waiter's update, then stop before commit
		self._arm('batch_commit_absorbed')
		self._release('batch_commit_start')
		self._wait_at('batch_commit_absorbed', owner_pid)
		self.ctrl.execute("SELECT pg_cancel_backend(%d)" % waiter_pid)
		time.sleep(0.5)
		self._release('batch_commit_absorbed')

		self._assertSucceeded(self._statement_result(owner_thread),
		                      "the owner's update")
		outcome = self._statement_result(waiter_thread)
		self.assertEqual(self._value(1), 11)
		self._assertSucceeded(outcome, "the waiter's update")
