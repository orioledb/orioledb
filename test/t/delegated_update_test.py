#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest, ThreadQueryExecutor


class DelegatedUpdateTest(BaseTest):
	"""
	The holder of a leaf page may finish the update of a process queued on it
	(orioledb.delegate_updates).  It must judge the row the way the normal
	path does: by its last version, not by the header on the page, which may
	be a lock-only record of another transaction above that version.
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
				v bigint NOT NULL
			) USING orioledb;
			INSERT INTO t SELECT g, 0 FROM generate_series(1, 10) g;
		""")
		self.ctrl = self.node.connect(autocommit=True)

	def tearDown(self):
		try:
			self.node.stop()
		except Exception:
			pass
		super().tearDown()

	def _session(self, name):
		conn = self.node.connect(autocommit=True)
		conn.execute("SET application_name = '%s'" % name)
		pid = conn.execute("SELECT pg_backend_pid()")[0][0]
		return conn, pid

	def _wait_at(self, event, pid):
		while not self.ctrl.execute(
		    "SELECT EXISTS(SELECT 1 FROM pg_stopevents() "
		    "WHERE stopevent = '%s' AND waiter_pids @> ARRAY[%d])" %
		    (event, pid))[0][0]:
			time.sleep(0.1)

	def test_committed_writer_below_lock_record(self):
		"""
		A writer that committed after the waiter's snapshot sits below a
		lock-only record of a locker that committed before it.  The holder
		must not write the waiter's tuple, made from the version before the
		writer's, over the writer's update.
		"""
		writer = self.node.connect()
		locker = self.node.connect()
		waiter, waiter_pid = self._session('waiter')
		holder, holder_pid = self._session('holder')

		# A writer updates row 1 and keeps its transaction open
		writer.execute("UPDATE t SET v = v + 1 WHERE id = 1")

		# A locker takes FOR KEY SHARE over it, compatible with the update,
		# and commits: its lock-only record is now the header of the row
		locker.execute("SELECT * FROM t WHERE id = 1 FOR KEY SHARE")
		locker.commit()

		# The waiter's snapshot sees v = 0: it makes its new tuple from that.
		# "v * 1 + 1" does not compile into a SET program, so the holder can
		# only write the tuple as the waiter made it.
		self.ctrl.execute("SELECT pg_stopevent_set('modify_start', "
		                  "'$applicationName == \"waiter\"')")
		waiter_thread = ThreadQueryExecutor(
		    waiter, "UPDATE t SET v = v * 1 + 1 WHERE id = 1")
		waiter_thread.start()
		self._wait_at('modify_start', waiter_pid)

		# The writer commits after the waiter's snapshot
		writer.commit()

		# A holder takes the leaf to insert a row, and keeps it
		self.ctrl.execute(
		    "SELECT pg_stopevent_set('before_get_waiters_with_tuples', "
		    "'$applicationName == \"holder\"')")
		holder_thread = ThreadQueryExecutor(holder,
		                                    "INSERT INTO t VALUES (11, 0)")
		holder_thread.start()
		self._wait_at('before_get_waiters_with_tuples', holder_pid)

		# The waiter queues on the leaf with its tuple
		self.ctrl.execute("SELECT pg_stopevent_reset('modify_start')")
		time.sleep(1)

		# The holder goes on, and looks at the waiter's update
		self.ctrl.execute(
		    "SELECT pg_stopevent_reset('before_get_waiters_with_tuples')")
		holder_thread.join(30)
		waiter_thread.join(30)

		# Both updates count: the waiter's is made from the writer's row
		self.assertEqual(
		    self.ctrl.execute("SELECT v FROM t WHERE id = 1")[0][0], 2)

		writer.close()
		locker.close()
