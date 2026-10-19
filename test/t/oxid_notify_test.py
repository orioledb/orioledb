#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class OxidNotifyTest(BaseTest):

	def test_savepoint_rollback_wakes_waiter_without_deadlock(self):
		"""
		Rolling back a savepoint releases row locks, so oxid_notify_all()
		lets the backends waiting for our transaction go and recheck.  It
		used to take them off the wait queue with RemoveFromWaitQueue(),
		which marks a waiter a deadlock victim, and overwrite the status
		afterwards.  A waiter awake in between failed with "deadlock
		detected" and no cycle in the detail (#1381).

		Park the notifier right after it has let the waiter go, and wake
		the waiter with a procsignal.  The waiter must carry on waiting for
		the row, and get it once the notifier's transaction is over.  Only
		PostgreSQL 18 could fail here: before it, ProcSleep() returned the
		status read again under the partition lock, after the notifier had
		overwritten it.

		The notifier is parked holding a lock manager partition lock.  Any
		heavyweight lock that goes to the main lock table may hash to that
		partition and block: opening a connection, reading pg_locks, or
		planning a query on pg_stat_activity, which opens indexes of shared
		catalogs.  So while it is parked the test only calls functions over
		a connection opened in advance, and runs each of those queries once
		beforehand to have the caches warm.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_oxid_notify (
				id int PRIMARY KEY,
				v int
			) USING orioledb;
			INSERT INTO o_oxid_notify VALUES (1, 1);
		""")

		control = node.connect(autocommit=True)
		notifier = node.connect()
		waiter = node.connect()
		notifier.execute("SET application_name = 'oxid_notifier';")
		notifier_pid = notifier.pid
		waiter_pid = waiter.pid

		notifier.execute(
		    "SELECT id FROM o_oxid_notify WHERE id = 1 FOR KEY SHARE;")
		notifier.execute("SAVEPOINT s;")
		notifier.execute(
		    "SELECT id FROM o_oxid_notify WHERE id = 1 FOR SHARE;")

		waits_on = ("SELECT coalesce(wait_event_type, '') "
		            "FROM pg_stat_get_activity(%d);" % waiter_pid)
		parked = ("SELECT count(*) > 0 FROM pg_stopevents() "
		          "WHERE waiter_pids @> ARRAY[%d];" % notifier_pid)
		control.execute(waits_on)
		control.execute(parked)
		control.execute("SELECT pg_log_backend_memory_contexts(%d);" %
		                waiter_pid)
		control.execute("SELECT pg_stopevent_reset('oxid_notify_woke');")

		t_waiter = ThreadQueryExecutor(
		    waiter, "SELECT id FROM o_oxid_notify WHERE id = 1 FOR UPDATE;")
		t_waiter.start()
		while control.execute(waits_on)[0][0] != 'Lock':
			time.sleep(0.05)

		control.execute("SELECT pg_stopevent_set('oxid_notify_woke', "
		                "'$applicationName == \"oxid_notifier\"');")
		t_notifier = ThreadQueryExecutor(notifier, "ROLLBACK TO SAVEPOINT s;")
		t_notifier.start()
		while not control.execute(parked)[0][0]:
			time.sleep(0.05)

		# Make sure the waiter is awake and looks at its wait status while
		# the notifier is parked.  It either fails at once -- the bug -- or
		# goes on and blocks on the partition lock the notifier holds.
		control.execute("SELECT pg_log_backend_memory_contexts(%d);" %
		                waiter_pid)
		while t_waiter.is_alive():
			if control.execute(waits_on)[0][0] == 'LWLock':
				break
			time.sleep(0.05)

		control.execute("SELECT pg_stopevent_reset('oxid_notify_woke');")
		t_notifier.join()
		notifier.rollback()

		# Still holding the row, the notifier made the waiter wait again;
		# once the notifier rolls back the waiter gets the lock.
		self.assertEqual([(1, )], t_waiter.join())
		waiter.rollback()

		with open(node.pg_log_file) as f:
			self.assertNotIn("deadlock detected", f.read())

		control.close()
		notifier.close()
		waiter.close()
		node.stop()
