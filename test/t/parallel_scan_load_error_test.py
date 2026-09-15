#!/usr/bin/env python3
# coding: utf-8

import os
import signal
import subprocess
import time

from testgres.enums import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class ParallelScanLoadErrorTest(BaseTest):
	"""An error inside a parallel seq scan's page load wedges the whole group.

	The participants of a parallel seq scan take turns loading the shared
	internal page: whoever finds the slot invalid marks it in progress under
	poscan->intpageAccess, takes poscan->intpageLoad exclusively, and fills
	the shared image.  Everybody else sees the slot in progress, waits on
	intpageLoad, and re-reads the slot once the loader is done.

	An error unwinding out of the load breaks that handover.  LWLockReleaseAll
	drops intpageLoad at abort, but the slot stays in progress, so the waiters
	are left in:

	    else if (curPage->status == OParallelScanPageInProgress)
	    {
	        SpinLockRelease(&poscan->intpageAccess);
	        if (LWLockAcquireOrWait(&poscan->intpageLoad, LW_EXCLUSIVE))
	            LWLockRelease(&poscan->intpageLoad);
	        continue;
	    }

	With the lock free, LWLockAcquireOrWait takes it without ever waiting, so
	this spins on the CPU and never reaches an interrupt point.  The error is
	meant to take the whole parallel group down -- that is what any error in a
	participant does -- but the survivors never see the signal.  The query
	never ends, cancelling it does nothing, and a fast shutdown reports
	"server does not shut down" because the postmaster waits for backends
	that are past every CHECK_FOR_INTERRUPTS.  Seen in the wild on a stand
	where a worker died of an error inside the load, with the survivors left
	at 99.9% CPU and wchan 0.

	The seq_scan_load_internal_page_fail stop event raises an error in exactly
	that place, so the group is left in exactly that state.  Ordering matters
	and cannot be left to luck: if the error lands before the others have
	queued behind the slot, the leader terminates them while they are still
	starting up and everything ends cleanly -- the bug simply does not
	trigger.  So the loader is first parked at seq_scan_load_internal_page,
	which holds intpageLoad and the slot, and the error is armed only once the
	other workers are seen waiting on that lock.
	"""

	# Enough for several level-1 pages, so that the walk prefetches the next
	# one -- seq_scan_load_internal_page only fires for a load that steps by
	# a key, which the very first load of a walk does not.
	ROWS = 300000
	LOAD_LOCK = 'OBTreeScanPageLoadTrancheId'

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf',
		    "orioledb.enable_stopevents = true\n"
		    "max_worker_processes = 16\n"
		    "max_parallel_workers = 8\n"
		    "max_parallel_workers_per_gather = 3\n"
		    "parallel_setup_cost = 0\n"
		    "parallel_tuple_cost = 0\n"
		    "min_parallel_table_scan_size = 0\n"
		    # The victim has to be a worker, not the leader: an error in the
		    # leader unwinds the Gather node itself, which is not the case
		    # this is about.  With the leader out of the scan it is a worker
		    # that reaches the page load first, and the leader is left
		    # waiting for participants that no longer end.
		    "parallel_leader_participation = off\n")

	def test_parallel_scan_load_error_ends_the_query(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_parallel (\n"
		    "  id int NOT NULL,\n"
		    "  val text NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_parallel SELECT g, g || 'val' "
		    "FROM generate_series(1, %d) g;\n" % self.ROWS)

		# The tree has to have an internal level: with a single leaf page the
		# scan hands that page out directly and never touches the shared slot
		# this is about.
		plan = node.execute("EXPLAIN (COSTS OFF) "
		                    "SELECT count(*) FROM o_parallel;")
		self.assertIn("Parallel Seq Scan", "\n".join(row[0] for row in plan))

		con_ev = node.connect()
		con_ev.execute(
		    "SELECT pg_stopevent_set('seq_scan_load_internal_page', "
		    "'$.treeName == \"o_parallel_pkey\"');")

		con = node.connect()
		t = ThreadQueryExecutor(con, "SELECT count(*) FROM o_parallel;")
		t.start()

		# One worker is parked in the load with the slot marked and the lock
		# held; wait for the others to drain the current page and queue up
		# behind it.  That is the state an error has to arrive in.
		self.assertTrue(
		    self.wait_for_waiters(node, 2, timeout=60),
		    "the other workers never queued behind the page being loaded")

		con_ev.execute(
		    "SELECT pg_stopevent_set('seq_scan_load_internal_page_fail', "
		    "'$.treeName == \"o_parallel_pkey\"');")
		con_ev.execute(
		    "SELECT pg_stopevent_reset('seq_scan_load_internal_page');")

		caught = None
		try:
			t.join(30)
		except Exception as e:
			caught = e

		if t.is_alive():
			self.report_stuck(node)
			# A spinning participant is past every interrupt point, so a fast
			# shutdown never completes and tearDown() would hang on it.
			self.kill_cluster(node)
			self.fail("the parallel scan did not end: its participants are "
			          "spinning on a page slot left in progress")

		self.assertIsNotNone(caught, "the injected error did not reach the "
		                     "client")
		self.assertIn("internal page load failed", str(caught))

		con_ev.execute(
		    "SELECT pg_stopevent_reset('seq_scan_load_internal_page_fail');")

		# Nobody may be left behind burning a core: the shutdown below would
		# never complete if somebody were.
		left = self.wait_for_no_workers(node, timeout=30)
		self.assertEqual(left, 0,
		                 "parallel workers outlived the query that failed")

		con.rollback()
		con.close()
		con_ev.close()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_parallel;")[0][0], self.ROWS)
		node.stop()

	def wait_for_waiters(self, node, count, timeout):
		"""Wait for `count` workers to be blocked on the page load lock."""
		deadline = time.time() + timeout
		while time.time() < deadline:
			if node.execute("SELECT count(*) FROM pg_stat_activity "
			                "WHERE backend_type = 'parallel worker' "
			                "  AND wait_event = '%s';" %
			                self.LOAD_LOCK)[0][0] >= count:
				return True
			time.sleep(0.1)
		return False

	def wait_for_no_workers(self, node, timeout):
		deadline = time.time() + timeout
		while True:
			left = node.execute(
			    "SELECT count(*) FROM pg_stat_activity "
			    "WHERE backend_type = 'parallel worker';")[0][0]
			if left == 0 or time.time() > deadline:
				return left
			time.sleep(0.5)

	def report_stuck(self, node):
		"""Print what the participants are doing, so a CI failure is
		actionable without a live box."""
		out = subprocess.run(
		    ["ps", "-o", "pid=,stat=,pcpu=,etime=,args=", "-e"],
		    capture_output=True,
		    text=True).stdout
		for line in out.splitlines():
			if "parallel worker" in line:
				print("STUCK:", line.strip())
		try:
			print(
			    node.execute(
			        "SELECT pid, backend_type, state, wait_event_type,"
			        " wait_event FROM pg_stat_activity"
			        " WHERE state = 'active';"))
		except Exception:
			pass
		print("data dir kept at", node.data_dir)

	def kill_cluster(self, node):
		"""Take the cluster down by hand.

		A fast shutdown never completes and an immediate one reports success
		while leaving the spinning children behind, so collect them and
		signal them all.
		"""
		try:
			with open(os.path.join(node.data_dir, "postmaster.pid")) as f:
				pid = int(f.readline().strip())
		except Exception:
			return

		victims = [pid] + self.child_pids(pid)
		for sig in (signal.SIGQUIT, signal.SIGKILL):
			for victim in victims:
				try:
					os.kill(victim, sig)
				except OSError:
					pass
			time.sleep(1)

		deadline = time.time() + 30
		while node.status() == NodeStatus.Running and time.time() < deadline:
			time.sleep(0.5)
		node.is_started = False

	def child_pids(self, pid):
		out = subprocess.run(["ps", "-eo", "pid=,ppid="],
		                     capture_output=True,
		                     text=True).stdout
		kids = []
		for line in out.splitlines():
			fields = line.split()
			if len(fields) == 2 and fields[1] == str(pid):
				kids.append(int(fields[0]))
		return kids
