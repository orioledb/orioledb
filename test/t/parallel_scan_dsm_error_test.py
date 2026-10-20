#!/usr/bin/env python3
# coding: utf-8

import os
import signal
import subprocess
import time

from testgres.enums import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class ParallelScanDsmErrorTest(BaseTest):
	"""An error while the scan leader sets up a parallel seq scan must only
	fail the query.

	Every participant of a parallel seq scan takes a worker number under
	poscan->workerStart.  Number 0, the scan leader, then registers the scan
	against the tree's checkpoint and creates the DSM segment for on-disk
	downlinks.  dsm_create() reports most of its failures by raising an error
	from inside rather than by returning NULL -- "could not resize shared memory
	segment ...: No space left on device" when /dev/shm is full is the common
	one.  If that error is raised while workerStart is held, the spinlock is
	never released, every participant that has not joined yet spins on it, and
	after a couple of minutes one of them reports "stuck spinlock detected at
	init_btree_seq_scan" and the postmaster restarts the instance.

	The seq_scan_dsm_create_fail stop event raises an error where dsm_create()
	is called.  If the error lands before the other workers try to join, the
	group is torn down while they are still starting up and the query ends
	cleanly whatever the code does.  So every worker is first parked at
	seq_scan_parallel_join, just before it takes its number, and the stop
	event is reset once all of them are seen waiting there.  The first one to
	take a number becomes the scan leader and hits the error.

	The leader is kept out of the scan so that the victims are workers, which is
	where the PANIC was seen; the leader is left waiting for them in
	IPC/BgworkerShutdown.
	"""

	ROWS = 20000
	WORKERS = 3
	TREE = 'o_parallel_dsm_pkey'

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "orioledb.enable_stopevents = true\n"
		    "max_worker_processes = 16\n"
		    "max_parallel_workers = 8\n"
		    "max_parallel_workers_per_gather = %d\n"
		    "parallel_setup_cost = 0\n"
		    "parallel_tuple_cost = 0\n"
		    "min_parallel_table_scan_size = 0\n"
		    "parallel_leader_participation = off\n" % self.WORKERS)

	def test_parallel_scan_dsm_error_ends_the_query(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_parallel_dsm (\n"
		    "  id int NOT NULL,\n"
		    "  val text NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_parallel_dsm SELECT g, g || 'val' "
		    "FROM generate_series(1, %d) g;\n" % self.ROWS)

		plan = node.execute("EXPLAIN (COSTS OFF) "
		                    "SELECT count(*) FROM o_parallel_dsm;")
		self.assertIn("Parallel Seq Scan", "\n".join(row[0] for row in plan))
		# A crash restart replaces the checkpointer; the postmaster start time
		# would not tell, since the postmaster itself survives it.
		checkpointer = self.checkpointer_pid(node)

		con_ev = node.connect()
		con_ev.execute("SELECT pg_stopevent_set('seq_scan_parallel_join', "
		               "'$.treeName == \"%s\"');" % self.TREE)

		con = node.connect()
		t = ThreadQueryExecutor(con, "SELECT count(*) FROM o_parallel_dsm;")
		t.start()

		self.assertTrue(self.wait_for_parked(node, self.WORKERS, timeout=60),
		                "the workers never reached seq_scan_parallel_join")

		con_ev.execute("SELECT pg_stopevent_set('seq_scan_dsm_create_fail', "
		               "'$.treeName == \"%s\"');" % self.TREE)
		con_ev.execute("SELECT pg_stopevent_reset('seq_scan_parallel_join');")

		caught = None
		try:
			t.join(30)
		except Exception as e:
			caught = e

		if t.is_alive():
			self.report_stuck(node)
			self.kill_cluster(node)
			self.fail("the parallel scan did not end after the workers "
			          "were released")

		self.assertIsNotNone(caught,
		                     "the injected error did not reach the client")
		self.assertIn("parallel scan DSM creation failed", str(caught))

		con_ev.execute(
		    "SELECT pg_stopevent_reset('seq_scan_dsm_create_fail');")

		left = self.wait_for_no_workers(node, timeout=30)
		self.assertEqual(left, 0,
		                 "parallel workers outlived the query that failed")

		con.rollback()
		con.close()
		con_ev.close()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_parallel_dsm;")[0][0],
		    self.ROWS)
		self.assertEqual(self.checkpointer_pid(node), checkpointer,
		                 "the instance went through a crash restart")
		node.stop()

	def checkpointer_pid(self, node):
		return node.execute("SELECT pid FROM pg_stat_activity "
		                    "WHERE backend_type = 'checkpointer';")[0][0]

	def wait_for_parked(self, node, count, timeout):
		deadline = time.time() + timeout
		while time.time() < deadline:
			n = node.execute("SELECT coalesce(cardinality(waiter_pids), 0) "
			                 "FROM pg_stopevents() "
			                 "WHERE stopevent = 'seq_scan_parallel_join';")
			if n and n[0][0] >= count:
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
		"""Print this node's parallel workers, so a CI failure is actionable
		without a live box."""
		pid = self.postmaster_pid(node)
		out = subprocess.run(
		    ["ps", "-o", "pid=,ppid=,stat=,pcpu=,etime=,args=", "-e"],
		    capture_output=True,
		    text=True).stdout
		for line in out.splitlines():
			fields = line.split()
			if (len(fields) > 1 and fields[1] == str(pid)
			    and "parallel worker" in line):
				print("STUCK:", line.strip())
		print("data dir kept at", node.data_dir)

	def postmaster_pid(self, node):
		try:
			with open(os.path.join(node.data_dir, "postmaster.pid")) as f:
				return int(f.readline().strip())
		except Exception:
			return None

	def kill_cluster(self, node):
		"""A fast shutdown waits for workers that are past every interrupt
		point, so take the postmaster and its children down by hand."""
		pid = self.postmaster_pid(node)
		if pid is None:
			return

		out = subprocess.run(["ps", "-eo", "pid=,ppid="],
		                     capture_output=True,
		                     text=True).stdout
		victims = [pid]
		for line in out.splitlines():
			fields = line.split()
			if len(fields) == 2 and fields[1] == str(pid):
				victims.append(int(fields[0]))
		for victim in victims:
			try:
				os.kill(victim, signal.SIGKILL)
			except OSError:
				pass

		deadline = time.time() + 30
		while node.status() == NodeStatus.Running and time.time() < deadline:
			time.sleep(0.5)
		node.is_started = False
