#!/usr/bin/env python3
# coding: utf-8

import os
import signal
import subprocess
import time

from testgres import NodeStatus

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent


class RecoveryLivelockTest(BaseTest):
	"""Crash recovery never finishes: replay spins in o_btree_modify_handle_conflicts().

	An OrioleDB transaction that also owns a heap xid writes no OrioleDB
	finish record of its own -- its verdict rides on the builtin commit
	record, and o_xact_redo_hook() turns that into a verdict for the oxid.
	But the hook can only resolve oxids that are on joint_commit_list, and an
	oxid only gets there by replaying its WAL_REC_JOINT_COMMIT.  That record
	is written at *subtransaction* commit, so it can be arbitrarily far ahead
	of the transaction's own end -- and, in particular, ahead of the
	replayStartPtr a later checkpoint pins.

	Replay that starts past the JOINT_COMMIT record therefore sees the
	transaction's later modifies (WAL_REC_XID does carry the heap xid, so the
	association is known) but never puts it on the list, so the builtin commit
	record resolves nothing.  The oxid stays COMMITSEQNO_INPROGRESS for the
	whole replay, and leftover in-flight oxids are only aborted in
	recovery_finish(), i.e. after replay -- which is what is stuck.  Any later
	record touching one of that transaction's rows then conflicts forever:
	row_lock_conflicts() reports a conflict, wait_for_oxid() returns at once
	because the owning process is long gone, refind_page() returns
	ConflictResolutionRetry, and modify.c:223 loops.  In a backend that is an
	interruptible hot loop; in the startup process it means the cluster never
	opens.

	Measured on a wedged production cluster, whose WAL reads exactly like the
	stream this test builds:

	    0/1C248B28  oxid 1162773  JOINT_COMMIT(xid=311481)
	    0/1C249B70  replayStartPtr pinned here by the next checkpoint
	    0/1C249E28  oxid 1162773  UPDATE            <- replayed, in progress
	    0/1C249EA8  builtin XACT_COMMIT xid=311481  <- resolves nothing
	    0/1C24ADE0  oxid 1162811  UPDATE, same tree <- spins forever
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "orioledb.enable_stopevents = true\n"
		    "checkpoint_timeout = 1h\n"
		    "max_wal_size = 10GB\n")

	def test_recovery_livelock_on_orphaned_joint_commit(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_livelock (\n"
		    "  id int NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_livelock SELECT g, 0 "
		    "FROM generate_series(1, 10) g;\n")
		node.safe_psql('postgres', "CHECKPOINT;")

		# B: a transaction that owns a heap xid and commits a subtransaction
		# that touched OrioleDB data.  The subtransaction commit is what emits
		# WAL_REC_JOINT_COMMIT, tying oxid to heap xid for replay.
		con_b = node.connect()
		con_b.begin()
		con_b.execute("SELECT txid_current();")
		con_b.execute("SAVEPOINT s1;")
		con_b.execute("UPDATE o_livelock SET v = v + 1 WHERE id = 1;")
		con_b.execute("RELEASE SAVEPOINT s1;")

		# Pin replayStartPtr past that JOINT_COMMIT record.  Everything before
		# it is covered by the checkpoint's page images, so replay will start
		# above it -- and will never learn that this oxid rides on a heap xid.
		node.safe_psql('postgres', "CHECKPOINT;")

		# ... and now modify again, above replayStartPtr.  This record WILL be
		# replayed, and it is what leaves an in-progress version behind.
		con_b.execute("UPDATE o_livelock SET v = v + 10 WHERE id = 1;")
		con_b.commit()
		con_b.close()

		# A: a later writer of the same row.  On the primary it simply waits
		# for B, which has already committed; in the WAL its record lands
		# after B's, which is where replay wedges.
		node.safe_psql('postgres',
		               "UPDATE o_livelock SET v = v + 100 WHERE id = 1;")

		node.stop(['-m', 'immediate'])
		try:
			node.start(params=['-t', '20'])
			opened = self.wait_until_accepting(node, timeout=20)
		except Exception:
			# pg_ctl gave up waiting: recovery is still running
			opened = self.wait_until_accepting(node, timeout=20)
		if not opened:
			self.report_stuck(node)
			# a livelocked cluster ignores a fast shutdown, and tearDown()
			# would hang on it
			self.kill_cluster(node)
		self.assertTrue(
		    opened, "cluster did not finish crash recovery: replay is "
		    "livelocked in o_btree_modify_handle_conflicts()")

		self.assertEqual(
		    node.execute("SELECT v FROM o_livelock WHERE id = 1;")[0][0], 111)

	def test_recovery_after_on_commit_drop(self):
		"""The other way an OrioleDB transaction can end up with no joint
		commit record: it acquires its oxid during commit.

		PostgreSQL runs PreCommit_on_commit_actions() after
		XACT_EVENT_PRE_COMMIT, so an ON COMMIT DROP of an OrioleDB temp table
		does its work past the point where wal_joint_commit() is written --
		and dropping the table touches the system trees, which are persistent
		and WAL logged, so this is not simply invisible to replay.

		Measured with a probe on the branch that guesses a verdict in
		recovery_finish(): nothing is left unsettled here.  The test pins that
		the cluster comes back and the transaction's other half is intact, so
		a change that starts leaving this oxid in flight shows up as a hang
		rather than silently.
		"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE h_drop (id int NOT NULL, PRIMARY KEY (id));")
		node.safe_psql('postgres', "CHECKPOINT;")

		con = node.connect()
		con.begin()
		con.execute("CREATE TEMP TABLE o_tmp (id int NOT NULL, v int,\n"
		            "  PRIMARY KEY (id)) USING orioledb ON COMMIT DROP;")
		con.execute(
		    "INSERT INTO o_tmp SELECT g, g FROM generate_series(1, 5) g;")
		con.execute("INSERT INTO h_drop VALUES (1);")
		con.commit()
		con.close()

		node.stop(['-m', 'immediate'])
		try:
			node.start(params=['-t', '20'])
			opened = self.wait_until_accepting(node, timeout=20)
		except Exception:
			opened = self.wait_until_accepting(node, timeout=20)
		if not opened:
			self.report_stuck(node)
			self.kill_cluster(node)
		self.assertTrue(opened, "cluster did not finish crash recovery")
		self.assertEqual(node.execute("SELECT count(*) FROM h_drop;")[0][0], 1)
		node.stop()

	def test_replay_conflict_with_unsettled_version(self):
		"""The same spin reached without any heap xid: a transaction that
		WAL mentions and never finishes.

		A checkpoint pins replayStartPtr and only afterwards walks the tables
		and writes their images, so a change made in between is both in the
		image and above the point replay starts from -- replay applies it a
		second time.  Park the checkpointer in that window and put two
		transactions in it.

		B opens first and writes enough rows to overflow its 8 KB local WAL
		buffer, which puts its WAL_REC_XID on the wire.  That matters: an oxid
		the checkpoint named but WAL never mentions is settled ABORTED by
		read_xids() (!state->wal_xid) and page_item_rollback() disposes of it
		on the first try.  Once WAL does mention it, recovery_map_oxid_csn()
		answers with the stored COMMITSEQNO_INPROGRESS instead, and B never
		writes a finish record.

		A then takes the contended row and commits, so its records are
		replayed; B touches the same row after A released it and never
		finishes, so B's mark is what the image carries.  Replay re-applies
		A's record onto that image and conflicts with an oxid it cannot
		settle.
		"""
		node = self.prepare_unsettled(
		    "UPDATE o_unsettled SET v = 100 WHERE id = 1;")
		self.assert_recovery_completes(node)

	def test_replay_conflict_with_unsettled_row_lock(self):
		"""Same shape, but B leaves only a row lock.

		row_lock_conflicts() drops a locker's record once it can see the
		locker finished.  One that replay cannot settle keeps its lock for the
		rest of recovery, so the conflict is reported again on every retry.
		"""
		node = self.prepare_unsettled(
		    "SELECT * FROM o_unsettled WHERE id = 1 FOR SHARE;")
		self.assert_recovery_completes(node)

	def prepare_unsettled(self, b_marks_row):
		"""Run A and B inside the checkpoint's double-apply window, then crash."""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_unsettled (\n"
		    "  id int NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_unsettled SELECT g, 0 "
		    "FROM generate_series(1, 2000) g;\n")
		node.safe_psql('postgres', "CHECKPOINT;")

		ctl = node.connect()
		ctl.execute("SELECT pg_stopevent_set('checkpoint_table_start',\n"
		            "format(E'$.table.reloid == \\045s',\n"
		            "'o_unsettled'::regclass::oid)::jsonpath);")
		ctl.commit()

		con_ck = node.connect()
		t_ck = ThreadQueryExecutor(con_ck, "CHECKPOINT;")
		t_ck.start()
		wait_checkpointer_stopevent(node)

		# replayStartPtr is pinned; o_unsettled's image is not written yet.

		# B overflows its local WAL buffer, so replay sees this oxid exists.
		con_b = node.connect()
		con_b.begin()
		con_b.execute(
		    "UPDATE o_unsettled SET v = 7 WHERE id BETWEEN 100 AND 1600;")

		# A takes the contended row and commits: its records are replayed.
		con_a = node.connect()
		con_a.begin()
		con_a.execute("UPDATE o_unsettled SET v = 1 WHERE id = 1;")
		con_a.commit()
		con_a.close()

		# B marks the row after A released it, and never finishes.
		con_b.execute(b_marks_row)

		ctl.execute("SELECT pg_stopevent_reset('checkpoint_table_start');")
		t_ck.join()
		con_ck.close()

		node.stop(['-m', 'immediate'])
		con_b.close()
		ctl.close()
		return node

	def assert_recovery_completes(self, node):
		try:
			node.start(params=['-t', '20'])
			opened = self.wait_until_accepting(node, timeout=20)
		except Exception:
			opened = self.wait_until_accepting(node, timeout=20)
		if not opened:
			self.report_stuck(node)
			self.kill_cluster(node)
		self.assertTrue(
		    opened, "cluster did not finish crash recovery: replay is "
		    "livelocked in o_btree_modify_handle_conflicts()")
		self.assertEqual(
		    node.execute("SELECT v FROM o_unsettled WHERE id = 1;")[0][0], 1)

	def wait_until_accepting(self, node, timeout):
		"""Wait for the cluster to open.

		Give up as soon as replay reports that its conflict retry is not
		converging: from there the cluster never opens, and sitting out the
		whole timeout only makes the failure slower to report.
		"""
		deadline = time.time() + timeout
		while time.time() < deadline:
			if self.saw_non_convergence(node):
				return False
			try:
				if node.status() != NodeStatus.Running:
					time.sleep(0.5)
					continue
				node.execute("SELECT 1;")
				return True
			except Exception:
				time.sleep(0.5)
		return False

	def report_stuck(self, node):
		"""Print what the startup process is doing, so a CI failure is
		actionable without a live box."""
		out = subprocess.run(["ps", "-o", "pid=,stat=,etime=,args=", "-e"],
		                     capture_output=True,
		                     text=True).stdout
		for line in out.splitlines():
			if "startup recovering" in line or "orioledb recovery" in line:
				print("STUCK:", line.strip())
		log = os.path.join(node.logs_dir, "postgresql.log")
		try:
			with open(log) as f:
				tail = f.readlines()[-25:]
			print("".join(tail))
		except Exception:
			pass
		print("data dir kept at", node.data_dir)

	def saw_non_convergence(self, node):
		log = os.path.join(node.logs_dir, "postgresql.log")
		try:
			with open(log) as f:
				return "not converging" in f.read()
		except Exception:
			return False

	def kill_cluster(self, node):
		"""Take a livelocked cluster down.

		A fast shutdown never completes -- the startup process is not at an
		interrupt point -- and an immediate one reports success while leaving
		the children running, including the recovery worker burning a core.
		So collect the children first and signal them all by hand.
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
