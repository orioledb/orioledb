#!/usr/bin/env python3
# coding: utf-8

import contextlib
import time

from threading import Thread

from .base_test import BaseTest


class EvictSeqScanRaceTest(BaseTest):
	"""A sequential scan must not start on a tree that is being evicted.

	evict_btree() asks twice whether anyone is reading the tree: once to
	decline the eviction, and once, ~70 lines later, as an assertion that
	nobody appeared meanwhile.  Between the two it writes the root page and
	frees it, with the page unlocked for the whole of that.

	On a primary nobody can appear: get_evict_btree_locks() holds an
	ordinary AccessExclusiveLock on the relation, so a reader does not get
	past relation_open().  On a standby that lock is not taken --

	    if (!recovery && !(state->indexRegularLock = ...))

	-- and the scan side asks for nothing in its place: relation_get_descr()
	answers out of rel->rd_amcache, and init_checkpoit_number() counts the
	scan into the meta page under no lock at all.  So a read-only query on a
	standby walks straight into the gap, which is issue #1133:

	    TRAP: meta_page_get_num_seq_scans(desc->rootInfo.metaPageBlkno) == 0
	          src/btree/io.c, in the standby's bgwriter

	The scan the assertion catches is a real one: the tree whose root page
	it is reading is about to be freed and taken out of shared memory.
	"""

	# How long to wait for an eviction of the victim's root to reach the
	# gap.  Generous, because a valgrind cell runs everything twenty times
	# slower; the test gives up with a skip rather than a failure when it
	# expires, since an eviction that never came tested nothing.
	PARK_TIMEOUT = 300

	def setUp(self):
		self.startTime = time.time()
		self.node = self.initNode(self.getBasePort(),
		                          suffix="tgsn",
		                          allows_streaming=True)

	@contextlib.contextmanager
	def _armed(self, ctrl, event, condition):
		"""Hold a stop event for a block, and release it whatever happens.

		Resetting the event is the only thing that frees whoever parked at
		it.  Leave that on the success path and a run that gives up early --
		under valgrind, where everything takes twenty times longer, that is
		the likely run -- leaves the event armed, the next process to reach
		it parks for good, and the node never shuts down.  The cell then
		dies on its own timeout with nothing to show for it, which is what
		this test did on its first valgrind outing.
		"""
		ctrl.execute("SELECT pg_stopevent_set('%s', '%s');" % (event, condition))
		try:
			yield
		finally:
			ctrl.execute("SELECT pg_stopevent_reset('%s');" % event)

	def _parked(self, con):
		rows = con.execute("SELECT waiter_pids FROM pg_stopevents()"
		                   " WHERE stopevent = 'after_tree_root_page_write';")
		return rows[0][0] if rows and rows[0][0] else None

	def test_seq_scan_starts_while_the_tree_is_being_evicted(self):
		master = self.node
		master.start()
		master.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_victim (id int NOT NULL, v text NOT NULL,
				PRIMARY KEY (id)) USING orioledb;
			INSERT INTO o_victim SELECT g, repeat('v', 100)
				FROM generate_series(1, 20) g;
			CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,
				PRIMARY KEY (id)) USING orioledb;
			INSERT INTO o_churn SELECT g, repeat('c', 400)
				FROM generate_series(1, 20000) g;
			CHECKPOINT;
		""")

		with self.getReplica() as replica:
			replica.append_conf(
			    'postgresql.conf', "orioledb.enable_stopevents = true\n"
			    # Small enough that replaying the churn starves it, which is
			    # what puts the replica's bgwriter on its eviction path.
			    "orioledb.main_buffers = 8MB\n"
			    "orioledb.bgwriter_num_workers = 1\n"
			    "checkpoint_timeout = 1h\n"
			    "max_wal_size = 4GB\n")
			replica.start()
			self.catchup_orioledb(replica)

			ctrl = replica.connect()
			reader = replica.connect()

			# One scan to leave the descriptor in rel->rd_amcache.  Without
			# it the scan below would have to build a descriptor, and that
			# does take a lock the evictor holds -- on a standby only that
			# one path does.
			reader.execute("SET enable_indexscan = off;"
			               " SET enable_bitmapscan = off;")
			reader.execute("SELECT count(*) FROM o_victim;")
			reader.commit()

			# Pressure goes on the master; the replica's pool fills as its
			# recovery workers replay it.  Nothing touches the victim: its
			# usage count has to decay for the clock sweep to pick its page.
			stop = []
			press_failed = []
			scan_failed = []
			parked = None

			def press():
				con = master.connect()
				try:
					while not stop:
						con.execute("UPDATE o_churn SET v = v || 'x'"
						            " WHERE id <= 18000;")
						con.commit()
				except Exception as e:  # noqa: BLE001 - reported below
					press_failed.append(e)
				finally:
					try:
						con.close()
					except Exception:  # noqa: BLE001 - teardown
						pass

			def scan():
				try:
					reader.execute("BEGIN;")
					reader.execute("DECLARE c NO SCROLL CURSOR FOR"
					               " SELECT id FROM o_victim;")
					reader.execute("FETCH 1 FROM c;")
				except Exception as e:  # noqa: BLE001 - reported below
					scan_failed.append(e)

			presser = Thread(target=press)
			scanner = None
			with self._armed(ctrl, 'after_tree_root_page_write',
			                 "$.treeName == \"o_victim_pkey\""):
				presser.start()
				try:
					deadline = time.time() + self.PARK_TIMEOUT
					while time.time() < deadline and not press_failed:
						parked = self._parked(ctrl)
						if parked:
							break
						time.sleep(0.05)
				finally:
					stop.append(True)

				if parked:
					# In the gap: register a sequential scan on the tree
					# being evicted, and keep it registered -- a scan that
					# finishes also counts itself back out, and the evictor
					# would see the zero it expects.  Detached, because a
					# build of the descriptor would block and only leaving
					# this block could free it.
					scanner = Thread(target=scan)
					scanner.start()
					scanner.join(timeout=30)

			presser.join(timeout=self.PARK_TIMEOUT)
			if scanner is not None:
				scanner.join(timeout=60)
			time.sleep(3)

			print("# parked: %s, presser: %s, scan: %s" %
			      (parked, press_failed[:1], scan_failed[:1]),
			      flush=True)

			with open(replica.pg_log_file, errors='replace') as f:
				log = f.read()
			crashed = [
			    line for line in log.splitlines()
			    if 'TRAP' in line or 'terminated by signal' in line
			]
			print("# crash: %s" % (crashed[:1], ), flush=True)
			self.assertEqual(
			    crashed, [], "a sequential scan started on a tree being"
			    " evicted: %s" % (crashed[:1], ))

			if not parked:
				# Nothing reached the gap, so nothing was tested.  Absence
				# of the crash says nothing here, and saying so is better
				# than a green tick that means "the eviction never came".
				self.skipTest(
				    "no eviction of o_victim's root reached the gap within"
				    " %d s; presser: %s" % (self.PARK_TIMEOUT,
				                            press_failed[:1]))