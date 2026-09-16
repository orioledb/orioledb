#!/usr/bin/env python3
# coding: utf-8

import os
import signal
import time

from .base_test import BaseTest


class RecoveryCollationTest(BaseTest):
	"""Replay must not read a catalog while it holds a page lock.

	A comparison function for a collatable type asks pg_locale.c for the
	collation, and pg_locale.c reads pg_collation the first time a process uses
	it.  In recovery that read is answered from OrioleDB's own system trees,
	which can mean reading a page -- and the comparison happens inside
	find_page(), on a leaf page locked for modification, where reserving a page
	is forbidden:

	    TRAP: failed Assert("!have_locked_pages()"), src/utils/page_pool.c:182

	Recovery then fails, the postmaster shuts down "due to startup process
	failure", and since the next start replays the same record, the cluster is
	stuck in a crash loop (issue ORI-328).

	Three conditions have to line up, and each one is arranged below:

	* The key's collation is neither the default nor C, so the comparison needs
	  a locale at all.
	* The first record replayed for the table is a DELETE or an UPDATE.  An
	  INSERT is safe: apply_tbl_insert() compares each key bound once before
	  o_btree_modify(), with no page locked.
	* The tree is one page, so the first comparison of the replay is the one on
	  the locked leaf.  In a taller tree find_page() first compares on internal
	  pages, which it reads without a lock, and that resolves the collation
	  early by accident.

	And the process doing the replay must have no database of its own, which is
	single-process recovery -- what the postmaster runs after a backend
	crashes.  Recovery workers are connected to a database, take the sort
	support path when building the comparator, and so resolve the collation
	while no page is locked.
	"""

	# Crash recovery of twenty rows is instant; this is only a bound on the
	# machine, not on the work.
	CRASH_TIMEOUT = 120

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf',
		    # The postmaster has to come back after the backend crash below,
		    # which is what makes the recovery single-process.  BaseTest turns
		    # this off.
		    "restart_after_crash = on\n"
		    "checkpoint_timeout = 1h\n"
		    "max_wal_size = 4GB\n")

	def _log(self, since=0):
		with open(self.node.pg_log_file, errors='replace') as f:
			f.seek(since)
			return f.read()

	def _wait_log(self, text, since, what):
		deadline = time.time() + self.CRASH_TIMEOUT
		while time.time() < deadline:
			if text in self._log(since):
				return
			time.sleep(0.1)
		self.fail("%s: %r never appeared in the log within %d s" %
		          (what, text, self.CRASH_TIMEOUT))

	def _crash_a_backend(self):
		"""Kill one backend so the postmaster reinitializes.

		That is what makes the following recovery single-process: OrioleDB
		reads IsFatalError() and reports "Unable to make multiprocess
		recovery".

		Waiting for a connection to succeed is not enough to know the crash
		has been dealt with.  For the first milliseconds the postmaster has
		not reaped the dead child yet, and a connection made in that window
		is served by the old, still-running set of backends -- a CI cell
		caught exactly that, with the wait's own "SELECT 1" logged one
		millisecond *before* the postmaster noticed.  So wait for the
		postmaster to say what it is doing, and only then for the node.

		Returns the log offset the crash starts at, so that what is checked
		afterwards is this crash's recovery and not an earlier one.
		"""
		since = os.path.getsize(self.node.pg_log_file)
		with self.node.connect() as victim:
			os.kill(victim.pid, signal.SIGKILL)

		self._wait_log("terminating any other active server processes", since,
		               "the postmaster did not notice the killed backend")
		self._wait_log("database system is ready to accept connections", since,
		               "the node did not come back after the crash")

		deadline = time.time() + self.CRASH_TIMEOUT
		while time.time() < deadline:
			try:
				self.node.execute("SELECT 1;")
				return since
			except Exception:  # noqa: BLE001 - the node is still restarting
				time.sleep(0.2)
		self.fail("the node did not accept connections after the crash")

	def _check_recovered(self, expected, since):
		log = self._log(since)
		self.assertIn(
		    "Unable to make multiprocess recovery", log,
		    "recovery was not single-process, so the descriptor was built"
		    " with a database and the collation got resolved early")
		trapped = [
		    line for line in log.splitlines() if 'TRAP' in line
		    or 'startup process' in line and 'terminated' in line
		]
		self.assertEqual(
		    trapped, [],
		    "recovery did not survive the replay: %s" % (trapped[:2], ))
		self.assertEqual(
		    self.node.execute("SELECT count(*) FROM o_coll;")[0][0], expected)
		self.assertTrue(
		    self.node.execute("SELECT orioledb_tbl_check('o_coll'::regclass);")
		    [0][0])

	def _prepare(self):
		self.node.start()
		self.node.safe_psql(
		    'postgres',
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    # One page of rows: the first comparison of the replay is then the
		    # one on the locked leaf.
		    "CREATE TABLE o_coll (\n"
		    "  k text COLLATE \"unicode\" NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  PRIMARY KEY (k)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_coll SELECT md5(g::text), g"
		    " FROM generate_series(1, 20) g;\n"
		    # Everything so far is checkpointed, so the replay below starts
		    # with the statement that follows.
		    "CHECKPOINT;\n")

	def test_first_replayed_record_is_a_delete(self):
		self._prepare()
		self.node.safe_psql('postgres', "DELETE FROM o_coll WHERE v = 7;")
		since = self._crash_a_backend()
		self._check_recovered(19, since)

	def test_first_replayed_record_is_an_update(self):
		self._prepare()
		self.node.safe_psql('postgres',
		                    "UPDATE o_coll SET v = v + 100 WHERE v = 7;")
		since = self._crash_a_backend()
		self._check_recovered(20, since)
		self.assertEqual(
		    self.node.execute("SELECT count(*) FROM o_coll WHERE v = 107;")[0]
		    [0], 1)
