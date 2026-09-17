#!/usr/bin/env python3
# coding: utf-8

import contextlib
import time

from threading import Thread

from .base_test import BaseTest


class EvictStaleDescrTest(BaseTest):
	"""A writer must not reach the seq bufs of a meta page it no longer owns.

	The seq bufs a page write goes through live in the tree's meta page, and
	the writer gets there from a BTreeDescr cached in its own process.  Nothing
	keeps the tree loaded for it: the page pool can free the meta page and hand
	it to another tree, whose data then reads as a seq buf -- including a
	"spinlock" word that is really a block number and so never becomes free.
	That is issue #1113, which presented as

	    PANIC: stuck spinlock detected at perform_page_io, src/btree/io.c

	47 seconds after the write began.

	The writer here is the orioledb bgwriter, as it was in the report.  A
	client backend gets its descriptor dropped by the invalidation eviction
	sends (walk_page_evict_root() -> o_invalidate_oids()), and repaired by the
	root page change count check on its next descent; the bgwriter neither runs
	statements nor descends -- it reaches pages from the clock sweep.

	Every check on the way to a write looks at the root page, so the bug needs
	the one shape in which they all pass and the meta page still differs: the
	tree reloaded into its old root block number but a new meta block number.
	Both numbers come from the two consecutive allocations in
	init_shared_root_info(), so _build_stale_shape() freezes the pool right
	after the eviction (after_tree_evict), parks the reload between those two
	allocations (after_tree_root_page_alloc), and lets a third backend take the
	block the meta page was about to get.

	The test then makes the bgwriter write that tree's dirty page, which is
	the write that would go through the seq bufs of the meta page's new owner.

	Each of those steps depends on the page pool and on the bgwriter's mode,
	so an attempt can come up short in several ways -- counted and reported --
	and the test retries within a budget.  What it asserts is the report the
	guard writes: that message is durable and unambiguous.  Its absence is
	not, since the bgwriter may simply not have walked the page in the window,
	or have walked it with a descriptor that was refreshed meanwhile; a run
	that never sees it therefore skips rather than fails.

	That alone would only ever say that the guard works, never that the bug
	is caught: with the guard removed there is no message to miss, so the
	test would skip on exactly the code it is meant to reject.  What closes
	that gap is the assertion perform_page_io() makes about the invariant its
	callers owe it.  On an unfixed build the shape above drives the bgwriter
	straight into it -- measured here as 3 failures out of 3, in 5 to 12
	seconds -- and the write never reaches the seq bufs of a page it does not
	own.  Whether the same write would go on to hang on the fake spinlock,
	as it did in the report, depends on the bytes that landed in the block;
	the assertion does not.
	"""

	# Which block the reload is handed is the pool's decision: the eviction
	# frees more blocks than the two the tree names, and the reload takes what
	# the usage count map offers.  Each attempt checks whether the reload got
	# the root block back, and gives up on that attempt if it did not.
	ATTEMPTS = 8

	# Wall clock budget for those attempts, so that a machine on which the
	# pool never cooperates gives up with a message instead of running
	# forever, and per-step budgets within an attempt.
	BUDGET = 180
	PRIME_TIMEOUT = 15
	EXPLOIT_TIMEOUT = 15

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf',
		    "orioledb.enable_stopevents = true\n"
		    # 1024 pages, and the churn table below is several times that:
		    # small enough to drive the pool into either mode on demand.
		    "orioledb.main_buffers = 8MB\n"
		    "orioledb.bgwriter_num_workers = 1\n"
		    "checkpoint_timeout = 1h\n"
		    "max_wal_size = 4GB\n")
		self.reasons = {
		    'no_prime': 0,
		    'no_root': 0,
		    'no_evict': 0,
		    'no_grab': 0,
		    'other_root': 0,
		    'stuck': 0,
		    'built': 0
		}
		self.node.start()
		self.node.safe_psql(
		    'postgres',
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    # The victim is one page big, so its root is its only leaf: that
		    # keeps the walk out of the merge and parent-refind branches, both
		    # of which repair a stale descriptor on their own.
		    "CREATE TABLE o_victim (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_victim SELECT g, repeat('v', 100)"
		    " FROM generate_series(1, 20) g;\n"
		    "CREATE TABLE o_churn (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_churn SELECT g, repeat('c', 400)"
		    " FROM generate_series(1, 20000) g;\n"
		    "CREATE TABLE o_grab (id int NOT NULL, v text NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_grab SELECT g, repeat('g', 400)"
		    " FROM generate_series(1, 100) g;\n"
		    "CHECKPOINT;\n")

	def _log(self):
		with open(self.node.pg_log_file, errors='replace') as f:
			return f.read()

	def _caught(self, what):
		return what in self._log()

	def _crash_lines(self):
		return [
		    line for line in self._log().splitlines() if 'TRAP' in line
		    or 'terminated by signal' in line or 'stuck spinlock' in line
		]

	def _parked(self, ctrl, pid):
		return ctrl.execute("SELECT EXISTS(SELECT 1 FROM pg_stopevents()"
		                    " WHERE waiter_pids @> ARRAY[%d]);" % pid)[0][0]

	def _wait_park(self, ctrl, pid, what, timeout=60):
		deadline = time.time() + timeout
		while time.time() < deadline:
			if self._parked(ctrl, pid):
				return
			time.sleep(0.05)
		self.fail("nothing parked in %s within %d s" % (what, timeout))

	@contextlib.contextmanager
	def _armed(self, ctrl, event, condition):
		"""Hold a stop event for a block, and release it whatever happens.

		Resetting the event is the only thing that releases whoever parked at
		it, and this thread is the only one that can do the reset.  So a
		statement issued inside an armed window must not be waited on by this
		thread: if that statement is itself the one that parks, the reset is
		unreachable and nothing in the test or in the server ever breaks the
		deadlock.  Such statements go on a thread of their own, and this
		block's exit is what lets them finish.
		"""
		ctrl.execute("SELECT pg_stopevent_set('%s', '%s');" %
		             (event, condition))
		try:
			yield
		finally:
			ctrl.execute("SELECT pg_stopevent_reset('%s');" % event)

	def _detached(self, work):
		"""Start something that is allowed to park, keeping this thread free."""
		thread = Thread(target=work)
		thread.start()
		return thread

	def _wait_bgwriter_park(self, ctrl, timeout=60):
		deadline = time.time() + timeout
		while time.time() < deadline:
			pids = ctrl.execute(
			    "SELECT pid FROM pg_stat_activity"
			    " WHERE backend_type = 'orioledb background writer';")
			if pids and self._parked(ctrl, pids[0][0]):
				return pids[0][0]
			time.sleep(0.05)
		return None

	def _victim_root(self, con):
		"""Block number of the victim's only page, which is its root.

		None when the victim is not a single resident page: the pool is free
		to evict the whole tree between attempts, and it is also free to split
		it if anything grew the rows.  Neither is a failure, just an attempt
		that cannot be built on.
		"""
		pages = con.execute(
		    "SELECT blkno FROM orioledb_table_pages('o_victim'::regclass);")
		return pages[0][0] if len(pages) == 1 else None

	def _relnode(self, con):
		return con.execute("SELECT index_relnode FROM orioledb_index_oids()"
		                   " WHERE table_reloid = 'o_victim'::regclass"
		                   "   AND index_type = 'primary';")[0][0]

	def _make_bgwriter_write(self, con):
		"""Leave the pool where the bgwriter takes its write path.

		It writes when more than half the pool is dirty and evicts when less
		than a twentieth of it is free, and only the write path walks the root
		page of a one-page tree (a root eviction goes down
		walk_page_evict_root() instead).  Evicting the churn leaves frees the
		blocks, updating them dirties more than half the pool, and the victim
		is dirtied in between so that there is something of its own to write.
		"""
		con.execute("SELECT orioledb_evict_pages('o_churn'::regclass, 0);")
		con.commit()
		con.execute("UPDATE o_victim SET v = v || 'y';")
		con.commit()
		con.execute("UPDATE o_churn SET v = v || 'x' WHERE id <= 18000;")
		con.commit()

	def _build_stale_shape(self, ctrl, presser, loader, grabber, relnode,
	                       attempt, give_up_at):
		"""Leave the bgwriter holding a descriptor of a gone incarnation.

		Returns False when the attempt cannot be built on -- most often
		because the reload did not come up on the root block the bgwriter's
		descriptor names, which makes it useless: an ordinary walk repairs
		the descriptor instead.  Every such way out is counted in
		self.reasons rather than failing, since none of them says anything
		about the guard under test.
		"""
		presser_pid = presser.pid
		loader_pid = loader.pid

		# 1. Make the bgwriter write the victim's page, which is what leaves a
		#    descriptor of this incarnation behind in it.
		#
		#    Both halves of that are transient: the pool is only in the
		#    bgwriter's write mode for as long as the churn keeps it dirty,
		#    and the victim's page can be evicted by a backend's own sweep
		#    before the bgwriter's next wakeup.  So keep re-establishing both
		#    for as long as it takes the bgwriter to show up.
		#
		#    The pressure runs on a thread because the event names the tree,
		#    not the writer: the presser's own sweep may evict a victim page
		#    and park there too, and then only the reset below releases it.
		stop = []
		blew_up = []

		def press_until_stopped():
			try:
				while not stop:
					self._make_bgwriter_write(presser)
			except Exception as e:  # noqa: BLE001 - reported below
				blew_up.append(e)

		bgw_pid = None
		with self._armed(ctrl, 'after_ionum_set',
		                 "$.treeName == \"o_victim_pkey\""):
			prime = self._detached(press_until_stopped)
			deadline = min(time.time() + self.PRIME_TIMEOUT, give_up_at)
			while bgw_pid is None and time.time() < deadline and not blew_up:
				bgw_pid = self._wait_bgwriter_park(ctrl, timeout=1)
			stop.append(True)
		prime.join(timeout=self.PRIME_TIMEOUT)
		if prime.is_alive():
			# The pressure is wedged on something other than the event just
			# reset, so its connection cannot be reused for the next step.
			self.reasons['stuck'] += 1
			return False
		self.assertEqual(blew_up, [], "the pressure failed")
		presser.commit()
		if bgw_pid is None:
			self.reasons['no_prime'] += 1
			return False
		root_before = self._victim_root(ctrl)
		if root_before is None:
			self.reasons['no_root'] += 1
			return False

		# 2. Evict the victim and stop the pool right there: the blocks it
		#    gives up are the ones its next incarnation is handed.  Only a
		#    client backend may park -- freezing the bgwriter here would take
		#    away the writer the test needs.
		#
		#    The pressure is an update of the churn table, which is several
		#    times the size of the pool.  It has to be an update rather than a
		#    scan: a scan leaves the pool clean, the bgwriter then runs in its
		#    eviction mode and does all the freeing itself, and the backend
		#    never gets to run the sweep this step needs it to run.  Which page
		#    the clock hand stops on is its own business, so the update is
		#    repeated over fresh ranges until it lands on the victim.
		grabbed = False
		with self._armed(
		    ctrl, 'after_tree_evict',
		    "$.relnode == %d && $backendType == \"client backend\"" % relnode):
			press = None
			for chunk in range(6):
				if time.time() > give_up_at and chunk > 0:
					break
				lo = 1 + ((attempt * 6 + chunk) % 2) * 10000
				press = self._detached(lambda: presser.execute(
				    "UPDATE o_churn SET v = v || 'p'"
				    " WHERE id BETWEEN %d AND %d;" % (lo, lo + 9999)))
				while press.is_alive():
					if self._parked(ctrl, presser_pid):
						break
					time.sleep(0.05)
				if self._parked(ctrl, presser_pid):
					break
				press.join(timeout=120)
				if press.is_alive():
					self.reasons['stuck'] += 1
					return False
				presser.commit()
				press = None
			if press is None:
				# The sweep never stopped on the victim.  Nothing is frozen,
				# so just start over.
				self.reasons['no_evict'] += 1
				return False

			# 3. Reload the victim, parked between its root and meta page
			#    allocations, and let another backend take the block the meta
			#    page was about to get.  The rows are wide so that the block
			#    comes back full of tuple data: read as a seq buf, that is the
			#    state where the lock word is a block number that never
			#    becomes free.
			with self._armed(ctrl, 'after_tree_root_page_alloc',
			                 '$.relnode == %d' % relnode):
				load = self._detached(
				    lambda: loader.execute("SELECT count(*) FROM o_victim;"))
				self._wait_park(ctrl, loader_pid, 'after_tree_root_page_alloc')

				# Detached as well: the reload is parked holding pool state,
				# so the grab can block on it, and then only leaving this
				# block lets either of them go.
				grab = self._detached(lambda: grabber.execute(
				    "INSERT INTO o_grab SELECT g, repeat('g', 1800)"
				    " FROM generate_series(%d, %d) g;" %
				    (1000 + attempt * 100, 1050 + attempt * 100)))
				grab.join(timeout=self.EXPLOIT_TIMEOUT)
				grabbed = not grab.is_alive()
			grab.join(timeout=60)
			load.join(timeout=60)
		press.join(timeout=180)
		if grab.is_alive() or load.is_alive() or press.is_alive():
			self.reasons['stuck'] += 1
			return False
		grabber.commit()
		presser.commit()
		loader.commit()

		if not grabbed:
			# Nobody took the block while the reload was parked, so the meta
			# page is free to come back where it was.  Nothing to exploit.
			self.reasons['no_grab'] += 1
			return False

		if self._victim_root(ctrl) != root_before:
			# The pool handed the reload a different root block, so the
			# bgwriter's descriptor names no page of this tree and an
			# ordinary walk repairs it.  Nothing to exploit.
			self.reasons['other_root'] += 1
			return False

		self.reasons['built'] += 1
		return True

	def _tbl_check(self, con, table):
		"""orioledb_tbl_check(), with whatever it said about a failure.

		check_btree() reports every way it can fail as a NOTICE and returns a
		bare false.  NOTICEs do not reach the server log, so a failure that
		does not carry them says only "false".
		"""
		del con.connection.notices[:]
		ok = con.execute("SELECT orioledb_tbl_check('%s'::regclass, true);" %
		                 table)[0][0]
		con.commit()
		return ok, [line.strip() for line in con.connection.notices]

	def _check_aftermath(self):
		node = self.node
		crashed = self._crash_lines()
		self.assertEqual(
		    crashed, [], "server did not survive the stale descriptor: %s" %
		    (crashed[:1], ))

		# The tree that holds the meta page now must be intact, and the victim
		# must still be readable and writable through a repaired descriptor --
		# the write the bgwriter skipped has to be done by someone.
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_victim;")[0][0], 20)
		node.execute("UPDATE o_victim SET v = v || 'w';")
		node.execute("CHECKPOINT;")
		con = node.connect()
		try:
			for table in ('o_victim', 'o_grab', 'o_churn'):
				ok, said = self._tbl_check(con, table)
				self.assertTrue(
				    ok, "%s did not survive the write through its meta"
				    " page: %s" % (table, said))
		finally:
			con.close()
		node.restart()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_victim;")[0][0], 20)

	def _report(self, built):
		"""Report, or say which step of the construction did not come up.

		Only the report proves anything from outside the server.  Its absence
		does not: the bgwriter may not have walked the page in the window, or
		may have walked it with a descriptor that had meanwhile been
		refreshed.  So a run that never sees it skips with the counts of where
		the attempts went, rather than calling it a failure.
		"""
		print("# attempts: %s" % (self.reasons, ), flush=True)
		if built:
			return
		self.skipTest("the bgwriter never reached the page while its"
		              " descriptor was stale; attempts: %s" % (self.reasons, ))

	def _run(self, exploit, what):
		node = self.node
		ctrl = node.connect()
		presser = node.connect()
		loader = node.connect()
		grabber = node.connect()
		built = False
		try:
			relnode = self._relnode(ctrl)
			give_up_at = time.time() + self.BUDGET
			for attempt in range(self.ATTEMPTS):
				if time.time() > give_up_at:
					break
				if not self._build_stale_shape(ctrl, presser, loader, grabber,
				                               relnode, attempt, give_up_at):
					continue
				# Keep re-establishing the write mode while watching the log:
				# the report is durable, so it is the one signal that says
				# the bgwriter reached the page with its stale descriptor.
				deadline = min(time.time() + self.EXPLOIT_TIMEOUT,
				               give_up_at + self.EXPLOIT_TIMEOUT)
				while time.time() < deadline and not built:
					try:
						exploit(presser, loader)
					except Exception:  # noqa: BLE001 - diagnosed below
						# Without the guard the write goes through, and the
						# assertion at its use site takes the bgwriter down
						# with it; that reaches the test as nothing more than
						# a lost connection, so name what the server said.
						crashed = self._crash_lines()
						if crashed:
							self.fail("the write went through a foreign meta"
							          " page: %s" % (crashed[0], ))
						raise
					for _ in range(10):
						if self._caught(what):
							built = True
							break
						time.sleep(0.3)
				if built:
					break
		finally:
			for event in ('after_ionum_set', 'after_tree_evict',
			              'after_tree_root_page_alloc'):
				try:
					ctrl.execute("SELECT pg_stopevent_reset('%s');" % event)
				except Exception:  # noqa: BLE001 - teardown
					pass
			for con in (ctrl, presser, loader, grabber):
				try:
					con.close()
				except Exception:  # noqa: BLE001 - teardown
					pass
		return built

	def test_write_through_a_reassigned_meta_page(self):
		"""perform_page_io() must not write through a foreign meta page."""

		def exploit(presser, loader):
			# The victim is back on the root block the bgwriter's descriptor
			# names, with a meta page that went to another tree.  Give the
			# bgwriter that page to write.
			self._make_bgwriter_write(presser)

		built = self._run(exploit, "skipped writing page")
		self._check_aftermath()
		self._report(built)
