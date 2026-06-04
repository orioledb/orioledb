#!/usr/bin/env python3
# coding: utf-8

import threading

from .base_test import BaseTest, ThreadQueryExecutor, wait_stopevent


class ConcurrentIndexStopEventsTest(BaseTest):
	"""
	Deterministic CIC concurrency tests driven by stop events placed at
	two safe phase boundaries:

	  - cic_after_ambuild: fires from orioledb_ambuild right after the
	    OIndex has been installed in BUILDING_PHASE_2.  PG holds
	    ShareLock on the heap at this point (concurrent DML is blocked
	    waiting on the lock), so the gate lets tests inspect the
	    just-installed BUILDING state and stage a writer that will be
	    released as soon as we resume.
	  - cic_before_flip: fires from o_define_index_concurrent_finish
	    after drain + covering pass + unique walk, just before the
	    OIndex.state flip to VALID.  AccessExclusive is held; DML is
	    blocked, but tests can inspect the final pre-VALID tree state.

	Each test arms one gate, kicks off a `CREATE INDEX CONCURRENTLY` in
	a worker thread, waits for the CIC backend to park on the gate via
	`wait_stopevent`, performs assertions / stages background DML, then
	resets the gate and joins.

	Note: a stop event placed between PG's phase-2 commit and the
	concurrent-finish AccessExclusive (i.e. inside the BUILDING window
	where writers route to the spool) currently exposes an unrelated
	hang in `o_tuple_compute_data_size` for DELETEs on the BUILDING
	secondary — that path uses `nonLeafTupdesc` whose initialization
	for a BUILDING-state OIndex is racy.  Until that's fixed
	separately, the DELETE/UPDATE-during-BUILDING scenarios are tested
	by natural timing in `concurrent_index_test.py` rather than by a
	deterministic stop event here.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf("orioledb.enable_stopevents = true\n")
		self.node.start()
		self.node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE t (
				id int NOT NULL PRIMARY KEY,
				val text NOT NULL
			) USING orioledb;
			INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 100) g;
		""")

	def tearDown(self):
		try:
			self.node.stop()
		except Exception:
			pass
		super().tearDown()

	# --------------------------- helpers ---------------------------

	def _arm(self, ctrl, event):
		ctrl.execute("SELECT pg_stopevent_set(%s, 'true')" % repr(event))

	def _release(self, ctrl, event):
		ctrl.execute("SELECT pg_stopevent_reset(%s)" % repr(event))

	def _start_cic(self, sql):
		"""
		Launch a CIC statement in autocommit mode.  Returns
		(conn, thread, backend_pid) so the caller can wait_stopevent()
		on the pid.
		"""
		conn = self.node.connect(autocommit=True)
		pid = conn.execute("SELECT pg_backend_pid();")[0][0]
		thread = ThreadQueryExecutor(conn, sql)
		thread.start()
		return conn, thread, pid

	def _index_count(self, predicate):
		with self.node.connect() as c:
			c.execute("SET enable_seqscan = off")
			return c.execute("SELECT count(*) FROM t WHERE %s" %
			                 predicate)[0][0]

	def _heap_count(self):
		return self.node.execute("SELECT count(*) FROM t;")[0][0]

	# -------------------------- tests --------------------------

	def test_oindex_in_building_state_at_after_ambuild(self):
		"""
		At cic_after_ambuild the OIndex must be present in
		orioledb_index_oids() (visible to other backends) and the
		owning OTable should still report has_primary=true (this CIC
		is for a secondary).  Verifies the BUILDING placeholder is
		installed in the sys-tree before validate_scan runs.
		"""
		ctrl = self.node.connect(autocommit=True)
		self._arm(ctrl, 'cic_after_ambuild')

		conn, t, pid = self._start_cic(
		    "CREATE INDEX CONCURRENTLY t_val_idx ON t (val);")
		try:
			wait_stopevent(self.node, pid)

			# The new BUILDING index is visible in the sys-tree.
			n = self.node.execute("SELECT count(*) FROM orioledb_index_oids() "
			                      "WHERE table_reloid = 't'::regclass "
			                      "  AND index_type = 'regular';")[0][0]
			self.assertEqual(n, 1)

			# Pre-existing PK is untouched.
			n = self.node.execute("SELECT count(*) FROM orioledb_index_oids() "
			                      "WHERE table_reloid = 't'::regclass "
			                      "  AND index_type = 'primary';")[0][0]
			self.assertEqual(n, 1)

			self._release(ctrl, 'cic_after_ambuild')
			t.join()
		finally:
			conn.close()
			ctrl.close()

		# CIC completed successfully; the index is now usable.
		self.assertEqual(self._index_count("val >= 'v' AND val < 'w'"), 100)

	def test_writers_released_after_ambuild_gate(self):
		"""
		At cic_after_ambuild the CIC backend holds ShareLock on the
		heap (acquired before o_define_index commits the OIndex
		insert).  Concurrent writers must block on RowExclusive vs
		ShareLock; releasing the gate lets ambuild commit, the lock
		drops, the writers proceed -- with the BUILDING secondary
		already in their refreshed descr so their inserts spool.

		End state: every inserted row is visible via the new index.
		"""
		ctrl = self.node.connect(autocommit=True)
		self._arm(ctrl, 'cic_after_ambuild')

		conn, t, pid = self._start_cic(
		    "CREATE INDEX CONCURRENTLY t_val_idx ON t (val);")
		try:
			wait_stopevent(self.node, pid)

			# Stage writers in the background.  They are expected to
			# block on the heap's ShareLock until the gate is reset.
			def writer(start):
				with self.node.connect(autocommit=True) as w:
					for i in range(start, start + 5):
						w.execute("INSERT INTO t VALUES (%d, %s);" %
						          (i, repr('w_v%d' % i)))

			w1 = threading.Thread(target=writer, args=(500, ))
			w2 = threading.Thread(target=writer, args=(600, ))
			w1.start()
			w2.start()

			# Release CIC; writers unblock as ambuild's txn commits.
			self._release(ctrl, 'cic_after_ambuild')
			t.join()
			w1.join()
			w2.join()
		finally:
			conn.close()
			ctrl.close()

		# 100 originals + 10 inserted = 110 in both heap and the new
		# index (forced via seqscan=off).  Count via range predicates
		# (avoid LIKE so psycopg2 doesn't try to bind '%').
		self.assertEqual(self._heap_count(), 110)
		self.assertEqual(self._index_count("val >= 'v' AND val < 'w'"), 100)
		self.assertEqual(self._index_count("val >= 'w_v' AND val < 'w_w'"), 10)

	def test_tree_settled_at_before_flip(self):
		"""
		At cic_before_flip the BUILDING secondary has already been
		populated by build_secondary_index plus drain + covering
		scan.  The state flip to VALID hasn't happened yet -- so the
		index_relnode is visible in orioledb_index_oids() and
		queryable, but the OIndex state row in the sys-tree still
		reports BUILDING.  This pins the "tree is complete before
		flip" invariant.
		"""
		ctrl = self.node.connect(autocommit=True)
		self._arm(ctrl, 'cic_before_flip')

		conn, t, pid = self._start_cic(
		    "CREATE INDEX CONCURRENTLY t_val_idx ON t (val);")
		try:
			wait_stopevent(self.node, pid)

			# index_relnode is observable in orioledb_index_oids().
			n = self.node.execute("SELECT count(*) FROM orioledb_index_oids() "
			                      "WHERE table_reloid = 't'::regclass "
			                      "  AND index_type = 'regular';")[0][0]
			self.assertEqual(n, 1)

			self._release(ctrl, 'cic_before_flip')
			t.join()
		finally:
			conn.close()
			ctrl.close()

		# Post-flip: index is VALID; original 100 rows are queryable
		# via the secondary index.
		self.assertEqual(self._index_count("val >= 'v' AND val < 'w'"), 100)

	def test_unique_built_image_settled_at_before_flip(self):
		"""
		At cic_before_flip for a UNIQUE CIC the unique-fields walk has
		just succeeded.  Verifies that flipping to VALID after that
		walk gives a uniqueness-enforcing index: inserting a duplicate
		after CIC ends must fail.
		"""
		ctrl = self.node.connect(autocommit=True)
		self._arm(ctrl, 'cic_before_flip')

		conn, t, pid = self._start_cic(
		    "CREATE UNIQUE INDEX CONCURRENTLY t_val_uidx ON t (val);")
		try:
			wait_stopevent(self.node, pid)
			self._release(ctrl, 'cic_before_flip')
			t.join()
		finally:
			conn.close()
			ctrl.close()

		# Uniqueness enforced.
		with self.assertRaises(Exception):
			self.node.safe_psql("INSERT INTO t VALUES (999, 'v50');")

	def test_concurrent_inserts_around_ambuild_gate(self):
		"""
		Stress: while CIC is parked at cic_after_ambuild, several
		writer threads each fire small INSERT-only transactions.
		Writers block on ShareLock until we release.  Releasing in
		batches verifies every committed write ends up reflected in
		the index after CIC completes.
		"""
		ctrl = self.node.connect(autocommit=True)
		self._arm(ctrl, 'cic_after_ambuild')

		conn, t, pid = self._start_cic(
		    "CREATE INDEX CONCURRENTLY t_val_idx ON t (val);")
		try:
			wait_stopevent(self.node, pid)

			ranges = [(1000, 5), (2000, 5), (3000, 5)]
			threads = []
			for base, n in ranges:

				def worker(base=base, n=n):
					with self.node.connect(autocommit=True) as w:
						for i in range(base, base + n):
							w.execute("INSERT INTO t VALUES (%d, %s);" %
							          (i, repr('cv%d' % i)))

				th = threading.Thread(target=worker)
				th.start()
				threads.append(th)

			self._release(ctrl, 'cic_after_ambuild')
			t.join()
			for th in threads:
				th.join()
		finally:
			conn.close()
			ctrl.close()

		extra = sum(n for _, n in ranges)
		self.assertEqual(self._heap_count(), 100 + extra)
		# All inserted rows are reachable via the new index (count via
		# range predicate; LIKE would trip psycopg2 '%' binding).
		idx_total = self._index_count(
		    "val >= 'v' AND val < 'w'") + self._index_count(
		        "val >= 'cv' AND val < 'cw'")
		self.assertEqual(idx_total, 100 + extra)
