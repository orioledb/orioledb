#!/usr/bin/env python3
# coding: utf-8
"""
Reading an old snapshot through every shape of page-level undo chain.

A page-level undo record either carries page bytes or it does not:

    UndoPageImageCompact    full   page compaction
    UndoPageImageSplit      full   split, when it drops a tuple or a sequential
                                   scan is registered on the tree
    UndoPageImageSplitDiff  diff   split, otherwise -- split key only
    UndoPageImageMerge      full   merge, when it drops a tuple
    UndoPageImageMergeDiff  diff   merge, otherwise -- boundary key only

Applying a full record overwrites the image the chain walk carries; a
differential one transforms it in place.  So what matters is the composition of
a chain and, just as much, *where the reader's snapshot sits in it*: the walk
applies records while the page csn is at or above the snapshot csn and stops
below, so the same chain gives a reader that predates its full image a shared
page, and a reader that postdates it the narrow halves.  Both readings have to
be right, and #1055 was the first of them going wrong.

Two facts, both measured with elog probes at the five write sites, shape these
cases:

  * A page's *first* image is always a full one.  A differential record is only
    written when the page already carries a retained undo location, and an
    operation on a page that has none writes no image at all (it freezes the
    page instead).  So there is no such thing as a purely differential chain,
    and every case that wants a differential record has to seed a full one
    first.
  * Which is why the snapshot position is the interesting axis: seeded before
    the full image, a reader walks through it; seeded after, it stops above it.

Every case therefore checks both positions.  A snapshot's own view, captured
when it is taken, must come back unchanged through every read path -- payload
included, since a read that returns the right keys with a stale version is
exactly what an isolation checker reports.
"""

import unittest

from .base_test import BaseTest

ROWS = 2000
PAD = 100
SAMPLE_COUNT = 5


class UndoImageChainTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.start()
		self.node.safe_psql('postgres',
		                    "CREATE EXTENSION IF NOT EXISTS orioledb;")

	# ------------------------------------------------------------------ helpers

	def _make(self, table, fillfactor=None, step=1):
		with_clause = ("WITH (fillfactor = %d)" %
		               fillfactor if fillfactor is not None else "")
		self.node.safe_psql(
		    'postgres', "DROP TABLE IF EXISTS %s;"
		    "CREATE TABLE %s ("
		    "    id int NOT NULL,"
		    "    payload text NOT NULL,"
		    "    PRIMARY KEY (id)"
		    ") USING orioledb %s;" % (table, table, with_clause))
		self.node.execute("INSERT INTO %s "
		                  "(SELECT id * %d, repeat('v0', %d) "
		                  "FROM generate_series(1, %d) id);" %
		                  (table, step, PAD, ROWS))

	def _snapshot(self, table):
		"""
		Open a REPEATABLE READ snapshot and capture the view it must keep.

		The capturing read finishes, so the snapshot leaves no sequential scan
		registered on the tree: only its retained undo matters from here on.
		"""
		con = self.node.connect()
		con.begin()
		con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
		view = con.execute("SELECT id, payload FROM %s ORDER BY id;" % table)
		self.assertTrue(len(view) > 0)
		return con, view

	class _ParkedSeqScan:
		"""A sequential scan parked on a cursor keeps numSeqScans raised, which
		is what makes a split write a full image instead of a differential one."""

		def __init__(self, test, table):
			self.con = test.node.connect()
			self.con.begin()
			self.con.execute("SET enable_indexscan = off;")
			self.con.execute("SET enable_bitmapscan = off;")
			self.con.execute("DECLARE ch NO SCROLL CURSOR FOR "
			                 "SELECT id FROM %s;" % table)
			test.assertEqual(len(self.con.execute("FETCH 1 FROM ch;")), 1)

		def __enter__(self):
			return self

		def __exit__(self, *exc):
			try:
				self.con.execute("CLOSE ch;")
				self.con.rollback()
			finally:
				self.con.close()
			return False

	def _park_seq_scan(self, table):
		return self._ParkedSeqScan(self, table)

	def _interleave(self, table, offset, step, payload):
		"""Insert keys between the existing ones, splitting every leaf."""
		self.node.execute("INSERT INTO %s "
		                  "(SELECT id * %d + %d, repeat('%s', %d) "
		                  "FROM generate_series(1, %d) id);" %
		                  (table, step, offset, payload, PAD, ROWS))

	def _rewrite(self, table, payload):
		"""Grow every row: compacts the leaves and splits some of them."""
		self.node.execute("UPDATE %s SET payload = repeat('%s', %d);" %
		                  (table, payload, PAD + 10))

	def _drop_third(self, table):
		"""Settled deletes, so a later merge has tuples to drop."""
		self.node.safe_psql('postgres',
		                    "DELETE FROM %s WHERE MOD(id, 3) = 0;" % table)

	def _merge(self, table):
		"""CHECKPOINT's page-merge pass, on a table sparse enough to merge."""
		self.node.execute("CHECKPOINT;")

	def _verify(self, con, table, view, label):
		"""The snapshot's captured view must come back through every path."""
		ids = [r[0] for r in view]
		pairs = sorted((r[0], r[1]) for r in view)

		plans = (
		    ("seq", "SET LOCAL enable_indexscan = off;"
		     "SET LOCAL enable_bitmapscan = off;"),
		    ("index", "SET LOCAL enable_seqscan = off;"
		     "SET LOCAL enable_bitmapscan = off;"),
		    ("bitmap", "SET LOCAL enable_seqscan = off;"
		     "SET LOCAL enable_indexscan = off;"),
		)
		for plan, prep in plans:
			where = "%s/%s" % (label, plan)
			rows, distinct = con.execute(
			    prep +
			    "SELECT count(*), count(DISTINCT id) FROM %s;" % table)[0]
			self.assertEqual((where, rows), (where, len(ids)))
			self.assertEqual((where, distinct), (where, len(ids)))
			got = sorted(
			    (r[0], r[1])
			    for r in con.execute(prep +
			                         "SELECT id, payload FROM %s;" % table))
			self.assertEqual((where, got == pairs), (where, True))

		# Ordered reads in both directions.  A dropped range or a duplicate
		# shows up in the length first, which keeps the message short.
		con.execute("SET enable_seqscan = off;")
		con.execute("SET enable_bitmapscan = off;")
		forward = [
		    r[0] for r in con.execute("SELECT id FROM %s ORDER BY id;" % table)
		]
		backward = [
		    r[0]
		    for r in con.execute("SELECT id FROM %s ORDER BY id DESC;" % table)
		]
		self.assertEqual((label, 'forward', len(forward)),
		                 (label, 'forward', len(ids)))
		self.assertEqual((label, 'backward', len(backward)),
		                 (label, 'backward', len(ids)))
		self.assertEqual(forward, ids)
		self.assertEqual(backward, list(reversed(ids)))

		# Point lookups: the shape a key/value workload reads with.
		stride = max(1, len(view) // SAMPLE_COUNT)
		for key, payload in view[::stride][:SAMPLE_COUNT]:
			self.assertEqual(
			    [(payload, )],
			    con.execute("SELECT payload FROM %s WHERE id = %d;" %
			                (table, key)))
		con.execute("RESET enable_seqscan;")
		con.execute("RESET enable_bitmapscan;")

	def _run(self, table, seed, rest, fillfactor=None, step=1):
		"""
		Run a chain shape against two readers.

		`seed` writes the chain's full image; `rest` stacks the rest on top.
		One snapshot is taken before the seed, so its walk applies that full
		image, and one after it, so its walk stops above it.  Both must read
		their own view back.
		"""
		self._make(table, fillfactor=fillfactor, step=step)
		before, before_view = self._snapshot(table)
		try:
			seed(table)
			after, after_view = self._snapshot(table)
			try:
				rest(table)
				self._verify(before, table, before_view, 'before-seed')
				self._verify(after, table, after_view, 'after-seed')
			finally:
				self._close(after)
		finally:
			self._close(before)
		self.assertTrue(
		    self.node.execute(
		        "SELECT orioledb_tbl_check('%s'::regclass, true)" %
		        table)[0][0])

	@staticmethod
	def _close(con):
		# An assert-enabled build can catch the defect inside the backend before
		# it returns rows, so the connection may already be gone.
		try:
			con.rollback()
			con.close()
		except Exception:
			pass

	# --------------------------------------------------- split-only chains

	def test_split_diff_above_split_full(self):
		"""
		The #1055 shape.  Measured: SplitFull=194, then SplitDiff=97.

		Several current leaves resolve to the one full historical page, so the
		identity a sideways step deduplicates on must name that page -- for the
		reader that predates it.  The reader that postdates it stops above the
		full image and sees the narrow halves, which must stay distinct.
		"""
		self._run('o_uic_diff_over_full',
		          seed=lambda t: self._parked_interleave(t, 1, 4, 'v1'),
		          rest=lambda t: self._interleave(t, 2, 4, 'v2'),
		          step=4)

	def test_split_full_above_split_full(self):
		"""Two full split images, no differential record.  Measured: SplitFull."""
		self._run('o_uic_full_over_full',
		          seed=lambda t: self._parked_interleave(t, 1, 4, 'v1'),
		          rest=lambda t: self._parked_interleave(t, 2, 4, 'v2'),
		          step=4)

	def test_split_diff_above_compact(self):
		"""
		A differential split above a full compaction image.

		Measured: Compact=288 and SplitFull=96 from the rewrite, then
		SplitDiff=96.
		"""
		self._run('o_uic_diff_over_compact',
		          seed=lambda t: self._rewrite(t, 'v1'),
		          rest=lambda t: self._interleave(t, 1, 4, 'v2'),
		          step=4)

	def test_two_split_diffs_above_split_full(self):
		"""
		Two differential splits stacked on one full image -- the case where each
		record names its own split key and the walk must keep the tightest bound
		rather than the oldest (#1036).

		Measured: SplitFull, then SplitDiff twice.
		"""

		def rest(table):
			self._interleave(table, 2, 8, 'v2')
			self._interleave(table, 3, 8, 'v3')

		self._run('o_uic_two_diffs',
		          seed=lambda t: self._parked_interleave(t, 1, 8, 'v1'),
		          rest=rest,
		          step=8)

	def test_long_alternating_chain(self):
		"""
		Four records deep, alternating full and differential.

		Measured: SplitFull=386 and SplitDiff=195 over the same leaves.  A rule
		that looks only at the newest differential record, or only at the record
		the walk stops on, gets this wrong at some depth.
		"""

		def rest(table):
			self._interleave(table, 2, 8, 'v2')
			self._parked_interleave(table, 3, 8, 'v3')
			self._interleave(table, 4, 8, 'v4')

		self._run('o_uic_long_alt',
		          seed=lambda t: self._parked_interleave(t, 1, 8, 'v1'),
		          rest=rest,
		          step=8)

	# --------------------------------------------------- merge chains

	def test_merge_full_alone(self):
		"""
		A full merge image: settled deletes give the merge tuples to drop.

		Measured: MergeFull=792.
		"""
		self._run('o_uic_merge_full',
		          seed=self._drop_third,
		          rest=self._merge,
		          fillfactor=10)

	def test_merge_diff_above_full(self):
		"""
		A differential merge image above the full images that seeded it.

		Measured: Compact and SplitFull from the rewrite, then MergeDiff -- the
		merge drops nothing, and the pages already carry a retained image, so it
		writes the boundary key only.
		"""
		self._run('o_uic_merge_diff',
		          seed=lambda t: self._rewrite(t, 'v1'),
		          rest=self._merge,
		          fillfactor=10)

	def test_split_diff_above_merge_full(self):
		"""A differential split above a full merge image.  Measured: MergeFull=792,
		then SplitDiff=14."""

		def seed(table):
			self._drop_third(table)
			self._merge(table)

		self._run('o_uic_diff_over_mergefull',
		          seed=seed,
		          rest=lambda t: self._interleave(t, 1, 4, 'v2'),
		          fillfactor=10,
		          step=4)

	def test_split_diff_above_merge_diff(self):
		"""A differential split above a differential merge.  Measured: MergeDiff,
		then SplitDiff=9."""

		def seed(table):
			self._rewrite(table, 'v1')
			self._merge(table)

		self._run('o_uic_diff_over_mergediff',
		          seed=seed,
		          rest=lambda t: self._interleave(t, 1, 4, 'v2'),
		          fillfactor=10,
		          step=4)

	def test_merge_diff_above_split_full(self):
		"""
		A differential merge above a full split image.  Measured: SplitFull=12,
		then MergeDiff=8.

		The two merge halves walk into their own predecessors' histories and
		converge on the seed's one full image, so a sideways step that identified
		a historical page by the record it stopped on took them for one page and
		skipped a half: twelve rows of four thousand, on the backward scan only.
		Issue #1056.
		"""
		self._run('o_uic_mergediff_over_full',
		          seed=lambda t: self._parked_interleave(t, 1, 4, 'v1'),
		          rest=lambda t: (self._rewrite(t, 'v2'), self._merge(t)),
		          fillfactor=10,
		          step=4)

	def test_merge_then_split_chain(self):
		"""
		Merge and split records in one chain: the pages are merged and then
		split again under the same readers.

		Measured: SplitFull, MergeDiff, then SplitDiff.
		"""

		def rest(table):
			self._merge(table)
			self._interleave(table, 1, 4, 'v2')

		self._run('o_uic_merge_and_split',
		          seed=lambda t: self._parked_rewrite(t, 'v1'),
		          rest=rest,
		          fillfactor=10,
		          step=4)

	# --------------------------------------------------- composed helpers

	def _parked_interleave(self, table, offset, step, payload):
		with self._park_seq_scan(table):
			self._interleave(table, offset, step, payload)

	def _parked_rewrite(self, table, payload):
		with self._park_seq_scan(table):
			self._rewrite(table, payload)


if __name__ == '__main__':
	unittest.main()
