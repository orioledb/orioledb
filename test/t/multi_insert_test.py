#!/usr/bin/env python3
# coding: utf-8
"""multi_insert tests.

test_concurrent_* exercise the per-leaf lwlock serialization between
two parallel COPY sessions.  orioledb_multi_insert acquires each
primary leaf's lwlock via plain lock_page (the helper does not set
insertTuple, so it does not queue as a waiter on a contended leaf --
it just blocks).  These tests verify no corruption, no deadlock, and
correct conflict semantics under concurrency.

test_ab_ordered_copy A/B-benchmarks orioledb.debug_disable_multi_insert
to catch regressions in the batched same-leaf primary insert path
(o_tbl_multi_insert), which orioledb_multi_insert dispatches to for
COPY-driven bulk loads.  Gated behind RUN_ORIOLEDB_PERF_TESTS=1.
"""

import io
import os
import threading
import time
import unittest

from .base_test import BaseTest

# Conservative threshold: ON may not be slower than this multiple of OFF.
# We mainly want to catch a regression, not enforce a guaranteed speedup
# (which varies by host).
MAX_ALLOWED_SLOWDOWN = 1.20

RUNS_PER_MODE = 3
ROWS_PER_RUN = 200_000
ROWS_PER_SESSION = 5000

VALGRIND = os.environ.get('USE_VALGRIND', '') == '1'

# Perf tests cost minutes (valgrind: 5+ minutes for the 1.4M-row benchmark)
# and add nothing to correctness coverage already in the concurrency cases
# and test/sql/multi_insert.sql.  Opt in via RUN_ORIOLEDB_PERF_TESTS=1 from
# a dedicated perf CI job or a manual pre-release run.
PERF_TESTS = os.environ.get('RUN_ORIOLEDB_PERF_TESTS', '') == '1'


def _build_csv(n: int) -> str:
	"""Generate an ordered TSV payload (id, val, grp)."""
	buf = io.StringIO()
	for i in range(1, n + 1):
		buf.write(f"{i}\t{i * 7}\t{i % 113}\n")
	return buf.getvalue()


def _csv(payload):
	"""Wrap a list of (id, val) tuples into a TSV StringIO."""
	buf = io.StringIO()
	for row in payload:
		buf.write(f"{row[0]}\t{row[1]}\n")
	buf.seek(0)
	return buf


def _run_copy(node, payload, errors):
	"""Open a connection, run a single COPY, record any exception."""
	con = node.connect()
	try:
		c = con.cursor
		c.execute("SET orioledb.debug_disable_multi_insert = off")
		try:
			c.copy_expert("COPY t (id, val) FROM STDIN", _csv(payload))
			con.connection.commit()
		except Exception as e:
			errors.append(e)
			try:
				con.connection.rollback()
			except Exception:
				pass
	finally:
		con.close()


class MultiInsertTest(BaseTest):

	def _prepare_t(self):
		"""Spin up a fresh table 't' used by the concurrency cases."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t (
				id  bigint PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX t_val_idx ON t(val);
		""")
		return node

	def _check_t(self, node, expected_count):
		con = node.connect()
		try:
			c = con.cursor
			c.execute("SELECT count(*) FROM t")
			(n, ) = c.fetchone()
			self.assertEqual(n, expected_count)
			c.execute("SELECT orioledb_tbl_check('t'::regclass)")
			(ok, ) = c.fetchone()
			self.assertTrue(ok, "orioledb_tbl_check returned false")
		finally:
			con.close()

	def test_concurrent_disjoint(self):
		"""Two sessions, disjoint key ranges, same table.

		Both must succeed; no contention loss, tree intact.
		"""
		node = self._prepare_t()
		try:
			a = [(i, i) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, i)
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy, args=(node, a, errors))
			tb = threading.Thread(target=_run_copy, args=(node, b, errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			self._check_t(node, 2 * ROWS_PER_SESSION)
		finally:
			node.stop()

	def test_concurrent_overlapping(self):
		"""Two sessions, interleaved keys on the same table.

		Session A writes even ids, B writes odd ids.  Both share the same
		primary leaves so they contend on each leaf's lwlock; the
		serialization path must produce a consistent tree.
		"""
		node = self._prepare_t()
		try:
			even = [(i, i) for i in range(2, 2 * ROWS_PER_SESSION + 1, 2)]
			odd = [(i, i) for i in range(1, 2 * ROWS_PER_SESSION + 1, 2)]
			errors = []
			ta = threading.Thread(target=_run_copy, args=(node, even, errors))
			tb = threading.Thread(target=_run_copy, args=(node, odd, errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			self._check_t(node, 2 * ROWS_PER_SESSION)
		finally:
			node.stop()

	def test_concurrent_same_key_conflict(self):
		"""Two sessions racing on overlapping PK values.

		Each thread COPYs 1..ROWS_PER_SESSION.  Exactly one row per id
		should land; the loser raises unique_violation (the entire COPY
		aborts on first conflict).  Tree must still be consistent.
		"""
		node = self._prepare_t()
		try:
			payload = [(i, i) for i in range(1, ROWS_PER_SESSION + 1)]
			errors = []
			ta = threading.Thread(target=_run_copy,
			                      args=(node, payload, errors))
			tb = threading.Thread(target=_run_copy,
			                      args=(node, payload, errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			# Both COPYs ran concurrently.  Either both raced fully and one
			# saw a unique_violation, or one finished before the other
			# started and the second saw the conflict.  Either way, exactly
			# one failure expected.
			self.assertEqual(
			    len(errors), 1, f"expected exactly one unique_violation, "
			    f"got {len(errors)}: {[str(e) for e in errors]}")
			self.assertIn(
			    "duplicate key",
			    str(errors[0]).lower() +
			    " ".join(str(arg) for arg in errors[0].args).lower())
			self._check_t(node, ROWS_PER_SESSION)
		finally:
			node.stop()

	def _copy_once(self, nodecon, payload: str, mode_on: bool) -> float:
		c = nodecon.cursor
		c.execute("SET orioledb.debug_disable_multi_insert = " +
		          ("off" if mode_on else "on"))
		c.execute("TRUNCATE t_perf")
		nodecon.connection.commit()
		start = time.perf_counter()
		c.copy_expert("COPY t_perf (id, val, grp) FROM STDIN",
		              io.StringIO(payload))
		nodecon.connection.commit()
		return time.perf_counter() - start

	@unittest.skipUnless(PERF_TESTS,
	                     "perf test; set RUN_ORIOLEDB_PERF_TESTS=1 to enable")
	def test_ab_ordered_copy(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_perf (
				id  bigint PRIMARY KEY,
				val int,
				grp int
			) USING orioledb;
			CREATE INDEX t_perf_val_idx ON t_perf(val);
		""")

		payload = _build_csv(ROWS_PER_RUN)
		nodecon = node.connect()
		try:
			# Warm-up so JIT/syscache costs don't taint the first measurement.
			self._copy_once(nodecon, payload, mode_on=False)

			off_times = [
			    self._copy_once(nodecon, payload, mode_on=False)
			    for _ in range(RUNS_PER_MODE)
			]
			on_times = [
			    self._copy_once(nodecon, payload, mode_on=True)
			    for _ in range(RUNS_PER_MODE)
			]

			off = min(off_times)
			on = min(on_times)
			ratio = on / off if off > 0 else float('inf')
			speedup_pct = (1.0 - ratio) * 100.0

			print(
			    f"\n[multi_insert] rows={ROWS_PER_RUN} "
			    f"off={off:.3f}s on={on:.3f}s "
			    f"ratio={ratio:.3f} speedup={speedup_pct:+.1f}%",
			    flush=True)

			nodecon.cursor.execute("SELECT count(*) FROM t_perf")
			(rowcount, ) = nodecon.cursor.fetchone()
			self.assertEqual(rowcount, ROWS_PER_RUN)

			if not VALGRIND:
				self.assertLess(
				    ratio, MAX_ALLOWED_SLOWDOWN,
				    f"batched multi_insert should not slow inserts "
				    f"(off={off:.3f}s, on={on:.3f}s, ratio={ratio:.3f})")
		finally:
			nodecon.close()
		node.stop()


if __name__ == "__main__":
	unittest.main()
