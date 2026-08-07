#!/usr/bin/env python3
# coding: utf-8
"""multi_insert tests.

test_concurrent_* exercise the per-leaf lwlock serialization between
two parallel COPY sessions.  orioledb_multi_insert acquires each
primary leaf's lwlock via plain lock_page (the helper does not set
insertTuple, so it does not queue as a waiter on a contended leaf --
it just blocks).  These tests verify no corruption, no deadlock, and
correct conflict semantics under concurrency.
"""

import io
import os
import threading
import unittest

from .base_test import BaseTest

ROWS_PER_SESSION = 5000
TOAST_ROWS_PER_SESSION = 200

VALGRIND = os.environ.get('USE_VALGRIND', '') == '1'


def _tsv(rows, columns):
	buf = io.StringIO()
	for row in rows:
		buf.write("\t".join(str(row[c]) for c in range(columns)) + "\n")
	buf.seek(0)
	return buf


def _run_copy(node, payload, errors):
	"""Open a connection, run a single COPY, record any exception."""
	con = node.connect()
	try:
		c = con.cursor
		c.execute("SET orioledb.debug_disable_multi_insert = 'none'")
		try:
			c.copy_expert("COPY t (id, val) FROM STDIN", _tsv(payload, 2))
			con.connection.commit()
		except Exception as e:
			errors.append(e)
			try:
				con.connection.rollback()
			except Exception:
				pass
	finally:
		con.close()


def _run_copy_toast(node, table, payload, columns, errors):
	con = node.connect()
	try:
		c = con.cursor
		c.execute("SET orioledb.debug_disable_multi_insert = 'none'")
		try:
			cols = ", ".join(columns)
			c.copy_expert(f"COPY {table} ({cols}) FROM STDIN",
			              _tsv(payload, len(columns)))
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

	def test_concurrent_toast(self):
		"""Two sessions COPY into a TOAST-bearing table with disjoint keys."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_toast_conc (
				id   bigint PRIMARY KEY,
				body text
			) USING orioledb;
		""")
		try:
			body_len = 6000
			a = [(i, 'A' * body_len)
			     for i in range(1, TOAST_ROWS_PER_SESSION + 1)]
			b = [(i, 'B' * body_len)
			     for i in range(TOAST_ROWS_PER_SESSION +
			                    1, 2 * TOAST_ROWS_PER_SESSION + 1)]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_toast_conc", a,
			                            ["id", "body"], errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_toast_conc", b,
			                            ["id", "body"], errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_toast_conc")
				(n, ) = c.fetchone()
				self.assertEqual(n, 2 * TOAST_ROWS_PER_SESSION)
				c.execute(
				    "SELECT orioledb_tbl_check('t_toast_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute(
				    """
					SELECT count(*) FROM t_toast_conc
					 WHERE length(body) = %s
				""", (body_len, ))
				(n, ) = c.fetchone()
				self.assertEqual(n, 2 * TOAST_ROWS_PER_SESSION)
			finally:
				con.close()
		finally:
			node.stop()


if __name__ == "__main__":
	unittest.main()
