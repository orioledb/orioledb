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
import random
import threading
import unittest
import uuid

from .base_test import BaseTest

ROWS_PER_SESSION = 5000
TOAST_ROWS_PER_SESSION = 200

# test_wide_key_copy_keeps_order: enough rows that the batch keeps inserting
# into interior leaves and those leaves grow several chunks each.
WIDE_KEY_IDS = 4000
WIDE_KEY_BATCHES = 3

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
		c.execute("SET orioledb.debug_disable_multi_insert = false")
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
		c.execute("SET orioledb.debug_disable_multi_insert = false")
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

	def test_wide_key_copy_keeps_order(self):
		"""Repeated COPY into a table keyed by (uuid, timestamp).

		A random leading key column sends the batch into interior leaves,
		and a wide key makes those leaves grow extra chunks mid-batch.  The
		batch helper gates each item on the leaf hikey, which is a pointer
		into the page; splitting a chunk memmoves the hikeys area, so a
		hikey read once per leaf goes stale and the gate starts comparing
		against unrelated bytes.  Items that belong to the next leaf then
		land in this one, and an ordered scan comes back unsorted.

		The concurrency cases above cannot see this: a bigint PK fed in
		ascending order always appends to the rightmost leaf, which has no
		hikey to gate on.
		"""
		node = self.node
		node.start()
		try:
			node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
			node.safe_psql("""
				CREATE TABLE t_wide (
					object_id uuid NOT NULL,
					ts        timestamp NOT NULL,
					payload   text NOT NULL,
					PRIMARY KEY (object_id, ts)
				) USING orioledb;
			""")

			rnd = random.Random(42)
			ids = [
			    str(uuid.UUID(int=rnd.getrandbits(128)))
			    for _ in range(WIDE_KEY_IDS)
			]

			con = node.connect()
			try:
				c = con.cursor
				c.execute("SET orioledb.debug_disable_multi_insert = false")
				for b in range(WIDE_KEY_BATCHES):
					buf = io.StringIO()
					for i, u in enumerate(ids):
						ts = "2026-01-01 00:00:%02d.%06d" % (b, i)
						buf.write("%s\t%s\t%s\n" % (u, ts, "x" * 64))
					buf.seek(0)
					c.copy_expert(
					    "COPY t_wide (object_id, ts, payload) FROM STDIN", buf)
					con.connection.commit()

				expected = WIDE_KEY_IDS * WIDE_KEY_BATCHES

				c.execute("SET enable_seqscan = off")
				c.execute("""
					SELECT count(*) FROM (
						SELECT (object_id, ts) <= lag((object_id, ts)) OVER () AS unsorted
						FROM t_wide
						WHERE object_id > '00000000-0000-0000-0000-000000000000'
					) s WHERE unsorted
				""")
				(unsorted, ) = c.fetchone()
				self.assertEqual(
				    unsorted, 0,
				    "ordered index scan returned rows out of order: "
				    "a leaf holds keys above its own hikey")

				c.execute("""
					SELECT count(*) FROM t_wide
					WHERE object_id > '00000000-0000-0000-0000-000000000000'
				""")
				(idx_count, ) = c.fetchone()
				self.assertEqual(idx_count, expected)

				c.execute("SET enable_seqscan = on")
				c.execute("SET enable_indexscan = off")
				c.execute("SET enable_bitmapscan = off")
				c.execute("SELECT count(*) FROM t_wide")
				(seq_count, ) = c.fetchone()
				self.assertEqual(seq_count, expected)
			finally:
				con.close()
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

	def test_concurrent_sk(self):
		"""Two sessions COPY disjoint keys into a table with secondary indexes."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_sk_conc (
				id  bigint PRIMARY KEY,
				val int NOT NULL
			) USING orioledb;
			CREATE INDEX ON t_sk_conc (val);
		""")
		try:
			a = [(i, i * 3) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, i * 3)
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_conc", a, ["id",
			                                                   "val"], errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_conc", b, ["id",
			                                                   "val"], errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_sk_conc")
				(n, ) = c.fetchone()
				self.assertEqual(n, 2 * ROWS_PER_SESSION)
				c.execute("SELECT orioledb_tbl_check('t_sk_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute(
				    "SELECT count(*) FROM t_sk_conc WHERE val BETWEEN 1000 AND 5000"
				)
				(n, ) = c.fetchone()
				self.assertGreater(n, 0)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_sk_toast(self):
		"""Two sessions COPY into a table with PK, SK, and TOAST columns."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_sk_toast_conc (
				id   bigint PRIMARY KEY,
				val  int NOT NULL,
				body text
			) USING orioledb;
			CREATE INDEX ON t_sk_toast_conc (val);
		""")
		try:
			body_len = 6000
			n = TOAST_ROWS_PER_SESSION
			a = [(i, i * 3, 'A' * body_len) for i in range(1, n + 1)]
			b = [(i, i * 3, 'B' * body_len) for i in range(n + 1, 2 * n + 1)]
			errors = []
			cols = ["id", "val", "body"]
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_toast_conc", a, cols,
			                            errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_toast_conc", b, cols,
			                            errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_sk_toast_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * n)
				c.execute(
				    "SELECT orioledb_tbl_check('t_sk_toast_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute(
				    "SELECT count(*) FROM t_sk_toast_conc WHERE val BETWEEN 100 AND 500"
				)
				(cnt, ) = c.fetchone()
				self.assertGreater(cnt, 0)
				c.execute("SET enable_seqscan = on")
				c.execute(
				    f"SELECT count(*) FROM t_sk_toast_conc WHERE length(body) = {body_len}"
				)
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * n)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_ctid_pk(self):
		"""Two sessions COPY into a ctid-PK table (no explicit PK)."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_ctid_conc (val int NOT NULL) USING orioledb;
		""")
		try:
			a = [(i, ) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, )
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_ctid_conc", a, ["val"],
			                            errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_ctid_conc", b, ["val"],
			                            errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_ctid_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * ROWS_PER_SESSION)
				c.execute("SELECT orioledb_tbl_check('t_ctid_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_ctid_sk(self):
		"""Two sessions COPY into a ctid-PK table with a secondary index."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_ctid_sk_conc (val int NOT NULL) USING orioledb;
			CREATE INDEX ON t_ctid_sk_conc (val);
		""")
		try:
			a = [(i, ) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, )
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_ctid_sk_conc", a, ["val"],
			                            errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_ctid_sk_conc", b, ["val"],
			                            errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_ctid_sk_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * ROWS_PER_SESSION)
				c.execute(
				    "SELECT orioledb_tbl_check('t_ctid_sk_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute(
				    "SELECT count(*) FROM t_ctid_sk_conc WHERE val BETWEEN 1000 AND 5000"
				)
				(cnt, ) = c.fetchone()
				self.assertGreater(cnt, 0)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_sk_overlapping(self):
		"""Two sessions with interleaved SK values contending on the same SK leaves."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_sk_overlap (
				id  bigint PRIMARY KEY,
				val int NOT NULL
			) USING orioledb;
			CREATE INDEX ON t_sk_overlap (val);
		""")
		try:
			a = [(2 * i, i % 100) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(2 * i - 1, i % 100) for i in range(1, ROWS_PER_SESSION + 1)]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_overlap", a, ["id", "val"],
			                            errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_overlap", b, ["id", "val"],
			                            errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_sk_overlap")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * ROWS_PER_SESSION)
				c.execute(
				    "SELECT orioledb_tbl_check('t_sk_overlap'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute("SELECT count(*) FROM t_sk_overlap WHERE val = 42")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * (ROWS_PER_SESSION // 100))
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_unique_sk_conflict(self):
		"""Two sessions racing on overlapping unique SK values."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_sk_uniq_conc (
				id   bigint PRIMARY KEY,
				code int NOT NULL UNIQUE
			) USING orioledb;
		""")
		try:
			a = [(i, i) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(ROWS_PER_SESSION + i, i)
			     for i in range(1, ROWS_PER_SESSION + 1)]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_uniq_conc", a,
			                            ["id", "code"], errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_sk_uniq_conc", b,
			                            ["id", "code"], errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(
			    len(errors), 1, f"expected exactly one unique_violation, "
			    f"got {len(errors)}: {[str(e) for e in errors]}")
			self.assertIn("duplicate key", str(errors[0]).lower())
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_sk_uniq_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, ROWS_PER_SESSION)
				c.execute(
				    "SELECT orioledb_tbl_check('t_sk_uniq_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_expr_sk(self):
		"""Two sessions COPY into a table with an expression SK index."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_expr_conc (
				id  bigint PRIMARY KEY,
				val int NOT NULL
			) USING orioledb;
			CREATE INDEX ON t_expr_conc ((val * 2 + 1));
		""")
		try:
			a = [(i, i * 3) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, i * 3)
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_expr_conc", a, ["id", "val"],
			                            errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_expr_conc", b, ["id", "val"],
			                            errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_expr_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * ROWS_PER_SESSION)
				c.execute("SELECT orioledb_tbl_check('t_expr_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute(
				    "SELECT count(*) FROM t_expr_conc WHERE (val * 2 + 1) BETWEEN 1000 AND 5000"
				)
				(cnt, ) = c.fetchone()
				self.assertGreater(cnt, 0)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_fallback_partial(self):
		"""Two sessions COPY into a table with a partial index (per-tuple fallback)."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_partial_conc (
				id  bigint PRIMARY KEY,
				val int NOT NULL
			) USING orioledb;
			CREATE INDEX ON t_partial_conc (val) WHERE val > 100;
		""")
		try:
			a = [(i, i * 3) for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, i * 3)
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_partial_conc", a,
			                            ["id", "val"], errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_partial_conc", b,
			                            ["id", "val"], errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_partial_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * ROWS_PER_SESSION)
				c.execute(
				    "SELECT orioledb_tbl_check('t_partial_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute(
				    "SELECT count(*) FROM t_partial_conc WHERE val > 100 AND val < 5000"
				)
				(cnt, ) = c.fetchone()
				self.assertGreater(cnt, 0)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_mixed_idx(self):
		"""Two sessions COPY into a table with both batchable and non-batchable indexes."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_mixed_conc (
				id  bigint PRIMARY KEY,
				a   int NOT NULL,
				b   int NOT NULL
			) USING orioledb;
			CREATE INDEX ON t_mixed_conc (a);
			CREATE INDEX ON t_mixed_conc (b) WHERE b > 500;
		""")
		try:
			a = [(i, i * 3, i * 7 % 1000)
			     for i in range(1, ROWS_PER_SESSION + 1)]
			b = [(i, i * 3, i * 7 % 1000)
			     for i in range(ROWS_PER_SESSION + 1, 2 * ROWS_PER_SESSION + 1)
			     ]
			errors = []
			ta = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_mixed_conc", a,
			                            ["id", "a", "b"], errors))
			tb = threading.Thread(target=_run_copy_toast,
			                      args=(node, "t_mixed_conc", b,
			                            ["id", "a", "b"], errors))
			ta.start()
			tb.start()
			ta.join()
			tb.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")
			con = node.connect()
			try:
				c = con.cursor
				c.execute("SELECT count(*) FROM t_mixed_conc")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, 2 * ROWS_PER_SESSION)
				c.execute(
				    "SELECT orioledb_tbl_check('t_mixed_conc'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
				c.execute("SET enable_seqscan = off")
				c.execute(
				    "SELECT count(*) FROM t_mixed_conc WHERE a BETWEEN 1000 AND 5000"
				)
				(cnt, ) = c.fetchone()
				self.assertGreater(cnt, 0)
				c.execute(
				    "SELECT count(*) FROM t_mixed_conc WHERE b > 500 AND b < 800"
				)
				(cnt, ) = c.fetchone()
				self.assertGreater(cnt, 0)
			finally:
				con.close()
		finally:
			node.stop()

	def test_concurrent_ctid_sk_key_aliasing(self):
		"""Reproducer for ctid-PK key-bound aliasing in SK multi-insert.

		orioledb_aminsertmulti uses a single index leaf slot for all tuples.
		fill_key_bound stores PointerGetDatum(&slot->tts_tid) for the TIDOID
		ctid suffix, a pointer, not a copy.  After the loop every key bound
		points to the SAME address (the last tuple's ctid).  Concurrent COPY
		interleaves ctids across backends; the aliased ctid causes
		btree_page_search to find a wrong insertion position, placing a tuple
		after entries with smaller ctids, corrupting the SK btree.

		The test COPYs from several threads into a ctid-PK table with an SK
		index, then compares seqscan vs indexscan counts per val group.
		Corruption makes some entries invisible to index scans."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql("""
			CREATE TABLE t_ctid_alias (val int NOT NULL) USING orioledb;
			CREATE INDEX ON t_ctid_alias (val);
		""")
		try:
			n_threads = 4
			rows_per = 20000 if not VALGRIND else 2000
			val_mod = 50
			errors = []
			threads = []
			for w in range(n_threads):

				def worker(wid=w):
					try:
						con = node.connect()
						try:
							c = con.cursor
							c.execute(
							    "SET orioledb.debug_disable_multi_insert = false"
							)
							c.execute(
							    "SET orioledb.debug_disable_sk_multi_insert = false"
							)
							buf = io.StringIO()
							for i in range(rows_per):
								val = (wid * rows_per + i) % val_mod
								buf.write(f"{val}\n")
							buf.seek(0)
							c.copy_expert("COPY t_ctid_alias FROM STDIN", buf)
							con.connection.commit()
						finally:
							con.close()
					except Exception as e:
						errors.append(e)

				t = threading.Thread(target=worker)
				threads.append(t)
			for t in threads:
				t.start()
			for t in threads:
				t.join()
			self.assertEqual(errors, [],
			                 f"unexpected errors: {[str(e) for e in errors]}")

			con = node.connect()
			try:
				c = con.cursor
				total = rows_per * n_threads

				c.execute("SELECT count(*) FROM t_ctid_alias")
				(cnt, ) = c.fetchone()
				self.assertEqual(cnt, total)

				# Seqscan counts
				c.execute("""
					SELECT val, count(*) FROM t_ctid_alias
					GROUP BY val ORDER BY val
				""")
				seq_counts = dict(c.fetchall())

				# Index-only scan counts
				c.execute("SET enable_seqscan = off")
				c.execute(
				    """
					SELECT val, count(*) FROM t_ctid_alias
					WHERE val BETWEEN 0 AND %s
					GROUP BY val ORDER BY val
				""", (val_mod - 1, ))
				idx_counts = dict(c.fetchall())
				c.execute("SET enable_seqscan = on")

				for v in sorted(seq_counts.keys()):
					self.assertEqual(
					    seq_counts[v], idx_counts.get(v, 0),
					    f"val={v}: seqscan={seq_counts[v]} vs "
					    f"indexscan={idx_counts.get(v, 0)}")

				c.execute(
				    "SELECT orioledb_tbl_check('t_ctid_alias'::regclass)")
				(ok, ) = c.fetchone()
				self.assertTrue(ok)
			finally:
				con.close()
		finally:
			node.stop()


if __name__ == "__main__":
	unittest.main()
