#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class CommitWindowTest(BaseTest):
	"""
	A transaction that owns a heap xid is committed for the heap before OrioleDB
	says so.

	ProcArrayEndTransaction() publishes the transaction's CSN, and OrioleDB's
	XACT_EVENT_COMMIT callback runs afterwards, so without the
	XACT_EVENT_PRE_PROC_ARRAY mark there is a window in which
	oxid_match_snapshot() reports the oxid as in progress although its CSN is
	already below every snapshot taken from then on.

	A reader whose snapshot falls in that window therefore walks back over the
	transaction's row versions and answers from the state before it, while the
	heap -- which published the same CSN atomically with
	ProcArrayEndTransaction() -- answers with the transaction's data.  Reading
	the same key again after the window answers differently, in the same
	snapshot.

	Those are the two jepsen anomalies: a read that misses a committed
	transaction, and a snapshot that holds two answers for one key.

	orioledb.debug_commit_window_ms widens the window so the schedule is a
	matter of arithmetic rather than luck.

	With the mark in place a reader that lands in the window waits for the
	commit instead of answering from the state before it, so both tests pass
	however wide the window is.
	"""

	WINDOW_MS = 4000

	def setup_tables(self, node):
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql(
		    'postgres', "CREATE TABLE o_row (\n"
		    "	id integer NOT NULL PRIMARY KEY,\n"
		    "	val text NOT NULL\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE h_row (\n"
		    "	id integer NOT NULL PRIMARY KEY,\n"
		    "	val text NOT NULL\n"
		    ") USING heap;\n"
		    "INSERT INTO o_row VALUES (1, 'seed');\n"
		    "INSERT INTO h_row VALUES (1, 'seed');")

	def start_writer(self, node):
		"""
		One transaction appending twice to the OrioleDB row and once to the heap
		row.  The heap row is what gives it a heap xid, so its CSN comes from the
		heap commit.  COMMIT then sits inside the widened window.
		"""
		con = node.connect()
		con.execute("SET orioledb.debug_commit_window_ms = %d" %
		            self.WINDOW_MS)
		con.begin()
		# The same two appends into both tables, so their values must match.
		con.execute("UPDATE o_row SET val = val || ',a' WHERE id = 1")
		con.execute("UPDATE h_row SET val = val || ',a' WHERE id = 1")
		con.execute("UPDATE o_row SET val = val || ',b' WHERE id = 1")
		con.execute("UPDATE h_row SET val = val || ',b' WHERE id = 1")
		t = ThreadQueryExecutor(con, "COMMIT")
		t.start()
		return con, t

	def test_read_inside_the_commit_window(self):
		node = self.node
		node.start()
		self.setup_tables(node)

		wcon, wthread = self.start_writer(node)
		# The writer is now inside the window: its CSN is assigned, its mark is
		# not.  Read from the middle of it.
		time.sleep(self.WINDOW_MS / 4000.0)

		reader = node.connect()
		reader.begin()
		reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		# One statement, one snapshot, both access methods.
		inside = reader.execute(
		    "SELECT (SELECT val FROM o_row WHERE id = 1), "
		    "       (SELECT val FROM h_row WHERE id = 1)")[0]

		wthread.join()
		wcon.close()

		# Same snapshot, read again now that the window has closed.
		after = reader.execute(
		    "SELECT (SELECT val FROM o_row WHERE id = 1), "
		    "       (SELECT val FROM h_row WHERE id = 1)")[0]
		reader.rollback()
		reader.close()

		# Whatever the snapshot decided about the writer, it must decide the same
		# for both tables, and it must not change its mind.
		self.assertEqual(
		    inside[0], inside[1],
		    "one snapshot disagrees between orioledb and heap: %r" %
		    (inside, ))
		self.assertEqual(
		    inside, after,
		    "one snapshot returned two answers: inside=%r after=%r" %
		    (inside, after))
		node.stop()

	def test_repeated_read_inside_the_commit_window(self):
		"""
		The same thing seen the way jepsen sees it: one key read twice in one
		transaction, the first read landing inside the window.
		"""
		node = self.node
		node.start()
		self.setup_tables(node)

		wcon, wthread = self.start_writer(node)
		time.sleep(self.WINDOW_MS / 4000.0)

		reader = node.connect()
		reader.begin()
		reader.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		first = reader.execute("SELECT val FROM o_row WHERE id = 1")[0][0]
		wthread.join()
		wcon.close()
		second = reader.execute("SELECT val FROM o_row WHERE id = 1")[0][0]
		# And through a sequential scan, which resolves versions the same way.
		reader.execute("SET LOCAL enable_indexscan = off;"
		               "SET LOCAL enable_bitmapscan = off")
		third = reader.execute("SELECT val FROM o_row WHERE id = 1")[0][0]
		reader.rollback()
		reader.close()

		self.assertEqual(first, second,
		                 "repeated read changed: %r then %r" % (first, second))
		self.assertEqual(
		    first, third,
		    "sequential scan disagrees: %r vs %r" % (first, third))
		# A partial view of the writer is never a legal answer: it appended twice.
		self.assertIn(
		    first, ("seed", "seed,a,b"),
		    "read a state the writer never committed: %r" % (first, ))
		node.stop()
