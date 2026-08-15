#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class RecoveryHeapXidBindingTest(BaseTest):
	"""Crash recovery rolls back half of a committed transaction.

	A transaction that owns a heap xid writes no OrioleDB finish record of its
	own -- its verdict reaches replay through the builtin commit record, and
	replay can only apply that verdict to the oxid if some record carried the
	(oxid, heap xid) pair.  Two records normally carry it, and both can be
	missing at once:

	  * add_xid_wal_record() samples GetTopTransactionIdIfAny() when a
	    container's first record is buffered, so a transaction that acquires
	    its heap xid *after* its last OrioleDB change has every WAL_REC_XID
	    carrying InvalidTransactionId;
	  * wal_joint_commit() is written at XACT_EVENT_PRE_COMMIT only while a
	    logical xid is held, and RELEASE SAVEPOINT hands that back to a parent
	    which -- if the transaction's first OrioleDB write happened inside the
	    subtransaction -- has none.

	Replay then applies the transaction's changes and never learns its
	verdict, so recovery_finish() rolls them back while PostgreSQL considers
	the transaction committed.  The WAL of the losing run reads:

	    XID(oxid=53 lxid=192 heapXid=0)  UPDATE o_t
	    XID(oxid=53 lxid=0   heapXid=0)  UPDATE o_t
	    XACT_COMMIT xl_xid=755

	-- a committed transaction whose OrioleDB half nothing can settle.

	Note this reproduces only while RELEASE SAVEPOINT can take the logical xid
	away.  Fixing that (orioledb/orioledb#1045) keeps the logical xid, so
	wal_joint_commit() fires again and carries the binding, and no SQL shape
	reaches the bare-binding path any more.  The assertion is kept as a guard
	on the property itself: a committed transaction's OrioleDB changes must
	survive crash recovery, whichever record told replay whose they were.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "checkpoint_timeout = 1h\n"
		    "max_wal_size = 10GB\n")

	def test_committed_changes_survive_a_late_heap_xid(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_bind (\n"
		    "  id int NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "CREATE TABLE h_bind (id int PRIMARY KEY);\n"
		    "INSERT INTO o_bind SELECT g, 0 FROM generate_series(1, 4) g;\n")
		node.safe_psql('postgres', "CHECKPOINT;")

		con = node.connect()
		con.begin()
		# first OrioleDB write inside a subtransaction: releasing it hands the
		# logical xid back to a parent that has none
		con.execute("SAVEPOINT s1;")
		con.execute("UPDATE o_bind SET v = 1 WHERE id = 1;")
		con.execute("RELEASE SAVEPOINT s1;")
		# ... more OrioleDB work, still with no heap xid, so its WAL_REC_XID
		# carries InvalidTransactionId ...
		con.execute("UPDATE o_bind SET v = 555 WHERE id = 2;")
		# ... and only now a heap xid, which no OrioleDB record has seen
		con.execute("INSERT INTO h_bind VALUES (1);")
		con.commit()
		con.close()

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertEqual(
		    node.execute("SELECT v FROM o_bind WHERE id = 2;")[0][0], 555,
		    "crash recovery rolled back the OrioleDB half of a committed "
		    "transaction")
		self.assertEqual(
		    node.execute("SELECT v FROM o_bind WHERE id = 1;")[0][0], 1)
		self.assertEqual(node.execute("SELECT count(*) FROM h_bind;")[0][0], 1)
		node.stop()
