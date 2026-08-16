#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class RecoveryHeapXidBindingTest(BaseTest):
	"""Crash recovery rolls back half of a committed transaction.

	This is what losing the logical xid at RELEASE SAVEPOINT costs beyond
	logical decoding, and it is why that defect is a data-loss bug rather than
	a replication one.

	A transaction that owns a heap xid writes no OrioleDB finish record of its
	own: replay settles it from the builtin commit record, and can only apply
	that verdict to the oxid if some record carried the (oxid, heap xid) pair.
	Two records normally carry it, and this transaction has neither:

	  * add_xid_wal_record() samples GetTopTransactionIdIfAny() when a
	    container's first record is buffered, so a transaction that acquires
	    its heap xid *after* its last OrioleDB change has every WAL_REC_XID
	    carrying InvalidTransactionId;
	  * wal_joint_commit() is written at XACT_EVENT_PRE_COMMIT only while a
	    logical xid is held -- and a transaction whose first OrioleDB write
	    happened inside a savepoint is handed back an invalid one when that
	    savepoint is released.

	Replay then applies the changes and never learns whose they are, so
	recovery_finish() rolls them back while PostgreSQL, which took the builtin
	commit record at face value, considers the transaction committed.  On an
	unfixed build:

	    XID(oxid=53 lxid=192 heapXid=0)  UPDATE o_bind
	    XID(oxid=53 lxid=0   heapXid=0)  UPDATE o_bind
	    XACT_COMMIT xl_xid=755

	    value before crash: 555
	    value after crash recovery: 0      <- OrioleDB half rolled back
	    heap row after crash recovery: 1   <- heap half survived
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
		# first OrioleDB write inside a subtransaction: releasing it used to
		# take the transaction's logical xid away
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
