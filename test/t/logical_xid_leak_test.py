#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

# with orioledb.logical_xid_buffers = 1 the shared bitmap holds
# 1 * (BLCKSZ / 4) * 32 = 65536 logical xids
BUDGET = 65536


class LogicalXidLeakTest(BaseTest):
	"""Rolling back a subtransaction leaks its logical xid.

	SUBXACT_EVENT_ABORT_SUB restored the parent's context without releasing
	the subtransaction's logical xid, and release_assigned_logical_xids() only
	walks the current context and the stacked parents -- so nothing ever
	reclaimed it.  The shared bitmap then runs out after exactly its budget of
	rollbacks and every later subtransaction fails with "not enough logical
	xids", which is a cluster that ordinary use has broken: rolling back to a
	savepoint per conflicting insert is the usual upsert fallback.

	The bitmap is shrunk to one block so the budget is reachable in a test;
	in production it is 64 blocks, i.e. 4.2M rollbacks.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "orioledb.logical_xid_buffers = 1\n")

	def test_rolled_back_subtransactions_do_not_exhaust_logical_xids(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_x (id int NOT NULL, v int NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_x VALUES (1, 0);")

		# each iteration opens a subtransaction, writes OrioleDB data in it and
		# rolls it back -- the PL/pgSQL exception block is just a cheap way to
		# drive SUBXACT_EVENT_ABORT_SUB many times
		node.safe_psql(
		    'postgres', "DO $$\n"
		    "BEGIN\n"
		    "  FOR i IN 1..%d LOOP\n"
		    "    BEGIN\n"
		    "      UPDATE o_x SET v = v + 1 WHERE id = 1;\n"
		    "      RAISE EXCEPTION 'rollback';\n"
		    "    EXCEPTION WHEN OTHERS THEN\n"
		    "      NULL;\n"
		    "    END;\n"
		    "  END LOOP;\n"
		    "END $$;" % (BUDGET + 4096))

		# the bitmap must still hand out ids afterwards
		node.safe_psql(
		    'postgres', "BEGIN;\n"
		    "SAVEPOINT s;\n"
		    "UPDATE o_x SET v = v + 1 WHERE id = 1;\n"
		    "RELEASE SAVEPOINT s;\n"
		    "COMMIT;")
		self.assertEqual(
		    node.execute("SELECT v FROM o_x WHERE id = 1;")[0][0], 1)
		node.stop()
