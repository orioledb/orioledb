#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

# with orioledb.logical_xid_buffers = 1 the shared bitmap holds
# 1 * (BLCKSZ / 4) * 32 = 65536 logical xids
BUDGET = 65536


class LogicalXidReleaseTest(BaseTest):
	"""Releasing a subtransaction holds its logical xid for the whole xact.

	A released subtransaction keeps its logical xid until the top-level
	transaction ends, so that logical decoding may not hand the xid to
	another subtransaction while it still buffers changes under it.  The
	bitmap is shared by the whole cluster, so one transaction that releases
	many subtransactions starves every other session.  Without logical
	decoding nothing buffers those changes, so the xid goes back at once.

	The bitmap is shrunk to one block so the budget is reachable in a test;
	in production it is 64 blocks, i.e. 4.2M releases.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "orioledb.logical_xid_buffers = 1\n")

	def test_released_subtransactions_do_not_exhaust_logical_xids(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_x (id int NOT NULL, v int NOT NULL,\n"
		    "  PRIMARY KEY (id)) USING orioledb;\n"
		    "INSERT INTO o_x VALUES (1, 0);")

		# each iteration opens a subtransaction, writes OrioleDB data in it
		# and releases it -- the PL/pgSQL block raises nothing, so it ends
		# with SUBXACT_EVENT_COMMIT_SUB
		node.safe_psql(
		    'postgres', "DO $$\n"
		    "BEGIN\n"
		    "  FOR i IN 1..%d LOOP\n"
		    "    BEGIN\n"
		    "      UPDATE o_x SET v = v + 1 WHERE id = 1;\n"
		    "    EXCEPTION WHEN OTHERS THEN\n"
		    "      NULL;\n"
		    "    END;\n"
		    "  END LOOP;\n"
		    "END $$;" % (BUDGET + 4096))
		self.assertEqual(
		    node.execute("SELECT v FROM o_x WHERE id = 1;")[0][0],
		    BUDGET + 4096)

		# the bitmap must still hand out ids afterwards
		node.safe_psql(
		    'postgres', "BEGIN;\n"
		    "SAVEPOINT s;\n"
		    "UPDATE o_x SET v = v + 1 WHERE id = 1;\n"
		    "RELEASE SAVEPOINT s;\n"
		    "COMMIT;")
		self.assertEqual(
		    node.execute("SELECT v FROM o_x WHERE id = 1;")[0][0],
		    BUDGET + 4097)
		node.stop()
