#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from testgres.exceptions import QueryException


class UnloggedTest(BaseTest):

	def test_unlogged_table_checkpoint_recovery(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE UNLOGGED TABLE o_test_1 (
				val_1 serial PRIMARY KEY,
				val_2 int,
				val_3 int,
				val_4 int
			) USING orioledb;

			INSERT INTO o_test_1 (val_1, val_2, val_3) VALUES (1,2,3);

			CHECKPOINT;
		""")

		node.stop(['-m', 'immediate'])

		node.start()

		self.assertEqual(node.execute("SELECT * FROM o_test_1;"), [])

		node.stop()

	def test_unlogged_table_recovery_checkpoint(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE UNLOGGED TABLE o_test_1 (
				val_1 int
			) USING orioledb;

			INSERT INTO o_test_1 VALUES (1);
		""")

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT * FROM o_test_1;"), [])
		node.safe_psql("""
			INSERT INTO o_test_1 VALUES (1);
		""")
		node.stop()

		node.start()
		self.assertEqual(node.execute("SELECT * FROM o_test_1;"), [(1, )])
		node.stop()

	def test_unlogged_table_replication(self):
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE UNLOGGED TABLE o_test_1(
							val_1 int
						) USING orioledb;

						INSERT INTO o_test_1 VALUES (1);
					""")

					con1.commit()

					self.catchup_orioledb(replica)

					self.assertEqual(master.execute("SELECT * FROM o_test_1;"),
					                 [(1, )])
					with self.assertRaises(QueryException) as e:
						replica.safe_psql("SELECT * FROM o_test_1;")
					self.assertErrorMessageEquals(e, (
					    f"cannot access temporary or unlogged relations during recovery"
					))
