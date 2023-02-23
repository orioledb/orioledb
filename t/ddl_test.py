#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

class DDLTest(BaseTest):
	def test_update_default_to_null_same_trx(self):
		with self.node as node:
			node.start()
			node.safe_psql("CREATE EXTENSION orioledb;")
			with node.connect() as con:
				con.execute("CREATE TABLE o_test () USING orioledb;")
				con.execute("INSERT INTO o_test DEFAULT VALUES;")
				con.execute("""
					ALTER TABLE o_test ADD COLUMN val_1 int DEFAULT 1;
				""")
				con.execute("""
					CREATE INDEX ON o_test (val_1);
				""")
				self.assertEqual([(1,)],
								 con.execute("""
									 SET LOCAL enable_seqscan = false;
									 SELECT * FROM o_test;
								 """))
				con.execute("""
					UPDATE o_test SET val_1 = NULL;
				""")
				con.execute("""
					SELECT * FROM o_test WHERE val_1 IS NULL;
				""")
				self.assertEqual([(None,)],
								 con.execute("""
									 SELECT * FROM o_test WHERE val_1 IS NULL;
								 """))
			node.stop()