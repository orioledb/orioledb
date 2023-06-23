#!/usr/bin/env python3
# coding: utf-8

import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException

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

	def test_cached_custom_path(self):
		with self.node as node:
			node.append_conf(filename='postgresql.conf',
							 default_table_access_method = 'orioledb')
			node.start()

			node.safe_psql("""CREATE EXTENSION orioledb;""")
			pgbench = node.pgbench(options=["-i", "-s", "1"],
								   stdout=subprocess.DEVNULL,
								   stderr=subprocess.DEVNULL)
			self.assertEqual(pgbench.wait(), 0)
			node.stop()
			node.start()
			with node.connect() as con:
				con.execute("""
					PREPARE P0_5 AS UPDATE pgbench_accounts
						SET abalance = abalance + $1 WHERE aid = $2;
				""")
				con.commit()
				con.execute("EXECUTE P0_5('2763', '76750');")
				con.execute("EXECUTE P0_5('-3338', '97628');")
				con.execute("EXECUTE P0_5('-3064', '59049');")
				con.execute("EXECUTE P0_5('586', '12143');")
				con.execute("EXECUTE P0_5('-1653', '13061');")
				con.execute("EXECUTE P0_5('-2841', '93929');")
				con.commit()
				con.execute("EXECUTE P0_5('3165', '17463');")

	def test_sys_attrs(self):
		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_1 (val_1 int)USING orioledb;
			INSERT INTO o_test_1 VALUES (1);
		""")
		tableoid = node.execute("SELECT 'o_test_1'::regclass::oid;")[0][0]
		self.assertEqual(node.execute("SELECT ctid, * FROM o_test_1;")[0],
		   				 ('(0,1)', 1))
		self.assertEqual(node.execute("SELECT tableoid, * FROM o_test_1;")[0],
		   				 (tableoid, 1))
		error_fields = ["xmin", "xmax", "cmin", "cmax"]
		for field in error_fields:
			with self.assertRaises(QueryException) as e:
				node.safe_psql(f"SELECT {field}, * FROM o_test_1;")
			self.assertErrorMessageEquals(e, (f"orioledb tuples does not have "
											  f"system attribute: {field}"))