#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

class ParallelTest(BaseTest):
	def test_parallel_empty_select(self):
		with self.node as node:
			node.start()
			node.safe_psql("CREATE EXTENSION orioledb;")
			with node.connect() as con:
				con.execute("""
					CREATE TABLE o_test (
						val_1 int
					) USING orioledb;
				""")
				self.assertEqual([],
								 con.execute("""
									 SELECT * FROM o_test;
								 """))
				self.assertEqual([],
								 con.execute("""
									SET LOCAL parallel_setup_cost = 0;
									SET LOCAL parallel_tuple_cost = 0;
									SET LOCAL min_parallel_table_scan_size = 0;
									SET LOCAL max_parallel_workers_per_gather = 1;
									SELECT * FROM o_test;
								 """))
			node.stop()