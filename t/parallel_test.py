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

	def test_not_using_gather_during_recovery_for_sql_opclass_function(self):
		with self.node as node:
			node.start()
			node.safe_psql('postgres', """

				CREATE EXTENSION IF NOT EXISTS orioledb;

				SELECT orioledb_parallel_debug_start();

				CREATE OR REPLACE FUNCTION my_int_cmp(a int, b int) RETURNS int
				AS $$
					SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
									(b::bit(5) & X'A8'::bit(5))::int);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);

				INSERT INTO o_test SELECT generate_series(1, 5);
			""")

			node.stop(['-m','immediate'])

			node.start()

			node.stop()
