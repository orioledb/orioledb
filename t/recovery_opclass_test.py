#!/usr/bin/env python3
# coding: utf-8

import unittest
from .base_test import BaseTest

class RecoveryOpclassTest(BaseTest):
	def setUp(self):
		super().setUp()
		self.node.append_conf('orioledb.recovery_pool_size = 1')

	def test_simple_sql_cmp_function(self):
		with self.node as node:
			node.start()
			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

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
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 5);
			""")

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(2,),(1,),(3,),(4,),(5,)])

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(2,),(1,),(3,),(4,),(5,)])

			node.stop()

	def test_deep_sql_cmp_function(self):
		with self.node as node:
			node.start()
			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE OR REPLACE FUNCTION my_int_cmp(a int, b int) RETURNS int
				AS $$
					SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
									(b::bit(5) & X'A8'::bit(5))::int);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE FUNCTION my_int_cmp_s_s_i(a int, b int)
					RETURNS int
				AS $$
					SELECT my_int_cmp(a, b);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE FUNCTION my_int_cmp_s_s_s_i(a int, b int)
					RETURNS int
				AS $$
					SELECT my_int_cmp_s_s_i(a, b);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE FUNCTION my_int_cmp_deep_sql(a int, b int)
					RETURNS int
				AS $$
					SELECT my_int_cmp_s_s_s_i(a, b);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_deep_sql(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 5);
			""")

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(2,),(1,),(3,),(4,),(5,)])

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(2,),(1,),(3,),(4,),(5,)])

			node.stop()

	def test_simple_expression_cmp_function(self):
		with self.node as node:
			node.start()
			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_simple_expression(a int, b int)
					RETURNS int
				AS $$
					SELECT (3 - a % 2) - (3 - b % 2);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_simple_expression(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 5);
			""")

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(1,),(3,),(5,),(2,),(4,)])

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(1,),(3,),(5,),(2,),(4,)])

			node.stop()

	def test_values_cmp_function(self):
		with self.node as node:
			node.start()
			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_values(a int, b int)
					RETURNS int
				AS $$
					SELECT (VALUES (a*a%11-b*b%11));
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_values(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 5);
			""")

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(1,),(5,),(2,),(4,),(3,)])

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(1,),(5,),(2,),(4,),(3,)])

			node.stop()

	def test_generate_series_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_generate_series(a int, b int)
					RETURNS int
				AS $$
					SELECT a % COUNT(*) - b % COUNT(*) FROM
						generate_series(1, 7);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_generate_series(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(7,),(1,),(8,),(2,),(9,),(3,),(10,),(4,),(5,),(6,)])

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(7,),(1,),(8,),(2,),(9,),(3,),(10,),(4,),(5,),(6,)])

			node.stop()

	def test_subselect_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_subselect(a int, b int)
					RETURNS int
				AS $$
					SELECT a % COUNT(*) - b % COUNT(*) FROM
						(SELECT v % 5, COUNT(v) FROM
							generate_series(1, 30) v GROUP BY 1) s1;
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_subselect(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(5,),(10,),(1,),(6,),(2,),(7,),(3,),(8,),(4,),(9,)])

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(node.execute("""
				SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
			"""), [(5,),(10,),(1,),(6,),(2,),(7,),(3,),(8,),(4,),(9,)])

			node.stop()

	def test_cte_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_cte(a int, b int)
					RETURNS int
				AS $$
					WITH a1 (v) AS (
						SELECT COUNT(*)+1 FROM generate_series(1, 5)
					)
					SELECT a%v-b%v FROM a1 LIMIT 1;
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_cte(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(6,), (1,), (7,), (2,), (8,), (3,), (9,), (4,), (10,), (5,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(6,), (1,), (7,), (2,), (8,), (3,), (9,), (4,), (10,), (5,)]
			)

			node.stop()

	def test_rows_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_test_func(a int, b int) RETURNS int AS $$
					SELECT LEAST(a, a)%8 - GREATEST(b, b)%8;
				$$ LANGUAGE SQL IMMUTABLE;
				CREATE FUNCTION my_int_cmp_rows_func(a int, b int)
					RETURNS int
				AS $$
					SELECT * FROM ROWS FROM(my_test_func(a, b));
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_rows_func(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(8,), (1,), (9,), (2,), (10,), (3,), (4,), (5,), (6,), (7,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(8,), (1,), (9,), (2,), (10,), (3,), (4,), (5,), (6,), (7,)]
			)

			node.stop()

	def test_join_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_join(a int, b int)
					RETURNS int
				AS $$
					SELECT * FROM (VALUES (a * 7 % 3 - b * 7 % 3)) s1
						LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_join(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(3,), (6,), (9,), (1,), (4,), (7,), (10,), (2,), (5,), (8,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(3,), (6,), (9,), (1,), (4,), (7,), (10,), (2,), (5,), (8,)]
			)

			node.stop()

	def test_complex_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_complex(a int, b int)
					RETURNS int
				AS $$
					SELECT pow(a, s1.r)::int%5 - pow(b, s1.r)::int%5
						FROM (SELECT 2) s1(r);
					WITH a1 (v) AS (
						SELECT COUNT(*)+1 FROM generate_series(1, 5)
					)
					SELECT a%v-b%v FROM a1 LIMIT 1;
					SELECT * FROM (VALUES (a * 7 % 3 - b * 7 % 3)) s1
						LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
					SELECT (VALUES (a*a%11-b*b%11));
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_complex(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(1,), (10,), (5,), (6,), (2,), (9,), (4,), (7,), (3,), (8,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(1,), (10,), (5,), (6,), (2,), (9,), (4,), (7,), (3,), (8,)]
			)

			node.stop()

	@unittest.skipIf(BaseTest.get_pg_version() < 14,
					 'SQL-standard function bodies added in postgres 14')
	def test_sqlbody_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_sqlbody(a int, b int)
					RETURNS int
				LANGUAGE SQL IMMUTABLE
				BEGIN ATOMIC
					SELECT * FROM (VALUES (a * 7 % 3 - b * 7 % 3)) s1
						LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
				END;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_sqlbody(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(3,), (6,), (9,), (1,), (4,), (7,), (10,), (2,), (5,), (8,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(3,), (6,), (9,), (1,), (4,), (7,), (10,), (2,), (5,), (8,)]
			)

			node.stop()

	def test_init_plan_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_init_plan(a int, b int)
					RETURNS int
				AS $$
					SELECT a % (select COUNT(*) from generate_series(1, 4)) -
						   b % (select COUNT(*) from generate_series(1, 4));
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_init_plan(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(4,), (8,), (1,), (5,), (9,), (2,), (6,), (10,), (3,), (7,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(4,), (8,), (1,), (5,), (9,), (2,), (6,), (10,), (3,), (7,)]
			)

			node.stop()

	def test_cast_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_cast(a int, b int)
					RETURNS int
				AS $$
					SELECT substring(CAST(ABS(a) as text) from 1 for 1)::int -
						   substring(CAST(ABS(b) as text) from 1 for 1)::int;
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_cast(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(1,), (10,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(1,), (10,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)]
			)

			node.stop()

	def test_windowagg_cmp_function(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp_windowagg(a int, b int)
					RETURNS int
				AS $$
					WITH s1 AS (
						select exists(
								   select COUNT(*) from generate_series(1, 4)
							   ) vv, sum(v) OVER () vvv
							from generate_series(1, 4) v
					)
					SELECT a % COUNT(vv) - b % COUNT(vvv) FROM s1;
				$$ LANGUAGE SQL IMMUTABLE;

				CREATE OPERATOR <^ (
					LEFTARG = int4,
					RIGHTARG = int4,
					PROCEDURE = int4lt
				);

				CREATE OPERATOR CLASS my_op_class FOR TYPE int
					USING btree
					AS OPERATOR 1 <^, OPERATOR 3 =,
					FUNCTION 1 my_int_cmp_windowagg(int, int);

				CREATE TABLE IF NOT EXISTS o_test (
					val integer
				) USING orioledb;

				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(4,), (8,), (1,), (5,), (9,), (2,), (6,), (10,), (3,), (7,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(4,), (8,), (1,), (5,), (9,), (2,), (6,), (10,), (3,), (7,)]
			)

			node.stop()

	def test_build_index(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE FUNCTION my_int_cmp(a int, b int)
					RETURNS int
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
			""")
			node.safe_psql("""
				INSERT INTO o_test SELECT generate_series(1, 10);
			""")
			node.safe_psql("""
				CREATE INDEX o_test_ix1 ON o_test(val my_op_class);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(10,), (8,), (2,), (1,), (3,), (9,), (4,), (6,), (7,), (5,)]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY val USING OPERATOR(<^);
				"""),
				[(10,), (8,), (2,), (1,), (3,), (9,), (4,), (6,), (7,), (5,)]
			)

			node.stop()

	def test_custom_type(self):
		with self.node as node:
			node.start()

			node.safe_psql('postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE TYPE my_rec_arr AS (arr char[]);
				CREATE FUNCTION my_rec_arr_to_int(my_rec_arr) RETURNS int
				AS $$
					SELECT ASCII($1.arr[1])-ASCII('A')+1;
				$$ LANGUAGE SQL IMMUTABLE;
				CREATE FUNCTION int_to_my_rec_arr(int) RETURNS my_rec_arr
				AS $$
					SELECT ROW(ARRAY[CHR(ASCII('A')+$1-1)])::my_rec_arr;
				$$ LANGUAGE SQL IMMUTABLE;
				CREATE CAST (my_rec_arr AS int)
					WITH FUNCTION my_rec_arr_to_int;
				CREATE CAST (int AS my_rec_arr)
					WITH FUNCTION int_to_my_rec_arr;

				CREATE TYPE my_record AS (
					val_1 integer,
					val_2 my_rec_arr
				);

				CREATE FUNCTION my_rec_cmp(a my_record, b my_record)
					RETURNS integer
				AS $$
					SELECT COALESCE(NULLIF(((a).val_2::int -
											(b).val_2::int),0),
										a.val_1 - b.val_1);
				$$ LANGUAGE SQL STRICT IMMUTABLE;

				CREATE FUNCTION my_rec_lt(a my_record, b my_record)
					RETURNS boolean
				AS $$
					BEGIN
						RETURN my_rec_cmp(a,b) < 0;
					END;
				$$ LANGUAGE plpgsql IMMUTABLE STRICT;

				CREATE FUNCTION my_rec_eq(a my_record, b my_record)
					RETURNS boolean
				AS $$
					BEGIN
						RETURN my_rec_cmp(a,b) = 0;
					END;
				$$ LANGUAGE plpgsql IMMUTABLE STRICT;

				CREATE OPERATOR #<# (
					LEFTARG = my_record,
					RIGHTARG = my_record,
					FUNCTION = my_rec_lt
				);

				CREATE OPERATOR #=# (
					LEFTARG = my_record,
					RIGHTARG = my_record,
					FUNCTION = my_rec_eq,
					MERGES
				);

				CREATE OPERATOR CLASS my_op_class_rec
					DEFAULT FOR TYPE my_record USING btree
					AS OPERATOR 1 #<#, OPERATOR 3 #=#,
					FUNCTION 1 my_rec_cmp(my_record, my_record);

				CREATE TABLE IF NOT EXISTS o_test (
					val		integer,
					rec_val	my_record
				) USING orioledb;
			""")
			node.safe_psql("""
				INSERT INTO o_test
					SELECT v,
						   (v % 4 + 1, ((v-1) % 3 + 1)::my_rec_arr)::my_record
					FROM generate_series(1, 10) v;
			""")
			node.safe_psql("""
				CREATE INDEX rec_val_ix ON o_test(rec_val);
			""")

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY rec_val USING OPERATOR(#<#);
				"""),
				[(4,'(1,"({A})")'), (1,'(2,"({A})")'),
				 (10,'(3,"({A})")'), (7,'(4,"({A})")'),
				 (8,'(1,"({B})")'), (5,'(2,"({B})")'), (2,'(3,"({B})")'),
				 (9,'(2,"({C})")'), (6,'(3,"({C})")'), (3,'(4,"({C})")')]
			)

			node.stop(['-m','immediate'])

			node.start()

			self.assertEqual(
				node.execute("""
					SELECT * FROM o_test ORDER BY rec_val USING OPERATOR(#<#);
				"""),
				[(4,'(1,"({A})")'), (1,'(2,"({A})")'),
				 (10,'(3,"({A})")'), (7,'(4,"({A})")'),
				 (8,'(1,"({B})")'), (5,'(2,"({B})")'), (2,'(3,"({B})")'),
				 (9,'(2,"({C})")'), (6,'(3,"({C})")'), (3,'(4,"({C})")')]
			)

			node.stop()
