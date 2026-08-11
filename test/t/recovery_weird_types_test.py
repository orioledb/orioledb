#!/usr/bin/env python3
# coding: utf-8

"""
Recovery tests for weird types and weird expression/partial indices.

These tests exercise code paths that could break comparator or hashing
logic during crash recovery:

  * Weird types — composite types (nested structs, ranges, multiranges,
    arrays, domains over composites, composites with range attributes),
    custom types with only btree opclass and no hash function, range
    types with domain subtypes.

  * Weird expression indices — CASE expressions, array subscripts,
    CoerceToDomain casts, ArrayCoerceExpr, row-construct casts
    (ConvertRowtypeExpr), whole-row variable references.

  * Weird partial indices — predicates using CASE, boolean expressions
    with IS DISTINCT FROM, etc.

Each test creates a table with an index that uses one of the above,
inserts rows, checks correctness, then crashes the server with
-m immediate and restarts to exercise the recovery code path.
Temp directories live under tmp_check_t and are cleaned up on success
by the BaseTest tearDown.
"""

import unittest

from .base_test import BaseTest


class RecoveryWeirdTypesTest(BaseTest):
	"""Crash-recovery tests for unusual data types in PK and index columns."""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "log_min_messages = notice\n")

	def _crash_and_recover(self, setup_sql, insert_sql, check_sql, expected):
		"""Run setup + insert, crash, restart, and verify data survives."""
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql('postgres', setup_sql)
		node.safe_psql('postgres', insert_sql)
		before = node.execute('postgres', check_sql)
		self.assertEqual(before, expected)
		node.stop(['-m', 'immediate'])
		node.start()
		after = node.execute('postgres', check_sql)
		self.assertEqual(after, expected)
		node.stop()

	# ------------------------------------------------------------------ #
	# Nested composite types in primary key                               #
	# ------------------------------------------------------------------ #
	def test_nested_composite_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE comp_inner AS (a int, b int);
			CREATE TYPE comp_outer AS (i comp_inner, c text);
			CREATE TABLE o_test (
				k comp_outer PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES
				(((ROW(1,2)::comp_inner, 'x'))::comp_outer, 10),
				(((ROW(3,4)::comp_inner, 'y'))::comp_outer, 20);
			""",
			"SELECT v FROM o_test ORDER BY k;",
			[(10, ), (20, )])

	# ------------------------------------------------------------------ #
	# Multirange in primary key                                           #
	# ------------------------------------------------------------------ #
	def test_multirange_pk(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				r int4multirange PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES ('{[1,5)}', 1);
			INSERT INTO o_test VALUES ('{[10,20)}', 2);
			""",
			"SELECT v FROM o_test ORDER BY r;",
			[(1, ), (2, )])

	# ------------------------------------------------------------------ #
	# Domain over composite type in primary key                           #
	# ------------------------------------------------------------------ #
	def test_domain_over_composite_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE dcomp AS (a int, b int);
			CREATE DOMAIN dom_comp AS dcomp CHECK ((VALUE).a > 0);
			CREATE TABLE o_test (
				k dom_comp PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES (ROW(1,2)::dom_comp, 10);
			INSERT INTO o_test VALUES (ROW(3,4)::dom_comp, 20);
			""",
			"SELECT v FROM o_test ORDER BY k;",
			[(10, ), (20, )])

	# ------------------------------------------------------------------ #
	# Range with domain subtype in primary key                           #
	# ------------------------------------------------------------------ #
	def test_range_domain_subtype_pk(self):
		self._crash_and_recover(
			"""
			CREATE DOMAIN dpos8 AS int8 CHECK (VALUE >= 0);
			CREATE TYPE dpos8range AS range (subtype = dpos8);
			CREATE TABLE o_test (
				r dpos8range PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES ('[1,5)', 1);
			INSERT INTO o_test VALUES ('[10,20)', 2);
			""",
			"SELECT v FROM o_test ORDER BY r;",
			[(1, ), (2, )])

	# ------------------------------------------------------------------ #
	# Composite type with a range attribute in primary key               #
	# ------------------------------------------------------------------ #
	def test_composite_with_range_attr_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE crange AS (rg int8range, name text);
			CREATE TABLE o_test (
				k crange PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES
				(ROW('[1,5)', 'a')::crange, 1),
				(ROW('[10,20)', 'b')::crange, 2);
			""",
			"SELECT v FROM o_test ORDER BY k;",
			[(1, ), (2, )])

	# ------------------------------------------------------------------ #
	# Array of composite type in table (non-PK, indexed)                 #
	# ------------------------------------------------------------------ #
	def test_composite_array_index(self):
		self._crash_and_recover(
			"""
			CREATE TYPE comp_t AS (x int, y int);
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				arr comp_t[]
			) USING orioledb;
			CREATE INDEX o_test_arr_ix ON o_test (arr);
			""",
			"""
			INSERT INTO o_test VALUES
				(1, ARRAY[ROW(1,2)::comp_t, ROW(3,4)::comp_t]),
				(2, ARRAY[ROW(5,6)::comp_t]);
			""",
			"SELECT id FROM o_test ORDER BY id;",
			[(1, ), (2, )])

	# ------------------------------------------------------------------ #
	# Domain with CHECK constraint used in expression index              #
	# (exercises the load_domaintype_info hook during recovery)          #
	# ------------------------------------------------------------------ #
	def test_domain_cast_in_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE DOMAIN dpos AS int CHECK (VALUE >= 0);
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test ((val::dpos));
			""",
			"""
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test WHERE (val::dpos) = 7 ORDER BY id;",
			[(7, )])

	# ------------------------------------------------------------------ #
	# Domain with NOT NULL constraint used in expression index            #
	# ------------------------------------------------------------------ #
	def test_domain_notnull_in_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE DOMAIN dnotnull AS int NOT NULL;
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test ((val::dnotnull));
			""",
			"""
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test WHERE (val::dnotnull) = 5 ORDER BY id;",
			[(5, )])


class RecoveryWeirdExprTest(BaseTest):
	"""Crash-recovery tests for unusual expression and partial indices."""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "log_min_messages = notice\n")

	def _crash_and_recover(self, setup_sql, insert_sql, check_sql, expected):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql('postgres', setup_sql)
		node.safe_psql('postgres', insert_sql)
		before = node.execute('postgres', check_sql)
		self.assertEqual(before, expected)
		node.stop(['-m', 'immediate'])
		node.start()
		after = node.execute('postgres', check_sql)
		self.assertEqual(after, expected)
		node.stop()

	# ------------------------------------------------------------------ #
	# CASE expression in index                                            #
	# ------------------------------------------------------------------ #
	def test_case_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test
				((CASE WHEN val > 5 THEN val ELSE 0 END));
			""",
			"""
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test "
			"WHERE (CASE WHEN val > 5 THEN val ELSE 0 END) = 7 ORDER BY id;",
			[(7, )])

	# ------------------------------------------------------------------ #
	# Array subscript expression in index                                 #
	# ------------------------------------------------------------------ #
	def test_subscript_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				arr int[]
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test ((arr[1]));
			""",
			"""
			INSERT INTO o_test
			SELECT g, ARRAY[g, g*2, g*3] FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test WHERE arr[1] = 5 ORDER BY id;",
			[(5, )])

	# ------------------------------------------------------------------ #
	# ArrayCoerceExpr — string_to_array cast in index                     #
	# ------------------------------------------------------------------ #
	def test_array_coerce_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				s text
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test
				((string_to_array(s, ',')::int[]));
			""",
			"""
			INSERT INTO o_test
			SELECT g, g::text || ',' || (g*2)::text
			FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test ORDER BY id LIMIT 1;",
			[(1, )])

	# ------------------------------------------------------------------ #
	# RowExpr cast to named composite type in expression index            #
	# (exercises the o_load_typcache_tupdesc_hook tdtypeid fix)           #
	# ------------------------------------------------------------------ #
	def test_rowexpr_cast_index(self):
		self._crash_and_recover(
			"""
			CREATE TYPE rt AS (x int, y int);
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				a int,
				b int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test (((a, b)::rt));
			""",
			"""
			INSERT INTO o_test
			SELECT g, g, g*2 FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test ORDER BY id LIMIT 1;",
			[(1, )])

	# ------------------------------------------------------------------ #
	# CoerceViaIO — cast via input/output functions in expression index   #
	# ------------------------------------------------------------------ #
	def test_coerce_via_io_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				s text
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test ((s::int));
			""",
			"""
			INSERT INTO o_test
			SELECT g, g::text FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test WHERE s::int = 5 ORDER BY id;",
			[(5, )])

	# ------------------------------------------------------------------ #
	# ScalarArrayOpExpr — ANY expression in index                        #
	# ------------------------------------------------------------------ #
	def test_scalar_array_op_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test ((val = ANY(ARRAY[1,3,5])));
			""",
			"""
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
			""",
			"SELECT count(*) FROM o_test;",
			[(10, )])

	# ------------------------------------------------------------------ #
	# Partial index with CASE in predicate                                #
	# ------------------------------------------------------------------ #
	def test_partial_index_case_predicate(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test (val)
				WHERE (CASE WHEN val % 2 = 0 THEN true ELSE false END);
			""",
			"""
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
			""",
			"SELECT count(*) FROM o_test;",
			[(10, )])

	# ------------------------------------------------------------------ #
	# Partial index with IS DISTINCT FROM in predicate                    #
	# ------------------------------------------------------------------ #
	def test_partial_index_is_distinct_from(self):
		self._crash_and_recover(
			"""
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				val int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test (val)
				WHERE (val IS DISTINCT FROM NULL);
			""",
			"""
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
			""",
			"SELECT count(*) FROM o_test;",
			[(10, )])

	# ------------------------------------------------------------------ #
	# Expression index combining domain cast + row construct              #
	# (exercises both load_domaintype_info and typcache tupdesc hooks)    #
	# ------------------------------------------------------------------ #
	def test_domain_cast_and_row_index(self):
		self._crash_and_recover(
			"""
			CREATE DOMAIN dpos AS int CHECK (VALUE >= 0);
			CREATE TYPE rt AS (x dpos, y dpos);
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				a int,
				b int
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test
				((((a::dpos, b::dpos))::rt));
			""",
			"""
			INSERT INTO o_test
			SELECT g, g, g*2 FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test ORDER BY id LIMIT 1;",
			[(1, )])

	# ------------------------------------------------------------------ #
	# Nested composite with domain attribute in expression index               #
	# ------------------------------------------------------------------ #
	def test_nested_composite_domain_expr_index(self):
		self._crash_and_recover(
			"""
			CREATE DOMAIN dpos AS int CHECK (VALUE >= 0);
			CREATE TYPE comp_inner AS (a dpos, b int);
			CREATE TYPE comp_outer AS (i comp_inner, c text);
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				a int,
				b int,
				c text
			) USING orioledb;
			CREATE INDEX o_test_expr ON o_test
				((ROW(ROW(a::dpos, b)::comp_inner, c)::comp_outer));
			""",
			"""
			INSERT INTO o_test
			SELECT g, g, g*2, g::text FROM generate_series(1, 10) g;
			""",
			"SELECT id FROM o_test ORDER BY id LIMIT 1;",
			[(1, )])


class RecoveryWeirdCustomTypeTest(BaseTest):
	"""Crash-recovery tests for custom types with limited operators."""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "log_min_messages = notice\n")

	def _crash_and_recover(self, setup_sql, insert_sql, check_sql, expected):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		node.safe_psql('postgres', setup_sql)
		node.safe_psql('postgres', insert_sql)
		before = node.execute('postgres', check_sql)
		self.assertEqual(before, expected)
		node.stop(['-m', 'immediate'])
		node.start()
		after = node.execute('postgres', check_sql)
		self.assertEqual(after, expected)
		node.stop()

	# ------------------------------------------------------------------ #
	# Composite type with a dropped column in primary key                 #
	# (tests that typcache hook handles attisdropped correctly)         #
	# ------------------------------------------------------------------ #
	def test_composite_with_dropped_col_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE ct AS (a int, b int, c int);
			ALTER TYPE ct DROP ATTRIBUTE c;
			CREATE TABLE o_test (
				k ct PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES (ROW(1, 2)::ct, 10);
			INSERT INTO o_test VALUES (ROW(3, 4)::ct, 20);
			""",
			"SELECT v FROM o_test ORDER BY v;",
			[(10, ), (20, )])

	# ------------------------------------------------------------------ #
	# Enum type in primary key                                            #
	# ------------------------------------------------------------------ #
	def test_enum_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
			CREATE TABLE o_test (
				m mood PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES ('sad', 1);
			INSERT INTO o_test VALUES ('happy', 2);
			INSERT INTO o_test VALUES ('ok', 3);
			""",
			"SELECT v FROM o_test ORDER BY m;",
			[(1, ), (3, ), (2, )])

	# ------------------------------------------------------------------ #
	# Composite type with enum attribute in primary key                   #
	# ------------------------------------------------------------------ #
	def test_composite_with_enum_attr_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
			CREATE TYPE cenum AS (m mood, id int);
			CREATE TABLE o_test (
				k cenum PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES
				(ROW('sad', 1)::cenum, 10),
				(ROW('happy', 2)::cenum, 20);
			""",
			"SELECT v FROM o_test ORDER BY k;",
			[(10, ), (20, )])

	# ------------------------------------------------------------------ #
	# Domain over enum type in primary key                                #
	# ------------------------------------------------------------------ #
	def test_domain_over_enum_pk(self):
		self._crash_and_recover(
			"""
			CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
			CREATE DOMAIN dmood AS mood CHECK (VALUE != 'sad');
			CREATE TABLE o_test (
				k dmood PRIMARY KEY,
				v int
			) USING orioledb;
			""",
			"""
			INSERT INTO o_test VALUES ('ok', 1);
			INSERT INTO o_test VALUES ('happy', 2);
			""",
			"SELECT v FROM o_test ORDER BY k;",
			[(1, ), (2, )])

	# ------------------------------------------------------------------ #
	# Array of enums in indexed column                                     #
	# ------------------------------------------------------------------ #
	def test_enum_array_index(self):
		self._crash_and_recover(
			"""
			CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
			CREATE TABLE o_test (
				id int PRIMARY KEY,
				moods mood[]
			) USING orioledb;
			CREATE INDEX o_test_moods_ix ON o_test (moods);
			""",
			"""
			INSERT INTO o_test VALUES
				(1, ARRAY['sad', 'ok']::mood[]),
				(2, ARRAY['happy']::mood[]);
			""",
			"SELECT id FROM o_test ORDER BY id;",
			[(1, ), (2, )])


if __name__ == '__main__':
	unittest.main()
