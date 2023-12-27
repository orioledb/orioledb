#!/usr/bin/env python3
# coding: utf-8

import subprocess
import unittest

from testgres.connection import DatabaseError
from testgres.exceptions import QueryException

from .base_test import BaseTest


class NotSupportedYetTest(BaseTest):

	def test_reindex_concurrently(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE SCHEMA ddl;
			SET SESSION search_path = 'ddl';
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE pg_test_1 (
				val_1 int,
				val_2 int
			) USING heap;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
			INSERT INTO pg_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);

			CREATE INDEX o_ind_1 ON o_test_1 (val_1);
			CREATE INDEX pg_ind_1 ON pg_test_1 (val_1);
		""")

		# We doesn't break REINDEX CONCURRENTLY for postgres tables
		_, _, err = node.psql("""
			SET SESSION search_path = 'ddl';
			REINDEX (VERBOSE) TABLE CONCURRENTLY pg_test_1;
		""")
		self.assertTrue(err.decode("utf-8").split("\n")[0].find("pg_test_1"))

		# Error for orioledb tables
		_, _, err = node.psql("""
			SET SESSION search_path = 'ddl';
			REINDEX (VERBOSE) TABLE CONCURRENTLY o_test_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" "
		    "does not support REINDEX CONCURRENTLY")

		# Error for orioledb indices
		_, _, err = node.psql("""
			SET SESSION search_path = 'ddl';
			REINDEX (VERBOSE) INDEX CONCURRENTLY o_ind_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" "
		    "does not support REINDEX CONCURRENTLY")

		# Error for schema containing orioledb tables
		_, _, err = node.psql("""
			REINDEX (VERBOSE) SCHEMA CONCURRENTLY ddl;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" "
		    "does not support REINDEX CONCURRENTLY")

		# Error for database containing orioledb tables
		_, _, err = node.psql("""
			REINDEX (VERBOSE) DATABASE CONCURRENTLY postgres;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" "
		    "does not support REINDEX CONCURRENTLY")

		node.stop()

	def test_cluster(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE pg_test_1 (
				val_1 int,
				val_2 int
			) USING heap;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
			INSERT INTO pg_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);

			CREATE INDEX o_ind_1 ON o_test_1 (val_1);
			CREATE INDEX pg_ind_1 ON pg_test_1 (val_1);
		""")

		# We doesn't break CLUSTER for postgres tables
		_, _, err = node.psql("""
			CLUSTER VERBOSE pg_test_1 USING pg_ind_1;
		""")
		self.assertTrue(err.decode("utf-8").split("\n")[0].find("pg_test_1"))

		# Error for orioledb tables
		_, _, err = node.psql("""
			CLUSTER VERBOSE o_test_1 USING o_ind_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb tables does not support CLUSTER")

		node.stop()

	def test_cluster(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE pg_test_1 (
				val_1 int,
				val_2 int
			) USING heap;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
			INSERT INTO pg_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);

			CREATE INDEX o_ind_1 ON o_test_1 (val_1);
			CREATE INDEX pg_ind_1 ON pg_test_1 (val_1);
		""")

		# We doesn't break CLUSTER for postgres tables
		_, _, err = node.psql("""
			CLUSTER VERBOSE pg_test_1 USING pg_ind_1;
		""")
		self.assertTrue(err.decode("utf-8").split("\n")[0].find("pg_test_1"))

		# Error for orioledb tables
		_, _, err = node.psql("""
			CLUSTER VERBOSE o_test_1 USING o_ind_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb tables does not support CLUSTER")

		node.stop()

	def test_vacuum_full(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				val_2 int
			)USING orioledb;

			CREATE TABLE pg_test_1 (
				val_1 int,
				val_2 int
			) USING heap;

			INSERT INTO o_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
			INSERT INTO pg_test_1
				(SELECT val_1, val_1 + 10 FROM generate_series(1, 10) AS val_1);
		""")

		# We doesn't break VACUUM FULL for postgres tables
		_, _, err = node.psql("""
			VACUUM (FULL, VERBOSE) pg_test_1;
		""")
		self.assertTrue(err.decode("utf-8").split("\n")[0].find("pg_test_1"))

		# Simple VACUUM works for both tables
		_, _, err = node.psql("""
			VACUUM pg_test_1, o_test_1;
		""")
		self.assertEqual(err.decode("utf-8"), "")

		# Error for orioledb tables
		_, _, err = node.psql("""
			VACUUM (FULL, VERBOSE) o_test_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" does " +
		    "not support VACUUM FULL")

		# Error if at least one table is orioledb
		_, _, err = node.psql("""
			VACUUM (FULL, VERBOSE) pg_test_1, o_test_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" does " +
		    "not support VACUUM FULL")

		# Error if no table specified
		_, _, err = node.psql("""
			VACUUM (FULL, VERBOSE);
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" does " +
		    "not support VACUUM FULL")
		node.stop()

	def test_tablesample(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_test_1(
				val_1 int,
				name text
			)USING orioledb;

			CREATE TABLE pg_test_1 (
				val_1 int,
				name text
			) USING heap;
		""")

		# We doesn't break TABLESAMPLE for postgres tables

		plan = node.execute("""
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT val_1 FROM pg_test_1 TABLESAMPLE SYSTEM (100);
		""")[0][0][0]["Plan"]
		self.assertEqual('Sample Scan', plan["Node Type"])
		self.assertEqual('pg_test_1', plan['Relation Name'])

		# Error for orioledb tables
		_, _, err = node.psql("""
			SELECT t.val_1 FROM o_test_1 AS t
				TABLESAMPLE SYSTEM (50) REPEATABLE (0);
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" does not "
		    "support TABLESAMPLE")

		# Error for deep plans that contain orioledb tables
		_, _, err = node.psql("""
			EXPLAIN (COSTS OFF)
				SELECT * FROM o_test_1 ot
					JOIN (SELECT t.val_1 FROM o_test_1 AS t
							TABLESAMPLE SYSTEM (50)
							REPEATABLE (0)) s1
						ON ot.val_1 = s1.val_1;
		""")
		self.assertEqual(
		    err.decode("utf-8").split("\n")[0],
		    "ERROR:  orioledb table \"o_test_1\" does not "
		    "support TABLESAMPLE")

	def test_prepared_transaction(self):
		node = self.node
		node.append_conf(max_prepared_transactions=2)
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
		""")

		with self.assertRaises(QueryException) as e:
			node.safe_psql("""
				BEGIN;

				CREATE TABLE o_test_1 (val_1 int) USING orioledb;
				INSERT INTO o_test_1 SELECT generate_series(1, 5);
				SELECT * FROM o_test_1;

				PREPARE TRANSACTION 't1';
			""")

		self.assertErrorMessageEquals(e, "cannot use PREPARE TRANSACTION in "
		                              "transaction that uses "
		                              "orioledb table",
		                              "OrioleDB does not support prepared "
		                              "transactions yet.",
		                              second_title="DETAIL")

	def list_available_locales(self):
		try:
			output = subprocess.check_output(['locale', '-a'],
			                                 universal_newlines=True)

			available_locales = output.strip().split('\n')

			return available_locales

		except subprocess.CalledProcessError:
			return []

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_collation_refresh_version(self):
		ONLY_ICU_ERR = ("Only C, POSIX and ICU collations supported for "
		                "orioledb tables")
		node = self.node
		node.append_conf(shared_preload_libraries='')
		node.start()

		node.safe_psql("""
			CREATE COLLATION "my_ICU" (provider='icu',
									   locale = "und-u-kf-upper-kn");
		""")
		old_collversion = node.execute("""
			SELECT collversion FROM pg_collation WHERE collname = 'my_ICU'
		""")[0][0]
		new_collversion = '1.5.2'
		COLLATION_MSG = "collation \"my_ICU\" has version mismatch"
		COLLATION_DETAIL = ("The collation in the database was created using "
		                    f"version {new_collversion}, but the operating "
		                    f"system provides version {old_collversion}.")
		COLLATION_HINT = ("Rebuild all objects affected by this collation and "
		                  "run ALTER COLLATION public.\"my_ICU\" "
		                  "REFRESH VERSION, or build PostgreSQL with the "
		                  "right library version.")

		node.safe_psql(f"""
			UPDATE pg_collation SET collversion = '{new_collversion}'
				WHERE collname = 'my_ICU';
		""")
		node.safe_psql("""
			CREATE TABLE pg_collate (
				t2 text COLLATE "my_ICU"
			);
		""")
		node.stop()
		node.append_conf(shared_preload_libraries='orioledb')
		node.start()
		with self.assertRaises(QueryException) as e:
			node.safe_psql("""
				CREATE INDEX ON pg_collate (t2);
			""")
		self.assertErrorMessageEquals(e, COLLATION_MSG, COLLATION_DETAIL,
		                              "DETAIL", COLLATION_HINT)
		node.safe_psql("""
			ALTER COLLATION "my_ICU" REFRESH VERSION;
		""")
		node.safe_psql("""
			CREATE INDEX ON pg_collate (t2);
		""")

		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE COLLATION "my_ICU2" (provider='icu',
										locale = "und-u-kf-upper-kr-space");
		""")

		locales = self.list_available_locales()

		non_c_posix_locales = [
		    locale for locale in locales if locale not in ('C', 'POSIX')
		]
		self.assertTrue(len(non_c_posix_locales) != 0)
		first_locale = non_c_posix_locales[0]

		with self.assertRaises(QueryException) as e:
			node.safe_psql(f"""
				CREATE TABLE o_only_icu_error (
					t text COLLATE "{first_locale}" NOT NULL
				) USING orioledb;
			""")

		self.assertErrorMessageEquals(e, ONLY_ICU_ERR)
		node.safe_psql("""
			CREATE TABLE o_collate (
				t text COLLATE "C" NOT NULL,
				t2 text COLLATE "my_ICU" NOT NULL
			) USING orioledb;
			INSERT INTO o_collate
				SELECT v.s, v.s FROM (VALUES ('5 1a'), ('50F'),
											 ('10b'), ('1a'), ('1A')) v(s);
		""")

		with self.assertRaises(QueryException) as e:
			node.safe_psql(f"""
				CREATE INDEX o_only_icu_idx_error
					ON o_collate (t COLLATE "{first_locale}");
			""")
		self.assertErrorMessageEquals(e, ONLY_ICU_ERR)

		self.assertEqual(node.execute("SELECT * FROM o_collate"),
		                 [('5 1a', '5 1a'), ('50F', '50F'), ('10b', '10b'),
		                  ('1a', '1a'), ('1A', '1A')])
		node.safe_psql("""
			CREATE INDEX ON o_collate (t);
			CREATE INDEX ON o_collate (t COLLATE "my_ICU2");
		""")
		with node.connect() as con:
			con.execute("SET LOCAL enable_seqscan = OFF;")
			self.assertEqual(
			    con.execute("""
								SELECT * FROM o_collate ORDER BY t;
							 """), [('10b', '10b'), ('1A', '1A'), ('1a', '1a'), ('5 1a', '5 1a'),
			   ('50F', '50F')])
			self.assertEqual(
			    con.execute("""
								SELECT * FROM o_collate
									ORDER BY t COLLATE "my_ICU2";
							 """), [('10b', '10b'), ('1A', '1A'), ('1a', '1a'), ('50F', '50F'),
			   ('5 1a', '5 1a')])
			self.assertEqual(
			    con.execute("""
								SELECT * FROM o_collate ORDER BY t2;
							 """), [('1A', '1A'), ('1a', '1a'), ('5 1a', '5 1a'), ('10b', '10b'),
			   ('50F', '50F')])
			con.commit()

		node.safe_psql(f"""
			UPDATE pg_collation SET collversion = '{new_collversion}'
				WHERE collname = 'my_ICU';
		""")

		with self.assertRaises(QueryException) as e:
			node.safe_psql("""
				CREATE INDEX ON o_collate (t2);
			""")
		self.assertErrorMessageEquals(e, COLLATION_MSG, COLLATION_DETAIL,
		                              "DETAIL", COLLATION_HINT)
		with self.assertRaises(QueryException) as e:
			node.safe_psql("""
				ALTER COLLATION "my_ICU" REFRESH VERSION;
			""")
		self.assertErrorMessageEquals(
		    e, "cannot refresh collation \"my_ICU\" because orioledb table "
		    "\"o_collate\" uses it")
		with node.connect() as con:
			con.execute("SET LOCAL enable_seqscan = OFF;")
			with self.assertRaises(DatabaseError) as e:
				con.execute("SELECT * FROM o_collate ORDER BY t2;")
			self.assertErrorMessageEquals(e, COLLATION_MSG, COLLATION_DETAIL,
			                              "DETAIL", COLLATION_HINT)
		node.safe_psql("""
			ALTER TABLE o_collate ALTER t2 TYPE text COLLATE "C";
		""")
		with node.connect() as con:
			con.execute("SET LOCAL enable_seqscan = OFF;")
			self.assertEqual(
			    con.execute("""
								SELECT * FROM o_collate ORDER BY t2;
							 """), [('10b', '10b'), ('1A', '1A'), ('1a', '1a'), ('5 1a', '5 1a'),
			   ('50F', '50F')])
			con.commit()
		node.safe_psql("""
			ALTER COLLATION "my_ICU" REFRESH VERSION;
		""")

	def test_temp_compression(self):
		node = self.node
		node.append_conf(max_prepared_transactions=2)
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
		""")

		with self.assertRaises(QueryException) as e:
			node.safe_psql("""
				CREATE TEMP TABLE o_test_1 (
					val_1 int
				) USING orioledb WITH (compress = 1);

				CHECKPOINT;
			""")

		self.assertErrorMessageEquals(
		    e, "temp and unlogged orioledb tables does not "
		    "support compression options")
