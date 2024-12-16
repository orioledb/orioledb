#!/usr/bin/env python3
# coding: utf-8

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
