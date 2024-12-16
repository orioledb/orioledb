#!/usr/bin/env python3
# coding: utf-8

import subprocess
import unittest
import re, os

from testgres.connection import DatabaseError
from testgres.exceptions import QueryException

from .base_test import BaseTest
from .base_test import generate_string as gen_str


class CollateTest(BaseTest):
	sys_tree_nums = {}

	@classmethod
	def setUpClass(cls):
		cls.parse_sys_tree_names()

	@classmethod
	def parse_sys_tree_names(cls):
		dirname = os.path.dirname(__file__)
		filename = os.path.join(dirname, '../include/catalog/sys_trees.h')
		f = open(filename, 'r')
		pattern = re.compile(r"^#define SYS_TREES_(\w+)\s+\((\d+)\)")
		line = f.readline()
		while line:
			search_result = re.search(pattern, line)
			if search_result and search_result.group(1) != 'NUM':
				cls.sys_tree_nums[search_result.group(1)] = int(
				    search_result.group(2))
			line = f.readline()
		f.close()

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_collation_simple(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;

			CREATE TABLE o_collate (
				t text COLLATE "en-x-icu" NOT NULL,
				PRIMARY KEY(t)
			) USING orioledb;
			DROP TABLE IF EXISTS o_collate;

			CREATE TABLE o_collate (
				t text COLLATE "POSIX" NOT NULL,
				PRIMARY KEY(t)
			) USING orioledb;
			DROP TABLE IF EXISTS o_collate;

			CREATE TABLE o_collate (
				t text COLLATE "C" NOT NULL,
				PRIMARY KEY(t)
			) USING orioledb;

			CREATE UNIQUE INDEX o_collate_idx_c on o_collate(t COLLATE "C");
			CREATE UNIQUE INDEX o_collate_idx_posix on o_collate(t COLLATE "POSIX");
			CREATE UNIQUE INDEX o_collate_idx_icu on o_collate(t COLLATE "en-x-icu");

			INSERT INTO o_collate VALUES ('x'), ('X'), ('y'), ('Y'), ('z'), ('Z');

			ANALYZE o_collate;
		""")

		self.assertEqual(
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_collate;
						 """), [('x', ), ('X', ), ('y', ), ('Y', ), ('z', ), ('Z', )])

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_collate WHERE t >= 'y';
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_collate_idx_c', plan['Index Name'])

		self.assertEqual(
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_collate WHERE t >= 'y';
						 """), [('y', ), ('z', )])

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_collate WHERE t >= 'y' COLLATE "C";
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_collate_idx_c', plan['Index Name'])

		self.assertEqual(
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_collate WHERE t >= 'y' COLLATE "C";
						 """), [('y', ), ('z', )])

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_collate WHERE t >= 'y' COLLATE "POSIX";
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_collate_idx_posix', plan['Index Name'])

		self.assertEqual(
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_collate WHERE t >= 'y' COLLATE "POSIX";
						 """), [('y', ), ('z', )])

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_collate WHERE t >= 'y' COLLATE "en-x-icu";
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_collate_idx_icu', plan['Index Name'])

		self.assertEqual(
		    node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_collate WHERE t >= 'y' COLLATE "en-x-icu";
						 """), [('y', ), ('Y', ), ('z', ), ('Z', )])

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_alter_column_collation(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_ddl_check
			(
				f1 text NOT NULL,
				f2 varchar NOT NULL,
				f3 integer,
				PRIMARY KEY (f1)
			) USING orioledb;

			INSERT INTO o_ddl_check VALUES ('ABC1', 'ABC2', NULL);
			INSERT INTO o_ddl_check VALUES ('ABC2', 'ABC4', NULL);
			INSERT INTO o_ddl_check VALUES ('ABC3', 'ABC6', NULL);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])

		node.safe_psql("""
			ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "en-x-icu";
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None)])
		node.safe_psql("""
			INSERT INTO o_ddl_check VALUES ('ABC4', 'D', NULL);
		""")
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None), ('ABC4', 'D', None)])
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(node.execute('SELECT * FROM o_ddl_check;'),
		                 [('ABC1', 'ABC2', None), ('ABC2', 'ABC4', None),
		                  ('ABC3', 'ABC6', None), ('ABC4', 'D', None)])
		node.stop()

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_database_encoding_no_trx(self):
		node = self.node
		node.start()
		node.stop(['-m', 'immediate'])
		node.start()
		node.stop()

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

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_all_databases_need_one_encoding(self):
		node = self.node
		node.start()
		with self.assertRaises(QueryException) as e:
			if self.get_pg_version() >= 16:
				node.safe_psql("""
					CREATE DATABASE encoding_test
						ENCODING SQL_ASCII
						LOCALE_PROVIDER libc
						TEMPLATE 'template0';
				""")
			else:
				node.safe_psql("""
					CREATE DATABASE encoding_test
						ENCODING SQL_ASCII
						TEMPLATE 'template0';
				""")

		self.assertErrorMessageEquals(
		    e, "Cannot create database with encoding \"SQL_ASCII\" "
		    "that is different from cluster encoding \"UTF8\"",
		    "OrioleDB now only supports single encoding for all databases. "
		    "It is easier to use single one during checkpoint.", "DETAIL")

		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		node.safe_psql(
		    'postgres', "CREATE TABLE IF NOT EXISTS o_toast_index (\n"
		    "	id integer PRIMARY KEY,\n"
		    "	v1 text,\n"
		    "	v2 text,\n"
		    "	v3 text\n"
		    ") USING orioledb;\n"
		    "CREATE COLLATION test_coll (LOCALE=\"de-DE-x-icu\", PROVIDER=\"ICU\");\n"
		    "CREATE INDEX o_toast_index_v1_idx ON o_toast_index(v1 COLLATE test_coll);\n"
		)

		for i in range(0, 10):
			baseIndex = i * 3 + 1
			node.safe_psql(
			    "INSERT INTO o_toast_index VALUES (%d, '%s', '%s', '%s')" %
			    (i + 1, gen_str(2500, baseIndex), gen_str(
			        2500, baseIndex + 1), gen_str(2500, baseIndex + 2)))
		con = node.connect()
		con.execute("CHECKPOINT;")
		con.begin()
		for j in range(1, 5):
			for i in range(0, 10):
				baseIndex = j * 30 + i * 3 + 1
				con.execute(
				    "UPDATE o_toast_index SET v1 = '%s', v2 = '%s', v3 = '%s' WHERE id = %d"
				    % (gen_str(2500, baseIndex), gen_str(2500, baseIndex + 1),
				       gen_str(2500, baseIndex + 2), i + 1))
		con.commit()
		con.execute("CHECKPOINT;")
		node.stop()

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_no_creating_databases_checkpointer_encoding(self):
		node = self.node
		node.start()
		node.execute("CHECKPOINT;")
		node.stop()

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_collation_icu_recovery(self):
		collation_amount = 0
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE COLLATION test_coll (LOCALE="de-DE-x-icu", PROVIDER="ICU");
			CREATE COLLATION test_coll2 (LOCALE="fr-FR-x-icu", PROVIDER="ICU");
			CREATE TABLE IF NOT EXISTS o_test (
				key text NOT NULL COLLATE test_coll,
				val text,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE INDEX o_test_ix1 ON o_test (val COLLATE test_coll2);
			CREATE INDEX o_test_ix2
				ON o_test ((val || 'U') COLLATE test_coll2);
		""")
		collation_amount += 1  # default
		collation_amount += 1  # test_coll
		collation_amount += 1  # test_coll2
		self.check_total_deleted(node, 'COLLATION_CACHE', collation_amount, 0)

		node.execute("""
			INSERT INTO o_test VALUES ('X', 'â'), ('A', 'T'), ('W', 'A'),
									  ('V', 'a'), ('C', 'N');
		""")

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY val COLLATE test_coll2;
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		self.assertEqual([('V', 'a'), ('W', 'A'), ('X', 'â'), ('C', 'N'),
		                  ('A', 'T')],
		                 node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test
								ORDER BY val COLLATE test_coll2;
						 """))
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'COLLATION_CACHE', collation_amount, 0)
		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY val COLLATE test_coll2;
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		self.assertEqual([('V', 'a'), ('W', 'A'), ('X', 'â'), ('C', 'N'),
		                  ('A', 'T')],
		                 node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test
								ORDER BY val COLLATE test_coll2;
						 """))
		node.stop()

	@unittest.skipIf(not BaseTest.pg_with_icu(),
	                 'postgres built without ICU support')
	def test_collation_multiple_dbs_recovery(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE DATABASE encoding_test;
		""")
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE COLLATION test_coll (LOCALE="de-DE-x-icu", PROVIDER="ICU");
			CREATE COLLATION test_coll2 (LOCALE="fr-FR-x-icu", PROVIDER="ICU");
			CREATE TABLE IF NOT EXISTS o_test (
				key text NOT NULL COLLATE test_coll,
				val text,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE INDEX o_test_ix1 ON o_test (val COLLATE test_coll2);
			CREATE INDEX o_test_ix2
				ON o_test ((val || 'U') COLLATE test_coll2);
			INSERT INTO o_test VALUES ('X', 'â'), ('A', 'T'), ('W', 'A'),
									  ('V', 'a'), ('C', 'N');
		""")
		node.safe_psql(
		    'encoding_test', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE COLLATION test_coll (LOCALE="C");
			CREATE COLLATION test_coll2 (LOCALE="POSIX");
			CREATE TABLE IF NOT EXISTS o_test (
				key text NOT NULL COLLATE test_coll,
				val text,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE INDEX o_test_ix1 ON o_test (val COLLATE test_coll2);
			CREATE INDEX o_test_ix2
				ON o_test ((val || 'U') COLLATE test_coll2);
			INSERT INTO o_test VALUES ('X', 'a'), ('A', 'T'), ('W', 'A'),
									('V', 'a'), ('C', 'N');
		""")

		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY val COLLATE test_coll2;
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		self.assertEqual([('V', 'a'), ('W', 'A'), ('X', 'â'), ('C', 'N'),
		                  ('A', 'T')],
		                 node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test
								ORDER BY val COLLATE test_coll2;
						 """))

		with node.connect("encoding_test") as con:
			plan = con.execute("""
				SET LOCAL enable_seqscan = off;
				EXPLAIN (COSTS OFF, FORMAT JSON)
					SELECT * FROM o_test ORDER BY val COLLATE test_coll2;
			""")[0][0][0]["Plan"]
			if plan["Node Type"] == 'Result':
				plan = plan['Plans'][0]
			self.assertEqual('Index Only Scan', plan["Node Type"])
			self.assertEqual('o_test_ix1', plan['Index Name'])
			self.assertEqual([('W', 'A'), ('C', 'N'), ('A', 'T'), ('V', 'a'),
			                  ('X', 'a')],
			                 con.execute("""
								SET LOCAL enable_seqscan = off;
								SELECT * FROM o_test
									ORDER BY val COLLATE test_coll2;
							"""))
		node.stop(['-m', 'immediate'])

		node.start()
		plan = node.execute("""
			SET LOCAL enable_seqscan = off;
			EXPLAIN (COSTS OFF, FORMAT JSON)
				SELECT * FROM o_test ORDER BY val COLLATE test_coll2;
		""")[0][0][0]["Plan"]
		if plan["Node Type"] == 'Result':
			plan = plan['Plans'][0]
		self.assertEqual('Index Only Scan', plan["Node Type"])
		self.assertEqual('o_test_ix1', plan['Index Name'])
		self.assertEqual([('V', 'a'), ('W', 'A'), ('X', 'â'), ('C', 'N'),
		                  ('A', 'T')],
		                 node.execute("""
							SET LOCAL enable_seqscan = off;
							SELECT * FROM o_test
								ORDER BY val COLLATE test_coll2;
						 """))

		with node.connect("encoding_test") as con:
			plan = con.execute("""
				SET LOCAL enable_seqscan = off;
				EXPLAIN (COSTS OFF, FORMAT JSON)
					SELECT * FROM o_test ORDER BY val COLLATE test_coll2;
			""")[0][0][0]["Plan"]
			if plan["Node Type"] == 'Result':
				plan = plan['Plans'][0]
			self.assertEqual('Index Only Scan', plan["Node Type"])
			self.assertEqual('o_test_ix1', plan['Index Name'])
			self.assertEqual([('W', 'A'), ('C', 'N'), ('A', 'T'), ('V', 'a'),
			                  ('X', 'a')],
			                 con.execute("""
								SET LOCAL enable_seqscan = off;
								SELECT * FROM o_test
									ORDER BY val COLLATE test_coll2;
							"""))
		node.stop()

	def sys_tree_name_to_num(self, name):
		return self.sys_tree_nums.get(name, 9999)

	def check_total_deleted(self, node, sys_tree_name, total, deleted):
		rows = node.execute("""
			SELECT k->'tupHdr'->'deleted', COUNT(k)
				FROM orioledb_sys_tree_rows(%d) k GROUP BY 1 ORDER BY 1;
		""" % self.sys_tree_name_to_num(sys_tree_name))
		cur_total = sum(x[1] for x in rows)
		cur_deleted = next((x[1] for x in rows if x[0] == True), 0)
		self.assertEqual((cur_total, cur_deleted), (total, deleted))

	def list_available_locales(self):
		try:
			output = subprocess.check_output(['locale', '-a'],
			                                 universal_newlines=True)

			available_locales = output.strip().split('\n')

			return available_locales

		except subprocess.CalledProcessError:
			return []
