#!/usr/bin/env python3
# coding: utf-8

import unittest
import re, os
from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent
from testgres.exceptions import QueryException


class TypesTest(BaseTest):
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

	def test_enum_index_recovery(self):
		enum_amount = 0
		enumoid_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE o_happiness AS ENUM ('happy', 'very happy',
											 'ecstatic');

			CREATE TABLE o_holidays (
				num_weeks integer NOT NULL,
				happiness o_happiness NOT NULL,
				PRIMARY KEY (happiness)
			) USING orioledb;

			ALTER TYPE o_happiness ADD VALUE 'sad' BEFORE 'very happy';
		""")

		node.safe_psql(
		    'postgres', """
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (2, 'happy');
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (4, 'sad');
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (6, 'very happy');
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (8, 'ecstatic');
		""")

		enum_amount += 4  # 'happy', 'sad', 'very happy', 'ecstatic'
		enumoid_amount += 4  # 'happy', 'sad', 'very happy', 'ecstatic'
		self.check_total_deleted(node, 'ENUM_CACHE', enum_amount, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', enumoid_amount, 0)
		node.safe_psql('postgres', "DROP TABLE o_holidays;")
		node.safe_psql('postgres', "DROP TYPE o_happiness;")
		self.check_total_deleted(node, 'ENUM_CACHE', enum_amount, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', enumoid_amount, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		# deleted records in o_enum_cache physically deleted during checkpoint
		# performed after recovery
		self.check_total_deleted(node, 'ENUM_CACHE', enum_amount, 4)
		self.check_total_deleted(node, 'ENUMOID_CACHE', enumoid_amount, 4)
		node.stop()

	def test_enum_cache_namedata_in_key(self):
		enum_amount = 0
		enumoid_amount = 0
		node = self.node
		node.start()

		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE o_happiness AS ENUM ('happy', 'very happy',
											 'ecstatic');

			CREATE TABLE o_holidays (
				num_weeks integer NOT NULL,
				happiness o_happiness NOT NULL,
				PRIMARY KEY (happiness)
			) USING orioledb;

			ALTER TYPE o_happiness ADD VALUE 'sad' BEFORE 'very happy';
		""")

		node.safe_psql(
		    'postgres', """
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (2, 'happy');
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (4, 'sad');
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (6, 'very happy');
			INSERT INTO o_holidays(num_weeks, happiness)
				VALUES (8, 'ecstatic');
		""")

		node.safe_psql("""
			ALTER TYPE o_happiness RENAME VALUE 'sad' TO 'depressed';
		""")

		enum_amount += 5  # 'happy', 'sad', 'very happy', 'ecstatic', 'depressed'
		enumoid_amount += 4  # 'happy', 'depressed', 'very happy', 'ecstatic'
		self.check_total_deleted(node, 'ENUM_CACHE', enum_amount, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', enumoid_amount, 0)
		node.safe_psql('postgres', "DROP TABLE o_holidays;")
		node.safe_psql('postgres', "DROP TYPE o_happiness;")
		self.check_total_deleted(node, 'ENUM_CACHE', enum_amount, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', enumoid_amount, 0)
		self.assertEqual(
		    ['depressed', 'ecstatic', 'happy', 'sad', 'very happy'], [
		        x[0] for x in node.execute("""
							SELECT k->'key'->'keys'->1
								FROM orioledb_sys_tree_rows(%d) k ORDER BY 1;
						 """ % self.sys_tree_name_to_num('ENUM_CACHE'))
		    ])
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
		    ['depressed', 'ecstatic', 'happy', 'sad', 'very happy'], [
		        x[0] for x in node.execute("""
							SELECT k->'key'->'keys'->1
								FROM orioledb_sys_tree_rows(%d) k ORDER BY 1;
						 """ % self.sys_tree_name_to_num('ENUM_CACHE'))
		    ])
		# deleted records in o_enum_cache physically deleted during checkpoint
		# performed after recovery
		self.check_total_deleted(node, 'ENUM_CACHE', enum_amount, 5)
		self.check_total_deleted(node, 'ENUMOID_CACHE', enumoid_amount, 4)
		node.stop()

	def test_array_index_recovery(self):
		type_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				arr integer[] NOT NULL,
				PRIMARY KEY (arr)
			) USING orioledb;
		""")
		type_amount += 3  # int2, int4, tid - types needed for all our tables
		type_amount += 1  # int8 - hash_array_extended return type
		type_amount += 1  # anyarray
		type_amount += 1  # internal - argument of sort support function
		type_amount += 1  # void - return type of sort support function
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)

		node.execute("INSERT INTO o_test VALUES ('{1, 2}');")
		node.execute("INSERT INTO o_test VALUES ('{2, 3, 4}');")
		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)
		self.assertEqual(2, node.execute("SELECT COUNT(*) FROM o_test;")[0][0])
		node.stop()

	def test_range_index_recovery(self):
		range_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE custom_range as range (subtype=int8);
			CREATE TABLE o_test_custom_range
			(
				area custom_range NOT NULL,
				val int NOT NULL,
				PRIMARY KEY(area)
			) USING orioledb;
			INSERT INTO o_test_custom_range
				SELECT custom_range(id * 5, id * 5 + 5),
					   id * 10 val
					FROM generate_series(1, 10) id;

			CREATE TYPE custom_range_removed as range (subtype=int8);
			CREATE TABLE o_test_custom_range_removed
			(
				key custom_range_removed NOT NULL,
				PRIMARY KEY(key)
			) USING orioledb;
		""")
		node.safe_psql('postgres', """
			DROP TYPE custom_range_removed CASCADE;
		""")

		range_amount += 2  # custom_range, custom_range_removed
		self.check_total_deleted(node, 'RANGE_CACHE', range_amount, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'RANGE_CACHE', range_amount, 1)
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_test_custom_range;")[0][0],
		    10)
		node.stop()

	def test_parallel_sys_cache_insert(self):
		range_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE custom_range as range (subtype=int8);
			CREATE TABLE o_test_custom_range
			(
				key int NOT NULL,
				area custom_range NOT NULL
			) USING orioledb;
		""")

		with node.connect() as con1:
			with node.connect() as con2:
				with node.connect() as con3:
					connection1_pid = con1.pid
					con1.execute("SET orioledb.trace_stopevents = true;")
					con1.execute("SET orioledb.enable_stopevents = true;")
					con3.execute("""
							SELECT pg_stopevent_set('modify_start',
								'$.treeName == "sys_tree" &&
								 $.reloid == %d');
						""" % (self.sys_tree_name_to_num('RANGE_CACHE')))

					t1 = ThreadQueryExecutor(
					    con1, """
						CREATE INDEX o_test_custom_range_idx1 ON
							o_test_custom_range (area);
					""")
					t1.start()
					wait_stopevent(node, connection1_pid)

					t2 = ThreadQueryExecutor(
					    con2, """
						CREATE TABLE o_test_custom_range2
						(
							area custom_range NOT NULL,
							val int NOT NULL,
							PRIMARY KEY(area)
						) USING orioledb;
					""")
					t2.start()
					con3.execute("SELECT pg_stopevent_reset('modify_start');")
					t1.join()
					t2.join()
					con1.commit()
					con2.commit()
		range_amount += 1  # single custom_range
		self.check_total_deleted(node, 'RANGE_CACHE', range_amount, 0)
		node.stop()

	def test_record_index_recovery(self):
		class_amount = 0
		type_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE coordinates AS (
				x int,
				y int
			);
			CREATE TYPE coordinates2 AS (
				x int,
				y int
			);
			CREATE TABLE o_test_record_type
			(
				location coordinates NOT NULL,
				val int NOT NULL,
				val2 coordinates2,
				PRIMARY KEY(location)
			) USING orioledb;
		""")
		class_amount += 1  # coordinates
		class_amount += 2  # pg_type
		class_amount += 2  # pg_proc
		class_amount += 4  # pg_amproc, pg_opclass, pg_amop, pg_authid
		type_amount += 3  # int2, int4, tid - types needed for all our tables
		type_amount += 1  # int8 - hash_array_extended return type
		type_amount += 1  # record
		type_amount += 1  # internal - argument of sort support function
		type_amount += 1  # void - return type of sort support function
		type_amount += 1  # coordinates
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)
		node.safe_psql(
		    'postgres', """
			ALTER TYPE coordinates2 RENAME TO coordinates_renamed;
			CREATE TYPE custom_type AS (a int, b float);
			ALTER TYPE coordinates_renamed ADD ATTRIBUTE z custom_type;
			ALTER TYPE coordinates_renamed DROP ATTRIBUTE y;
			ALTER TYPE coordinates_renamed RENAME ATTRIBUTE x TO y;

			INSERT INTO o_test_record_type
				SELECT
					(id, id * 5)::coordinates,
					id * 10 val,
					(id, (id, id * 5)::custom_type)::coordinates_renamed
					FROM generate_series(1, 10) id;

			CREATE TYPE coordinates_removed AS (x int, y int);
			CREATE TABLE o_test_record_type_removed
			(
				key coordinates_removed NOT NULL,
				PRIMARY KEY(key)
			) USING orioledb;
		""")
		class_amount += 1  # coordinates_removed
		type_amount += 1  # coordinates_removed
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)

		node.safe_psql(
		    'postgres', """
			CREATE TYPE custom_type_removed AS (a char, b text);
		""")
		with self.assertRaises(QueryException):
			node.safe_psql(
			    'postgres', """
				ALTER TYPE coordinates_removed
					ALTER ATTRIBUTE y TYPE custom_type_removed;
			""")
		node.safe_psql(
		    'postgres', """
			DROP TABLE o_test_record_type_removed;
			ALTER TYPE coordinates_removed
				ALTER ATTRIBUTE y TYPE custom_type_removed;
		""")

		node.safe_psql(
		    'postgres', """
			CREATE TABLE o_test_record_type_removed2
			(
				key coordinates_removed NOT NULL,
				PRIMARY KEY(key)
			) USING orioledb;
			INSERT INTO o_test_record_type_removed2
				SELECT
					(id,
					 (to_char(id, '9'),
					  id::text)::custom_type_removed
					 )::coordinates_removed
					FROM generate_series(1, 10) id;
		""")
		class_amount += 1  # custom_type_removed
		type_amount += 3  # custom_type_removed, bpchar, text
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)

		with self.assertRaises(QueryException):
			node.safe_psql(
			    'postgres', """
				DROP TYPE custom_type_removed CASCADE;
			""")
		node.safe_psql(
		    'postgres', """
			ALTER TABLE o_test_record_type_removed2
				DROP CONSTRAINT o_test_record_type_removed2_pkey;
			DROP TYPE custom_type_removed CASCADE;
		""")
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 1)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 1)
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_test_record_type;")[0][0], 10)
		node.stop()

	def test_record_array_index_recovery(self):
		class_amount = 0
		type_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE coordinates AS (
				x int,
				y int
			);

			CREATE TABLE o_test_array
			(
				locations coordinates[] NOT NULL,
				id int NOT NULL,
				PRIMARY KEY(locations)
			) USING orioledb;

			INSERT INTO o_test_array
				SELECT ARRAY[(id, id * 2)::coordinates,
							 (id, id * 10)::coordinates], id
					FROM generate_series(1, 10) id;

			CREATE TYPE coordinates_removed AS (x int, y int);
			CREATE TABLE o_test_record_type_removed
			(
				key coordinates_removed[] NOT NULL,
				PRIMARY KEY(key)
			) USING orioledb;
		""")

		node.safe_psql('postgres', """
			DROP TYPE coordinates_removed CASCADE;
		""")
		class_amount += 1  # coordinates
		class_amount += 1  # coordinates_removed
		class_amount += 2  # pg_type
		class_amount += 2  # pg_proc
		class_amount += 4  # pg_amproc, pg_opclass, pg_amop, pg_authid
		type_amount += 3  # int2, int4, tid - types needed for all our tables
		type_amount += 1  # int8
		type_amount += 2  # record, anyarray
		type_amount += 1  # internal - argument of sort support function
		type_amount += 1  # void - return type of sort support function
		type_amount += 2  # coordinates, coordinates_removed
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'CLASS_CACHE', class_amount, 1)
		self.check_total_deleted(node, 'TYPE_CACHE', type_amount, 1)
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_test_array;")[0][0], 10)
		node.stop()

	def test_complex_index_recovery(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE happiness AS ENUM ('happy', 'very happy',
										   'ecstatic', 'ecstatic2',
										   'ecstatic3', 'ecstatic4');
			CREATE TYPE happiness_record AS (
				v1 happiness,
				v2 int
			);
			CREATE TYPE happiness_range as range (subtype=happiness_record);

			CREATE TYPE custom_range as range (subtype=int8);
			CREATE TYPE custom_enum AS ENUM ('enum1', 'enum2', 'enum3');

			CREATE TYPE int_record AS (x int, y int);
			CREATE DOMAIN myint AS int_record;

			CREATE TYPE complex_type0 AS (
				x text, y date, z date,
				a int8, b int8, c int8,
				d int8, e int8, f int2,
				g int8, h int8, i int8,
				j int8, k int8, l int8,
				m int8, n int8, o int8,
				p myint[], q custom_enum,
				r int8range,
				s custom_range,
				t happiness_range
			);

			CREATE TYPE complex_type1 AS (
				a text,
				b date,
				c timestamp[],
				coc complex_type0[]
			);

			CREATE TYPE complex_type2 AS (
				a complex_type1[],
				b char[]
			);

			CREATE TABLE o_test_complex_type
			(
				structure complex_type2[] NOT NULL,
				val1 int NOT NULL,
				val2 int NOT NULL,
				PRIMARY KEY(structure)
			) USING orioledb;

			INSERT INTO o_test_complex_type
			SELECT
				ARRAY[
					(
						ARRAY[
							(id::text, '2001-10-05',
							ARRAY['2001-09-28 01:00:00'::timestamp],
							ARRAY[('x', '2001-05-02', '2001-05-02',
									id + 1, id + 2, id + 3,
									id + 4, id + 5, id + 6,
									id + 7, id + 8, id + 9,
									id + 10, id + 11, id + 12,
									id + 13, id + 14, id + 15,
									ARRAY[(id + 1, id + 2)::int_record],
									'enum1'::custom_enum,
									int8range(1, 200000),
									custom_range(1, 200000),
									happiness_range(
										('very happy', id)::happiness_record,
										('ecstatic2', id + 5)::happiness_record
									)
								)::complex_type0]
						)::complex_type1],
						ARRAY['a', 'b', 'c']
					)::complex_type2
				] structure,
				id val1,
				id val2
			FROM generate_series(1, 10) id;""")
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_test_complex_type;")[0][0],
		    10)
		self.assertEqual(
		    node.execute("""SELECT structure[1].a[1].a
							FROM o_test_complex_type LIMIT 1;""")[0][0], '1')
		self.assertEqual(
		    node.execute("""SELECT structure[1].a[1].coc[1].p[1]
							FROM o_test_complex_type LIMIT 1;""")[0][0], '(2,3)')
		node.stop()

	def test_collation_recovery(self):
		collation_amount = 0
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE COLLATION test_coll (LOCALE="POSIX");
			CREATE COLLATION test_coll2 (LOCALE="POSIX");
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
			INSERT INTO o_test VALUES ('X', 'R'), ('A', 'T'), ('W', 'A'),
									  ('V', 'C'), ('C', 'N');
		""")
		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'COLLATION_CACHE', collation_amount, 0)
		self.assertEqual([('A', 'T'), ('C', 'N'), ('V', 'C'), ('W', 'A'),
		                  ('X', 'R')],
		                 node.execute("SELECT * FROM o_test ORDER BY key;"))
		node.stop()

	@unittest.skipIf(BaseTest.get_pg_version() < 14,
	                 'Multiranges added in postgres 14')
	def test_multirange_index_recovery(self):
		node = self.node
		node.start()

		with node.connect() as con:
			con.execute("""
				CREATE EXTENSION IF NOT EXISTS orioledb;

				CREATE TABLE o_test_1 (
					val_1 int4multirange
				) USING orioledb;

				INSERT INTO o_test_1
					SELECT int4multirange(int4range(g, g+10),
						int4range(g+20, g+30),int4range(g+40, g+50))
							FROM generate_series(1,1000) g;

				CREATE INDEX ind_1 ON o_test_1 (val_1);

				SELECT count(*) FROM o_test_1
					WHERE val_1 = '{}'::int4multirange;
			""")

			con.execute("""
				CHECKPOINT;
			""")
			con.commit()

		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_test_1")[0][0], 1000)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_test_1")[0][0], 1000)
		node.stop()
