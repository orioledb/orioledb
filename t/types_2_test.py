#!/usr/bin/env python3
# coding: utf-8

import re, os
from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent

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
				cls.sys_tree_nums[search_result.group(1)] = int(search_result.group(2))
			line = f.readline()
		f.close()

	def sys_tree_name_to_num(self, name):
		return self.sys_tree_nums.get(name, 9999)

	def check_total_deleted(self, node, sys_tree_name, total, deleted):
		self.assertEqual(
			node.execute('postgres',
						 'SELECT orioledb_sys_tree_rows(' +
						 str(self.sys_tree_name_to_num(sys_tree_name)) +
						 ');')[0][0],
			"(%d,%d)" % (total, deleted))


	def test_enum_index_recovery_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE type_numbers AS ENUM ('zero', 'one', 'two', 'three');
			CREATE TABLE table_numbers (
				number type_numbers NOT NULL,
				num INT NOT NULL,
				PRIMARY KEY (number)
			) USING orioledb;

			ALTER TYPE type_numbers ADD VALUE 'five';
		""")

		node.safe_psql('postgres', """
			INSERT INTO table_numbers(number, num)
				VALUES ('zero', 0);
			INSERT INTO table_numbers(number, num)
				VALUES ('one', 1);
			INSERT INTO table_numbers(number, num)
				VALUES ('two', 2);
			INSERT INTO table_numbers(number, num)
				VALUES ('three', 3);
			INSERT INTO table_numbers(number, num)
				VALUES ('five', 5);
		""")

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 5, 0)

		node.safe_psql('postgres', "DROP TABLE table_numbers")
		node.safe_psql('postgres', "DROP TYPE type_numbers")

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 5, 0)

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 1)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 5, 5)

		node.stop()

	def test_enum_index_recovery_2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE one AS ENUM('111', '222', '333');
			CREATE TYPE two AS ENUM('444', '555', '666');

			CREATE TABLE num_table(
				num_1 integer NOT NULL,
				num_2 one NOT NULL,
				num_3 two NOT NULL,
				PRIMARY KEY(num_1)
			)USING orioledb;
		""")

		node.safe_psql('postgres', """
			INSERT INTO num_table(num_1, num_2, num_3)
				VALUES (1, '111', '444');
			INSERT INTO num_table(num_1, num_2, num_3)
				VALUES(2, '222', '555');
			INSERT INTO num_table(num_1, num_2, num_3)
				VALUES(3, '333', '666');
		""")
		self.check_total_deleted(node, 'ENUM_CACHE', 0, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 0, 0)

		node.safe_psql('postgres', "ALTER TABLE num_table DROP COLUMN num_3;")
		node.safe_psql('postgres', "DROP TYPE two;")

		self.check_total_deleted(node, 'ENUM_CACHE', 0, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 0, 0)

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'ENUM_CACHE', 0, 0)
		self.check_total_deleted(node, "ENUMOID_CACHE", 0, 0)

		node.stop()

	def test_enum_index_recovery_3(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
		CREATE EXTENSION IF NOT EXISTS orioledb;

		CREATE TYPE num_1 AS ENUM ('11','12','13');
		CREATE TYPE num_2 AS ENUM ('21','22','23');
		CREATE TYPE num_3 AS ENUM ('31','32','33');

		CREATE TABLE table_1 (
			key_1 int NOT NULL,
			value_1 num_1 NOT NULL,
			value_2 num_2 NOT NULL,
			PRIMARY KEY(key_1)
		)USING orioledb;

		CREATE TABLE table_2(
			key_2 int NOT NULL,
			value_3 int NOT NULL,
			value_4 num_3 NOT NULL,
			PRIMARY KEY(key_2)
		)USING orioledb;

		INSERT INTO table_1 (key_1, value_1, value_2)
			VALUES(1, '11', '21');
		INSERT INTO table_1 (key_1, value_1, value_2)
			VALUES(2,'12','22');
		INSERT INTO table_1 (key_1, value_1, value_2)
			VALUES(3,'13','23');


		INSERT INTO table_2 (key_2, value_3, value_4)
			VALUES(4,7,'31');
		INSERT INTO table_2 (key_2, value_3, value_4)
			VALUES(5,8,'32');
		INSERT INTO table_2 (key_2, value_3, value_4)
			VALUES(6,9,'33');
		""")
		self.check_total_deleted(node, 'ENUM_CACHE', 0, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 0, 0)

		node.safe_psql('postgres', """
			DROP TYPE num_2 CASCADE;
		""")
		self.check_total_deleted(node, 'ENUM_CACHE', 0, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 0, 0)

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'ENUM_CACHE', 0, 0)
		self.check_total_deleted(node, 'ENUMOID_CACHE', 0, 0)

		node.stop()

	def test_array_index_recovery_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS name_table (
				num_arr integer[] NOT NULL,
				num_arr_1 integer[] NOT NULL,
				num_arr_2 integer[] NOT NULL,
				PRIMARY KEY (num_arr)
			) USING orioledb;
		""")
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		node.execute("INSERT INTO name_table VALUES ('{1,3,5,7,9}','{2,3}','{4,5}');")
		node.execute("INSERT INTO name_table VALUES ('{3,3,3,3,3}','{2,3}','{4,5}');")
		node.execute("INSERT INTO name_table VALUES ('{1,2,3}','{2,3}','{4,5}');")
		node.execute("INSERT INTO name_table VALUES ('{2,4,6,8}','{2,3}','{4,5}');")

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)
		self.assertEqual(4, node.execute("SELECT COUNT(*) FROM name_table")[0][0])
		node.stop()

	def test_array_index_recovery_2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS table_num (
				a_num integer[] NOT NULL,
				b_num varchar(50)[] NOT NULL,
				PRIMARY KEY(a_num)
			) USING orioledb;
		""")
		node.execute("INSERT INTO table_num VALUES ('{1,11,111}','{\"2\",\"22\",\"222\"}');")
		node.execute("INSERT INTO table_num VALUES ('{3,33,333}','{\"4\",\"44\",\"444\"}');")

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)
		self.assertEqual(2, node.execute("SELECT COUNT(*) FROM table_num;")[0][0])

		node.stop()

	def test_record_index_recovery_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE num_1 AS (a integer);
			CREATE TYPE num_2 AS (b integer);

			CREATE TABLE num_table(
				val_1 integer NOT NULL,
				val_2 num_1 NOT NULL,
				val_3 num_2 NOT NULL,
				PRIMARY KEY(val_1)
			)USING orioledb;

			INSERT INTO num_table(val_1, val_2, val_3)
				VALUES(1, '(5)'::num_1, '(2)');
			INSERT INTO num_table(val_1, val_2, val_3)
				VALUES(2, '(2)', '(5)');
			INSERT INTO num_table(val_1, val_2, val_3)
				VALUES(3, '(2)', '(4)');
		""")
		node.safe_psql('postgres', """
			DROP TYPE num_1 CASCADE;
		""")
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM num_table;")[0][0],
			3)
		node.stop()

	def test_range_index_recovery_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE type_range_1 AS RANGE(subtype=int8);

			CREATE TABLE table_range(
				num_1 type_range_1 NOT NULL,
				num_2 int NOT NULL,
				PRIMARY KEY(num_1)
			) USING orioledb;

			INSERT INTO table_range
				SELECT type_range_1(id * 2, id * 2 + 2),
						id * 8 num_2
					FROM generate_series(1, 10) id;

		""")
		node.safe_psql('postgres', """
			DROP TYPE type_range_1 CASCADE;
		""")

		self.check_total_deleted(node, 'RANGE_CACHE', 1, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'RANGE_CACHE', 1, 1)
		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM table_range;")[0][0],
			10)
		node.stop()

	def test_record_array_index_recovery_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TYPE coordinates AS (
				x int,
				y int
			);

			CREATE TABLE coordinates_table
			(
				locat coordinates[] NOT NULL,
				id int NOT NULL,
				PRIMARY KEY (locat)
			) USING orioledb;

			INSERT INTO coordinates_table
				SELECT ARRAY[(id, id * 3)::coordinates,
							(id, id * 33)::coordinates], id
					FROM generate_series(1, 10) id;

			CREATE TYPE coord_1 AS (x int, y int);
			CREATE TABLE test_coord
			(
				key coord_1[] NOT NULL,
				PRIMARY KEY(key)
			) USING orioledb;
		""")
		node.safe_psql('postgres', """
			DROP TYPE coord_1 CASCADE;
		""")
		self.check_total_deleted(node, 'CLASS_CACHE', 3, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 8, 0)
		node.stop(['-m', 'immediate'])

		node.start()
		self.check_total_deleted(node, 'CLASS_CACHE', 3, 1)
		self.check_total_deleted(node, 'TYPE_CACHE', 8, 1)
		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM coordinates_table;")[0][0],
			10)
		node.stop()

	def test_complex_index_recovery_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE type_enum AS ENUM ('one','two','three');
			CREATE TYPE type_record AS (a integer);

			CREATE TABLE IF NOT EXISTS table_name (
				val_1 integer[] NOT NULL,
				val_2 type_enum NOT NULL,
				val_3 type_record NOT NULL,
				PRIMARY KEY(val_2)
			) USING orioledb;

			INSERT INTO table_name(val_1, val_2, val_3)
				VALUES('{1,5,44}','one','(3)');
			INSERT INTO table_name(val_1, val_2, val_3)
				VALUES('{4,44,8}','two','(77)');
			INSERT INTO table_name(val_1, val_2, val_3)
				VALUES('{3,33,0}','three','(9)');
		""")

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM table_name;")[0][0],
			3)

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM table_name;")[0][0],
			3)

		node.stop()

	def test_complex_index_recovery_2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE type_enum AS ENUM ('one','two','three');
			CREATE TYPE type_record AS (a integer);

			CREATE TABLE IF NOT EXISTS table_name (
				val_1 integer[] NOT NULL,
				val_2 type_enum NOT NULL,
				val_3 type_record NOT NULL,
				PRIMARY KEY(val_2)
			) USING orioledb;

			INSERT INTO table_name(val_1, val_2, val_3)
				VALUES('{1,5,44}','one','(3)');
			INSERT INTO table_name(val_1, val_2, val_3)
				VALUES('{4,44,8}','two','(77)');
			INSERT INTO table_name(val_1, val_2, val_3)
				VALUES('{3,33,0}','three','(9)');
		""")

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM table_name;")[0][0],
			3)

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		self.assertEqual(
			node.execute("SELECT COUNT(*) FROM table_name;")[0][0],
			3)

		node.safe_psql('postgres', """
			DROP TABLE table_name;
		""")

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		node.stop(['-m', 'immediate'])

		node.start()

		self.check_total_deleted(node, 'ENUM_CACHE', 1, 0)
		self.check_total_deleted(node, 'CLASS_CACHE', 1, 0)
		self.check_total_deleted(node, 'TYPE_CACHE', 5, 0)

		node.stop()




