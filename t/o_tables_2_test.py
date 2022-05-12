#!/usr/bin/env python3
# coding: utf-8

import unittest
from unittest.util import safe_repr
from pkg_resources import safe_extra
import testgres
import os
import re

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

from testgres.enums import NodeStatus

class OTablesTest(BaseTest):
	def assertTblCount(self, size):
		self.assertEqual(size,
						 self.node.execute('postgres',
										   'SELECT count(*) FROM orioledb_table_oids();')[0][0])
	
	def test_o_tables_wal_commit_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 varchar(50) NOT NULL,
				PRIMARY KEY(num_1)
			) USING orioledb;
			
			INSERT INTO table_name VALUES(55, 'num');
		""")
		self.assertTblCount(1)

		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name_2(
				num_3 int NOT NULL,
				num_4 int NOT NULL,
				PRIMARY KEY(num_4)
			) USING orioledb;

			INSERT INTO table_name_2 VALUES(222, 333);
		""")

		self.assertTblCount(2)

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(2)

		node.stop()

	def test_o_tables_wal_commit_2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL,
				num_3 int NOT NULL,
				PRIMARY KEY(num_1)
			) USING orioledb;

			INSERT INTO table_name (num_1, num_2, num_3)
				VALUES(1,2,3);
			INSERT INTO table_name (num_1, num_2, num_3)
				VALUES(4,5,6);
			INSERT INTO table_name (num_1, num_2, num_3)
				VALUES(7,8,9);
		""")

		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres','SELECT count(*) FROM table_name')[0][0], 3)

		node.stop(['-m', 'immediate'])
		node.start()

		self.assertTblCount(1)

		node.safe_psql('postgres', """
			DELETE FROM table_name WHERE num_1 = 1;
		""")
		self.assertTblCount(1)
		self.assertEqual(node.execute('postgres','SELECT count(*) FROM table_name')[0][0], 2)
		node.stop()

	def test_o_tables_wal_commit_3(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE type_name AS ENUM('one','two');

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 type_name NOT NULL,
				PRIMARY KEY(num_1)
			) USING orioledb;

			INSERT INTO table_name(num_1, num_2)
				VALUES(1, 'one');
			INSERT INTO table_name(num_1, num_2)
				VALUES(2, 'two');
		""")

		self.assertTblCount(1)
		
		node.safe_psql('postgres', """
			DROP TYPE type_name CASCADE;
		""")

		self.assertTblCount(1)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)

		node.stop

	def test_o_tables_wal_rollback_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			) USING orioledb;

			INSERT INTO table_name VALUES (11,22);
		""")
		self.assertTblCount(1)

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("DROP TABLE table_name;")
		self.assertTblCount(1)
		con1.rollback()
		self.assertTblCount(1)
		con1.close()
		con2.execute("DROP TABLE table_name;")
		self.assertTblCount(1)
		con2.commit()
		con2.close()
		self.assertTblCount(0)

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(0)

		node.stop()

	def test_o_tables_wal_drop_rollback_2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			) USING orioledb;

			INSERT INTO table_name VALUES (11,22);

			CREATE TABLE IF NOT EXISTS table_name_2(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			) USING orioledb;

			INSERT INTO table_name_2 VALUES (111,222);
		""")
		self.assertTblCount(2)

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("DROP TABLE table_name;")
		self.assertTblCount(2)
		con1.commit()
		con1.close()
		con2.execute("DROP TABLE table_name_2;")
		self.assertTblCount(1)
		con2.rollback()
		self.assertTblCount(1)
		con2.close()

		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)
		node.stop()

	def test_o_tables_wal_drop_extension_rollback_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			)USING orioledb;

			INSERT INTO table_name VALUES (11, 22);
		""")
		self.assertTblCount(1)

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("DROP EXTENSION orioledb CASCADE;")
		con1.rollback()
		con1.close()
		con2.execute("ALTER TABLE table_name DROP COLUMN num_2")
		con2.commit()
		con2.close()

		self.assertTblCount(1)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)
		node.stop()

	def test_o_tables_mix_1(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			)USING orioledb;

			INSERT INTO table_name (num_1, num_2)
				VALUES (11,22);
			INSERT INTO table_name (num_1, num_2)
				VALUES (33,44);
			INSERT INTO table_name (num_1, num_2)
				VALUES (55,66);
			INSERT INTO table_name (num_1, num_2)
				VALUES (77,88);
		""")

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		self.assertTblCount(1)
		con1.execute("DELETE FROM table_name WHERE num_1 = 55")
		self.assertTblCount(1)
		con1.commit()
		con1.close()
		con2.execute("ALTER TABLE table_name DROP COLUMN num_1")
		self.assertTblCount(1)
		con2.commit()
		con2.close()

		self.assertTblCount(1)
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertTblCount(1)

		node.safe_psql('postgres', """
			DROP TABLE table_name;
		""")
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_mix_2(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		self.assertTblCount(0)
		con1 = node.connect()

		con1.begin()
		con1.execute("""CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			)USING orioledb;

			INSERT INTO table_name VALUES (11, 22);""")
		self.assertTblCount(1)
		con1.commit()
		con1.close()

		self.assertTblCount(1)
		node.safe_psql('postgres', """
			DROP TABLE table_name;
		""")
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_mix_3(self):
		node = self.node
		node.start()

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		con1.execute("CREATE EXTENSION IF NOT EXISTS orioledb;")
		self.assertEqual(0,
						 con1.execute('SELECT count(*) FROM orioledb_table_oids();')[0][0])
		con1.execute("""
			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			)USING orioledb;

			INSERT INTO table_name (num_1, num_2)
				VALUES (11,22);
			INSERT INTO table_name (num_1, num_2)
				VALUES (33,44);
			INSERT INTO table_name (num_1, num_2)
				VALUES (55,66);
			INSERT INTO table_name (num_1, num_2)
				VALUES (77,88);
		""")		
		con1.commit()
		con1.close()
		self.assertTblCount(1)
		con2.execute("DROP EXTENSION orioledb CASCADE;")
		self.assertTblCount(1)
		con2.commit()
		con2.close()
		node.stop()

	def test_o_tables_mix_4(self):
		node = self.node
		node.start()

		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		self.assertTblCount(0)

		con1 = node.connect()
		con1.begin()
		con1.execute("""
			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 int NOT NULL
			)USING orioledb;

			INSERT INTO table_name (num_1, num_2)
				VALUES (11,22);
			INSERT INTO table_name (num_1, num_2)
				VALUES (33,44);
		""")
		self.assertTblCount(1)
		con1.execute("""
			DROP TABLE table_name
		""")
		self.assertTblCount(1)
		con1.commit()
		con1.close()
		self.assertTblCount(0)
		node.stop()

	def test_o_tables_mix_5(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;

			CREATE TYPE type_name AS ENUM('one','two');

			CREATE TABLE IF NOT EXISTS table_name(
				num_1 int NOT NULL,
				num_2 type_name NOT NULL
			)USING orioledb;

			INSERT INTO table_name (num_1, num_2)
				VALUES (11,'one');
			INSERT INTO table_name (num_1, num_2)
				VALUES (33,'two');
		""")

		self.assertTblCount(1)

		con1 = node.connect()
		con2 = node.connect()

		con1.begin()
		con2.begin()
		self.assertTblCount(1)

		con1.execute("DROP TYPE type_name CASCADE;")
		con1.execute("""
			CREATE TABLE table_name_2(
				num_3 int NOT NULL,
				num_4 int NOT NULL
			)USING orioledb;
		""")
		self.assertTblCount(2)
		con1.commit()
		con1.close()
		self.assertTblCount(2)
		con2.execute("""
			INSERT INTO table_name_2 (num_3, num_4)
				VALUES (11,22);
			INSERT INTO table_name_2 (num_3, num_4)
				VALUES (33,44);
			INSERT INTO table_name_2 (num_3, num_4)
				VALUES (55,66);
			INSERT INTO table_name_2 (num_3, num_4)
				VALUES (77,88);
		""")
		self.assertTblCount(2)
		con2.commit()
		con2.close()

		node.stop(['-m', 'immediate'])
		node.start()
		self.assertTblCount(2)

		node.safe_psql('postgres', """
			DROP EXTENSION orioledb CASCADE;
			CREATE EXTENSION IF NOT EXISTS orioledb;
		""")
		self.assertTblCount(0)
		node.stop()

	def test_null_o_table(self):
		node = self.node
		node.start()
		con_control = node.connect()
		con_control.execute("""
		CREATE EXTENSION IF NOT EXISTS orioledb;

		CREATE TABLE o_test_1(
			val_1 int,
			val_2 int
		)USING orioledb;

		INSERT INTO o_test_1(val_1, val_2)
			(SELECT val_1, val_1 * 100 FROM generate_series (1, 11) val_1);
		""")
		con_control.commit()
		con1 = node.connect()
		con2 = node.connect()
		con1.begin()
		con2.begin()
		con1.execute("DROP TABLE o_test_1;")
		con2.commit()
		con1.commit()
		con_control.execute("""
		DROP TABLE o_test_1;
		""")
		con_control.commit()




