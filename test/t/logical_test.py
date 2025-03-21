#!/usr/bin/env python3
# coding: utf-8

import json
import os
import sys
import testgres
import unittest

from tempfile import mkdtemp
from testgres.utils import get_pg_config

from .base_test import BaseTest

class LogicalTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")

	def squashLogicalChanges(self, rows):
		result = ''
		for row in rows:
			line = row[2]
			if line.startswith('BEGIN') or line.startswith('COMMIT'):
				line = line[0:line.index(' ')]
			result = result + line + "\n"
		return result

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_simple(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE data(id serial primary key, data text) USING orioledb;\n"
		)

		node.safe_psql(
		    'postgres',
		    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
		)

		node.safe_psql(
		    'postgres', "BEGIN;\n"
		    "INSERT INTO data(data) VALUES('1');\n"
		    "INSERT INTO data(data) VALUES('2');\n"
		    "COMMIT;\n")

		result = self.squashLogicalChanges(
		    node.execute(
		        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
		    ))
		self.assertEqual(
		    result,
		    "BEGIN\ntable public.data: INSERT: id[integer]:1 data[text]:'1'\ntable public.data: INSERT: id[integer]:2 data[text]:'2'\nCOMMIT\n"
		)

	@unittest.skipIf(not BaseTest.extension_installed("wal2json"),
	                 "'wal2json' is not installed")
	def test_wal2json(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE data(
				id serial primary key,
				data text
			) USING orioledb;
		""")

		node.safe_psql(
		    "SELECT * FROM pg_create_logical_replication_slot('slot', 'wal2json');"
		)

		with node.connect() as con1:
			con1.execute("INSERT INTO data(data) VALUES('1');")
			con1.execute("INSERT INTO data(data) VALUES('2');")
			con1.execute("UPDATE data SET data = 'NO' WHERE id = 1;")
			con1.execute("DELETE FROM data WHERE id = 1;")
			con1.commit()

		result = node.execute(
		    "SELECT * FROM pg_logical_slot_get_changes('slot', NULL, NULL);")
		self.assertDictEqual(
		    json.loads(result[0][2]), {
		        "change": [{
		            "kind": "insert",
		            "schema": "public",
		            "table": "data",
		            "columnnames": ["id", "data"],
		            "columntypes": ["integer", "text"],
		            "columnvalues": [1, "1"]
		        }, {
		            "kind": "insert",
		            "schema": "public",
		            "table": "data",
		            "columnnames": ["id", "data"],
		            "columntypes": ["integer", "text"],
		            "columnvalues": [2, "2"]
		        }, {
		            'kind': 'update',
		            'schema': 'public',
		            'table': 'data',
		            'columnnames': ['id', 'data'],
		            'columntypes': ['integer', 'text'],
		            'columnvalues': [1, 'NO'],
		            'oldkeys': {
		                'keynames': ['id'],
		                'keytypes': ['integer'],
		                'keyvalues': [1]
		            }
		        }, {
		            'kind': 'delete',
		            'schema': 'public',
		            'table': 'data',
		            'oldkeys': {
		                'keynames': ['id'],
		                'keytypes': ['integer'],
		                'keyvalues': [1]
		            }
		        }]
		    })

	def test_logical_subscription(self):
		with self.node as publisher:
			publisher.start()

			baseDir = mkdtemp(prefix=self.myName + '_tgsb_')
			subscriber = testgres.get_new_node('subscriber',
			                                   port=self.getBasePort() + 1,
			                                   base_dir=baseDir)
			subscriber.init(["--no-locale", "--encoding=UTF8"])
			subscriber.append_conf(shared_preload_libraries='orioledb')
			subscriber.append_conf(wal_level='logical')

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test1 (
						id serial primary key,
						data text
					) USING orioledb;
					CREATE TABLE o_test2 (
						id serial primary key,
						data text
					) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub', tables=['o_test1'])
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute("INSERT INTO o_test1 (data) VALUES('1');")
						con2.execute("INSERT INTO o_test1 (data) VALUES('2');")
						con1.execute("INSERT INTO o_test1 (data) VALUES('3');")
						con2.execute("INSERT INTO o_test1 (data) VALUES('4');")

						con1.execute("INSERT INTO o_test2 (data) VALUES('1');")
						con2.execute("INSERT INTO o_test2 (data) VALUES('2');")
						con1.execute("INSERT INTO o_test2 (data) VALUES('3');")
						con2.execute("INSERT INTO o_test2 (data) VALUES('4');")

						con1.commit()
						con2.commit()

						con1.execute(
						    "UPDATE o_test1 SET data = 'YES' WHERE id = 1;")
						con2.execute(
						    "UPDATE o_test2 SET data = 'YES' WHERE id = 1;")

						con1.execute(
						    "UPDATE o_test1 SET data = 'NO' WHERE id = 4;")
						con2.execute(
						    "UPDATE o_test2 SET data = 'NO' WHERE id = 4;")

						con1.execute("DELETE FROM o_test1 WHERE id = 1;")
						con2.execute("DELETE FROM o_test2 WHERE id = 2;")
						con1.execute("DELETE FROM o_test1 WHERE id = 3;")
						con2.execute("DELETE FROM o_test2 WHERE id = 4;")

						con1.commit()
						con2.commit()

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test1 ORDER BY id'),
					    [(2, '2'), (4, 'NO')])
					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test2 ORDER BY id'),
					    [(1, 'YES'), (3, '3')])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					# sub.poll_query_until("SELECT orioledb_recovery_synchronized();", expected=True)
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [(2, '2'),
					                                               (4, 'NO')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test2 ORDER BY id'), [])
