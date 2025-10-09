#!/usr/bin/env python3
# coding: utf-8

import json
import subprocess
import testgres
import unittest

from tempfile import mkdtemp
from testgres.utils import get_bin_path

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

# Update with changed pkey on a table with TOAST attributes

	def test_logical_subscription_toast_update_pkey(self):
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
					CREATE TABLE o_test1 (id int PRIMARY KEY, bid int, junk text
					) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub', tables=['o_test1'])
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "INSERT INTO o_test1 (id, bid, junk) VALUES (1, 1, repeat(pi()::text,20000));"
						)
						con2.execute(
						    "INSERT INTO o_test1 (id, bid) VALUES (2, 2);")
						con1.execute("UPDATE o_test1 SET id = 6 WHERE id = 1;")

						con1.commit()
						con2.commit()

					self.assertListEqual(
					    publisher.execute(
					        'SELECT id, bid FROM o_test1 ORDER BY id'),
					    [(2, 2), (6, 1)])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					# sub.poll_query_until("SELECT orioledb_recovery_synchronized();", expected=True)
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT id, bid FROM o_test1 ORDER BY id'),
					    [(2, 2), (6, 1)])

	def test_recvlogical_and_drop_database(self):
		node = self.node
		node.start()

		node.safe_psql("postgres", "CREATE DATABASE logicaldb")
		node.safe_psql(
		    "logicaldb",
		    "SELECT pg_create_logical_replication_slot('logicaldb_slot', 'test_decoding')"
		)

		pg_recvlogical = subprocess.Popen([
		    get_bin_path("pg_recvlogical"), "-d", "logicaldb", "-p",
		    str(node.port), "-S", "logicaldb_slot", "-v", "-f", "-", "--start"
		],
		                                  stdout=subprocess.PIPE,
		                                  stderr=subprocess.PIPE,
		                                  text=True)

		# Check that pg_recvlogical started without error
		self.assertIsNone(pg_recvlogical.poll())

		# Wait until pg_recvlogical starts streaming
		node.poll_query_until(
		    "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'logicaldb_slot' AND active_pid IS NOT NULL)"
		)

		with self.assertRaises(testgres.QueryException) as e:
			node.safe_psql("postgres", "DROP DATABASE logicaldb")

		self.assertErrorMessageEquals(
		    e,
		    "database \"logicaldb\" is used by an active logical replication slot",
		    "There is 1 active slot.", "DETAIL")

		pg_recvlogical.terminate()
		node.stop()
