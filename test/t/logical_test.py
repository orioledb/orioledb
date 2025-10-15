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

	def test_logical_subscription(self):
		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
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
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [(2, '2'),
					                                               (4, 'NO')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test2 ORDER BY id'), [])

	def test_logical_subscription_ctid(self):
		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test_ctid (
						data1 text,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE TABLE o_test_bridge (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_bridge USING spgist (data2);
					CREATE TABLE o_test_secondary (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_secondary (data2);
					CREATE TABLE o_test_ctid_bridge (
						data1 text,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_ctid_bridge USING spgist (data1);
					CREATE TABLE o_test_ctid_secondary (
						data1 text,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_secondary (data2);

					CREATE TABLE o_test_bridge_secondary (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_bridge_secondary USING spgist (data2);
					CREATE INDEX ON o_test_bridge_secondary (data3);
					CREATE TABLE o_test_ctid_bridge_secondary (
						data1 text,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_ctid_bridge_secondary USING spgist (data2);
					CREATE INDEX ON o_test_ctid_bridge_secondary (data3);
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub',
				                        tables=[
				                            'o_test_ctid', 'o_test_bridge',
				                            'o_test_secondary',
				                            'o_test_ctid_bridge',
				                            'o_test_ctid_secondary',
				                            'o_test_ctid_bridge_secondary',
				                            'o_test_bridge_secondary'
				                        ])
				sub = subscriber.subscribe(pub, 'test_sub')

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "INSERT INTO o_test_ctid VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_bridge VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_bridge VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_secondary VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_secondary VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid_bridge VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid_bridge VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid_secondary VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid_secondary VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_bridge_secondary VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_bridge_secondary VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid_bridge_secondary VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid_bridge_secondary VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.commit()
						con2.commit()


#						con2.execute("CHECKPOINT;")
#						con2.execute("SELECT orioledb_get_current_oxid();")

#					publisher.safe_psql("CHECKPOINT;")
#					subscriber.execute("SELECT orioledb_get_current_oxid();")
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_bridge ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_secondary ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid_bridge ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid_secondary ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_bridge_secondary ORDER BY i'
					    ), [('foofoo', 'barbar', 'aaaaaa', 1),
					        ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid_bridge_secondary ORDER BY i'
					    ), [('foofoo', 'barbar', 'aaaaaa', 1),
					        ('mmm', 'nnn', 'ooo', 2)])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					subscriber.poll_query_until(
					    "SELECT orioledb_recovery_synchronized();",
					    expected=True)
					#					subscriber.safe_psql("CHECKPOINT;")
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_bridge ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_secondary ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid_bridge ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid_secondary ORDER BY i'),
					    [('foofoo', 'barbar', 'aaaaaa', 1),
					     ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_bridge_secondary ORDER BY i'
					    ), [('foofoo', 'barbar', 'aaaaaa', 1),
					        ('mmm', 'nnn', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid_bridge_secondary ORDER BY i'
					    ), [('foofoo', 'barbar', 'aaaaaa', 1),
					        ('mmm', 'nnn', 'ooo', 2)])

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
