#!/usr/bin/env python3
# coding: utf-8

import json
import subprocess
import testgres
import unittest

from tempfile import mkdtemp
from testgres.utils import get_bin_path

from .base_test import BaseTest


def clear_table(
    node, table
):  # BUG on orioledb sys relations visibility: new relfilenode during truncate
	node.safe_psql('postgres', f'TRUNCATE TABLE {table};')
	node.execute(
	    "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
	)


def create_slot(node):
	node.safe_psql(
	    'postgres',
	    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
	)


def node_prepare_orel(node, table):
	node.start()  # start PostgreSQL
	node.safe_psql(
	    'postgres',
	    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
	    f'CREATE TABLE {table}(id serial primary key, data text) USING orioledb;\n'  # oriole relation
	)

	node.safe_psql(
	    'postgres',
	    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
	)


def wait_ready(subscriber):
	# Wait on subscriber until it becomes ready (r state)
	# pg_subscription_rel.srsubstate means synchronization state on subscriber
	#
	# i — initializing
	#	NO tablesync worker
	#	NO initial copy
	#
	# d — data copy
	#	tablesync worker in progress (`COPY public.table FROM STDIN`)
	#
	# s — sync
	#	initial copy done
	#	tablesync worker is applying WAL
	#	NO apply worker
	#
	# r — ready
	#	initial copy done
	#	catch-up done
	#	apply worker in progress
	#
	with subscriber.connect() as con:
		con.execute(f"""
			DO $$
			BEGIN
			WHILE EXISTS (
				SELECT 1 FROM pg_subscription_rel WHERE srsubstate <> 'r'
			)
			LOOP
				PERFORM pg_sleep(0.1);
			END LOOP;
			END $$;
		""")


class LogicalTest(BaseTest):

	o_relname = "o_data"
	h_relname = "h_data"

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

	def retrieve_logical_changes(self):
		return self.squashLogicalChanges(
		    self.node.execute(
		        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
		    ))

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
		    result, "BEGIN\n"
		    "table public.data: INSERT: id[integer]:1 data[text]:'1'\n"
		    "table public.data: INSERT: id[integer]:2 data[text]:'2'\n"
		    "COMMIT\n")

	def test_simple_replident(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres', """CREATE EXTENSION IF NOT EXISTS orioledb;
		                CREATE TABLE data (
			                        i   int,
			                        data1 text,
			                        data2 text,
			                        data3 text
			                        ) USING orioledb;
		          ALTER TABLE data REPLICA IDENTITY FULL;""")

		node.safe_psql(
		    'postgres',
		    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
		)

		node.safe_psql(
		    'postgres',
		    "BEGIN;\n"
		    "INSERT INTO data VALUES(1, 'foofoo','barbar', 'aaaaaa');\n"
		    "INSERT INTO data VALUES(2, 'mmm','nnn', 'ooo');\n"
		    #		    "UPDATE data SET data2 = 'ssssss' where data2 = 'barbar';\n"
		    "COMMIT;\n")
		node.safe_psql(
		    'postgres', "BEGIN;\n"
		    "UPDATE data SET data2 = 'ssssss' where data2 = 'barbar';\n"
		    "UPDATE data SET data2 = 'ppp' where data2 = 'nnn';\n"
		    "COMMIT;\n")
		result = self.squashLogicalChanges(
		    node.execute(
		        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
		    ))
		self.assertEqual(
		    result, "BEGIN\n"
		    "table public.data: INSERT: i[integer]:1 data1[text]:'foofoo' data2[text]:'barbar' data3[text]:'aaaaaa'\n"
		    "table public.data: INSERT: i[integer]:2 data1[text]:'mmm' data2[text]:'nnn' data3[text]:'ooo'\n"
		    "COMMIT\n"
		    "BEGIN\n"
		    "table public.data: UPDATE: old-key: i[integer]:1 data1[text]:'foofoo' data2[text]:'barbar' data3[text]:'aaaaaa' new-tuple: i[integer]:1 data1[text]:'foofoo' data2[text]:'ssssss' data3[text]:'aaaaaa'\n"
		    "table public.data: UPDATE: old-key: i[integer]:2 data1[text]:'mmm' data2[text]:'nnn' data3[text]:'ooo' new-tuple: i[integer]:2 data1[text]:'mmm' data2[text]:'ppp' data3[text]:'ooo'\n"
		    "COMMIT\n")

#
# The next two tests reproduce an existing issue: incorrect state of OrioleDB system catalogs during logical decoding.
#
# TRAP: failed Assert("descr != NULL"), File: "src/recovery/logical.c", Line: 975
#
# This problem arises because changes to Oriole system trees are not included in MVCC-historical snapshot and
# are not applied on replaying changes from the reorder buffer.
#
# During logical decoding, when processing each command, we observe a final state of the Oriole system catalogs
# rather than some intermediate state that was relevant at the time when the current command has been executed within transaction.
#
# These tests should be enabled but only after this issue has been resolved.
#
#@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
#                 "'test_decoding' is not installed")
#def test_switch_logical_xid_BUG_COMMIT(self):
#	# System catalogs Oriole changes visibility during logical decoding
#	node = self.node
#	node.start()  # start PostgreSQL
#	node.safe_psql(
#	    'postgres',
#	    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
#		"CREATE TABLE o_data(id serial primary key, data text) USING orioledb;\n"  # oriole relation
#	)
#
#	node.safe_psql(
#	    'postgres',
#	    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
#	)
#
#	node.safe_psql(
#	    'postgres', '''
#			BEGIN;
#			INSERT INTO o_data(data) VALUES('20');
#			INSERT INTO o_data(data) VALUES('40');
#			DROP TABLE IF EXISTS o_data;
#			COMMIT;
#		''')
#
#	result = self.squashLogicalChanges(
#	    node.execute( # TRAP: failed Assert("descr != NULL") because there is no relation `o_data` in orioledb_table
#	        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
#	    ))
#	#print(result)

#@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
#                 "'test_decoding' is not installed")
#def test_switch_logical_xid_BUG_ABORT(self):
#	# System catalogs Oriole changes visibility during logical decoding
#	node = self.node
#	node.start()  # start PostgreSQL
#	node.safe_psql(
#	    'postgres',
#	    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
#	)
#
#	node.safe_psql(
#	    'postgres',
#	    "SELECT * FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding', false, true);\n"
#	)
#
#	node.safe_psql(
#	    'postgres', '''
#			BEGIN;
#			CREATE TABLE o_data(id serial primary key, data text) USING orioledb;
#			INSERT INTO o_data(data) VALUES('10');
#			ABORT;
#		''')
#
#	result = self.squashLogicalChanges(
#	    node.execute( # TRAP: failed Assert("descr != NULL") because there is no relation `o_data` in orioledb_table
#	        "SELECT * FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL);"
#	    ))
#	#print(result)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_h2o(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres',
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_data(id serial primary key, data text) USING orioledb;\n"  # oriole relation
		    "CREATE TABLE h_data(id serial primary key, data text);\n"  # heap relation
		)

		create_slot(node)

		# part 1

		node.safe_psql(
		    'postgres', '''
				BEGIN;
				INSERT INTO h_data(data) VALUES('10');
				INSERT INTO o_data(data) VALUES('20');
				INSERT INTO h_data(data) VALUES('30');
				INSERT INTO o_data(data) VALUES('40');
				COMMIT;
			''')

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, "BEGIN\n"
		    "table public.h_data: INSERT: id[integer]:1 data[text]:'10'\n"
		    "table public.h_data: INSERT: id[integer]:2 data[text]:'30'\n"
		    "table public.o_data: INSERT: id[integer]:1 data[text]:'20'\n"
		    "table public.o_data: INSERT: id[integer]:2 data[text]:'40'\n"
		    "COMMIT\n")

		# part 2

		node.safe_psql(
		    'postgres', '''
				BEGIN;
				INSERT INTO h_data(data) VALUES('50');
				DELETE FROM o_data WHERE id=2;
				INSERT INTO h_data(data) SELECT data FROM o_data;
				INSERT INTO o_data SELECT * FROM h_data WHERE id > 1;
				DELETE FROM h_data WHERE id > 1;
				COMMIT;
			''')

		self.assertEqual(node.execute('postgres', "SELECT * FROM h_data;\n"),
		                 [(1, '10')])

		self.assertEqual(node.execute('postgres', "SELECT * FROM o_data;\n"),
		                 [(1, '20'), (2, '30'), (3, '50'), (4, '20')])

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, "BEGIN\n"
		    "table public.h_data: INSERT: id[integer]:3 data[text]:'50'\n"
		    "table public.h_data: INSERT: id[integer]:4 data[text]:'20'\n"
		    "table public.h_data: DELETE: id[integer]:2\n"
		    "table public.h_data: DELETE: id[integer]:3\n"
		    "table public.h_data: DELETE: id[integer]:4\n"
		    "table public.o_data: DELETE: id[integer]:2\n"
		    "table public.o_data: INSERT: id[integer]:2 data[text]:'30'\n"
		    "table public.o_data: INSERT: id[integer]:3 data[text]:'50'\n"
		    "table public.o_data: INSERT: id[integer]:4 data[text]:'20'\n"
		    "COMMIT\n")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_simple(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		""")

		create_slot(node)

		node.safe_psql(
		    'postgres', f'''
				BEGIN;
				INSERT INTO {o_relname}(data) VALUES(100);
				ALTER TABLE {o_relname} RENAME COLUMN data TO data_2;
				ALTER TABLE {o_relname} ADD COLUMN data_3 text;
				INSERT INTO {o_relname}(data_2,data_3) VALUES(300,400);
				COMMIT;
			''')
		# Final version of relation descr from systree o_tables after this COMMIT will be with three attrs (id,data_2,data_3),
		# which does not match with the first version (id,data).
		# But it is necessary to observe both these versions in logical decoder to properly decode INSERT'ed tuples.

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result,
		    "BEGIN\n"
		    f"""table public.{o_relname}: INSERT: id[integer]:1 data[text]:'100'\n"""  # initial schema
		    f"""table public.{o_relname}: INSERT: id[integer]:2 data_2[text]:'300' data_3[text]:'400'\n"""  # new schema
		    "COMMIT\n")

	def test_systrees_versions_simple__subscriber(self):
		o_relname = self.o_relname

		setup_sql = f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		"""

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:

				publisher.safe_psql(setup_sql)
				subscriber.safe_psql(setup_sql)

				pub = publisher.publish('test_pub', tables=[f'{o_relname}'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					con1.begin()
					con1.execute(f"""
						INSERT INTO {o_relname}(data) VALUES(100);
						ALTER TABLE {o_relname} RENAME COLUMN data TO data_2;
					""")
					con1.commit()

				# wait until changes apply on subscriber and check them
				sub.catchup()

				with subscriber.connect() as con:
					output = con.execute(f"""SELECT * FROM {o_relname};""")
					tup = output[0]
					# print(tup)
					self.assertEqual(len(tup), 2)
					self.assertEqual(tup, (1, '100'))

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_o_table_from_pagelevel_undo_on_compaction(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		""")

		create_slot(node)

		# First transaction generates a challenging WAL sequence for logical decoder (ldec).
		# This transaction uses an experimental relation in multi-version manner and finally does a logical deletion.
		#
		# Logical decoding for this WAL sequence needs two versions of relation:
		#     - initial version (for first INSERT)
		#     - next version after modifications (for second INSERT)
		# but in the final state of this txn - relation tuple is logically deleted.
		#
		# After COMMIT of this transaction, experimental relation is logically deleted
		# but physically stored in actual page of systree O_TABLE, as a dead tuple with tuple-level undo chain.
		node.safe_psql(
		    'postgres',
		    "BEGIN;\n"
		    f"""INSERT INTO {o_relname}(data) VALUES(100);\n"""  # ldec needs initial version of relation
		    f"""ALTER TABLE {o_relname} RENAME COLUMN data TO data_2;\n"""  # add relation version to tuple-level undo chain
		    f"""ALTER TABLE {o_relname} ADD COLUMN data_3 text;\n"""  # add relation version to tuple-level undo chain
		    f"""INSERT INTO {o_relname}(data_2,data_3) VALUES(300,400);\n"""  # ldec needs next version of relation
		    f"""DROP TABLE {o_relname};\n"""  # finally relation tuple is logically deleted: marked as deleted
		    "COMMIT;\n")

		# Second transaction performs GC on compaction and then splits a page in systree O_TABLE.
		# GC is taking place on a write path - while INSERT.
		# Both versions of experimental relation becomes stored in page-level undo chain - within previous page images.
		node.safe_psql(
		    'postgres', '''
				DO $$
				DECLARE
					i int;
					sql text;
					n int := 6;
				BEGIN
					FOR i IN 1..n LOOP
						sql := format(
							'CREATE TABLE o_test_%s (
								id   serial PRIMARY KEY,
								data text
							) USING orioledb;', i);
						EXECUTE sql;
					END LOOP;
				END$$;
			''')

		# Logical decoder uses page-level undo to obtain old image of systree page where deleted tuple is located,
		# and uses tuple-level undo to retrieve a proper tuple version.
		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, "BEGIN\n"
		    f"""table {o_relname}: INSERT: id[integer]:1 data[text]:'100'\n"""
		    f"""table public.{o_relname}: INSERT: id[integer]:2 data_2[text]:'300' data_3[text]:'400'\n"""
		    "COMMIT\n"
		    "BEGIN\n"
		    "COMMIT\n")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql('postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    CREATE INDEX data_idx ON {o_relname}(data);
		""")  # regular secondary btree index

		create_slot(node)

		node.safe_psql(
		    'postgres', f'''
				BEGIN;
				INSERT INTO {o_relname}(data) VALUES(100);
				ALTER TABLE {o_relname} RENAME COLUMN data TO data_2;
				ALTER TABLE {o_relname} ADD COLUMN data_3 text;
				INSERT INTO {o_relname}(data_2,data_3) VALUES(300,400);
				COMMIT;
			''')
		# Final version of relation tupedescr from systree o_indices after this COMMIT will be with three attrs (id,data_2,data_3),
		# which does not match with the first version (id,data).
		# But it is necessary to observe both these versions in logical decoder to properly decode INSERT'ed tuples.

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, "BEGIN\n"
		    f"""table public.{o_relname}: INSERT: id[integer]:1 data[text]:'100'\n"""
		    f"""table public.{o_relname}: INSERT: id[integer]:2 data_2[text]:'300' data_3[text]:'400'\n"""
		    "COMMIT\n")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_INSERT(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
		    CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol = 'A'

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol}\', {toast_len}));
			COMMIT;
		""")

		result = self.retrieve_logical_changes()

		expected = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
		b = len(expected)  # border index for checking large output

		self.assertEqual(result[0:b], expected)
		for i in range(b, b + toast_len):
			self.assertEqual(result[i], symbol)
		self.assertEqual(result[b + toast_len:], "'\nCOMMIT\n")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_INSERT_ALTER_ADD_COLUMN(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol = 'A'

		# txn creates second version of relation but uses only the initial one
		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol}\', {toast_len}));
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)

		expected = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
		b = len(expected)  # border index for checking large output

		self.assertEqual(result[0:b], expected)
		for i in range(b, b + toast_len):
			self.assertEqual(result[i], symbol)

		tail = result[b + toast_len:]
		self.assertEqual(tail, "'\nCOMMIT\n")

	def test_systrees_versions_index_TOAST_INSERT_ALTER_ADD_COLUMN__subscriber(
	        self):
		o_relname = self.o_relname

		setup_sql = f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		"""

		toast_len = 500000  # len of TOASTed value
		symbol = 'A'

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:

				publisher.safe_psql(setup_sql)
				subscriber.safe_psql(setup_sql)

				pub = publisher.publish('test_pub', tables=[f'{o_relname}'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					con1.begin()
					con1.execute(f"""
						INSERT INTO {o_relname}(data) VALUES (repeat('{symbol}', {toast_len}));
						ALTER TABLE {o_relname} ADD COLUMN data_3 text;
					""")
					con1.commit()

				# wait until changes apply on subscriber and check them
				sub.catchup()

				with subscriber.connect() as con:
					output = con.execute(f"""SELECT * FROM {o_relname};""")
					tup = output[0]
					#print(tup)
					self.assertEqual(len(tup),
					                 2)  # check correct number of attrs
					self.assertEqual(tup[0], 1)
					toasted = tup[1]
					self.assertEqual(len(toasted), toast_len)
					for i in range(0, toast_len):
						self.assertEqual(toasted[i], symbol)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_INSERT_ALTER_DROP_COLUMN(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol = 'A'

		# txn creates second version of relation but uses only the initial one
		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol}\', {toast_len}));
			ALTER TABLE {o_relname} DROP COLUMN data;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)

		expected = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
		b = len(expected)  # border index for checking large output

		self.assertEqual(result[0:b], expected)
		for i in range(b, b + toast_len):
			self.assertEqual(result[i], symbol)
		self.assertEqual(result[b + toast_len:], "'\nCOMMIT\n")

	def test_systrees_versions_index_TOAST_INSERT_ALTER_DROP_COLUMN__subscriber(
	        self):
		o_relname = self.o_relname

		setup_sql = f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		"""

		toast_len = 500000  # len of TOASTed value
		symbol = 'A'

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:

				publisher.safe_psql(setup_sql)
				subscriber.safe_psql(setup_sql)

				pub = publisher.publish('test_pub', tables=[f'{o_relname}'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					con1.begin()
					con1.execute(f"""
						INSERT INTO {o_relname}(data) VALUES (repeat('{symbol}', {toast_len}));
						ALTER TABLE {o_relname} DROP COLUMN data;
					""")
					con1.commit()

				# wait until changes apply on subscriber and check them
				sub.catchup()

				with subscriber.connect() as con:
					output = con.execute(f"""SELECT * FROM {o_relname};""")
					tup = output[0]
					#print(tup)
					self.assertEqual(len(tup),
					                 2)  # check correct number of attrs
					self.assertEqual(tup[0], 1)
					toasted = tup[1]
					self.assertEqual(len(toasted), toast_len)
					for i in range(0, toast_len):
						self.assertEqual(toasted[i], symbol)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_2INSERT_ALTER_ADD_COLUMN(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol1 = 'A'
		symbol2 = 'B'

		# txn creates second version of relation but uses only the initial one
		node.safe_psql(
		    'postgres', f"""
				BEGIN;
				INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol1}\', {toast_len}));
				INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol2}\', {toast_len}));
				ALTER TABLE {o_relname} ADD COLUMN data_3 text;
				COMMIT;
			""")

		result = self.retrieve_logical_changes()
		#print(result)

		expected1 = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
		expected2 = f"""'\ntable public.{o_relname}: INSERT: id[integer]:2 data[text]:'"""
		expected_tail = "'\nCOMMIT\n"

		start = 0  # start index
		b = 0  # border index for checking large output

		def check(expected):
			nonlocal start
			nonlocal b
			nonlocal result
			b = start + len(expected)
			self.assertEqual(result[start:b], expected)
			start = b

		def check_toast(expected, symbol):
			nonlocal start
			nonlocal b
			nonlocal toast_len
			nonlocal result
			check(expected)
			for i in range(b, b + toast_len):
				self.assertEqual(result[i], symbol)
			start += toast_len

		check_toast(expected1, symbol1)
		check_toast(expected2, symbol2)
		check(expected_tail)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_3INSERT_ALTER_ADD_COLUMN(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol1 = 'A'
		symbol2 = 'B'
		symbol3 = 'C'

		# txn creates second version of relation but uses only the initial one
		node.safe_psql(
		    'postgres', f"""
				BEGIN;
				INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol1}\', {toast_len}));
				INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol2}\', {toast_len}));
				ALTER TABLE {o_relname} ADD COLUMN data_3 text;
				INSERT INTO {o_relname}(data,data_3) VALUES (repeat(\'{symbol3}\', {toast_len}),repeat(\'{symbol3}\', {toast_len}));
				COMMIT;
			""")

		result = self.retrieve_logical_changes()
		#print(result)

		expected1 = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
		expected2 = f"""'\ntable public.{o_relname}: INSERT: id[integer]:2 data[text]:'"""
		expected3 = f"""'\ntable public.{o_relname}: INSERT: id[integer]:3 data[text]:'"""
		expected3_data3 = f"""' data_3[text]:'"""
		expected_tail = "'\nCOMMIT\n"

		start = 0  # start index
		b = 0  # border index for checking large output

		def check(expected):
			nonlocal start
			nonlocal b
			nonlocal result
			b = start + len(expected)
			self.assertEqual(result[start:b], expected)
			start = b

		def check_toast(expected, symbol):
			nonlocal start
			nonlocal b
			nonlocal toast_len
			nonlocal result
			check(expected)
			for i in range(b, b + toast_len):
				self.assertEqual(result[i], symbol)
			start += toast_len

		check_toast(expected1, symbol1)
		check_toast(expected2, symbol2)
		check_toast(expected3, symbol3)
		check_toast(expected3_data3, symbol3)
		check(expected_tail)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_INSERT_rewrite_rel(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol1 = 'A'
		symbol2 = 'B'

		# txn operates with two	iterationns of TOAST index descr & with two versions of relation
		with node.connect() as con:
			con.begin()
			con.execute(
			    f"""INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol1}\', {toast_len}));"""
			)
			check1 = con.execute(
			    f"""SELECT oid,relfilenode,reltoastrelid FROM pg_class WHERE relname='{o_relname}';"""
			)
			con.execute(f"""ALTER TABLE {o_relname} ADD COLUMN data_3 text;""")
			con.execute(
			    f"""ALTER TABLE {o_relname} ALTER COLUMN data_3 TYPE bigint USING 0::bigint;"""
			)
			check2 = con.execute(
			    f"""SELECT oid,relfilenode,reltoastrelid FROM pg_class WHERE relname='{o_relname}';"""
			)
			con.execute(
			    f"""INSERT INTO {o_relname}(data,data_3) VALUES (repeat(\'{symbol2}\', {toast_len}),1);"""
			)
			check3 = con.execute(
			    f"""SELECT oid,relfilenode,reltoastrelid FROM pg_class WHERE relname='{o_relname}';"""
			)
			con.commit()

			v1 = check1[0]
			v2 = check2[0]
			v3 = check3[0]
			relfilenode_id = 1
			reltoastrelid_id = 2
			self.assertTrue(
			    v1[relfilenode_id]
			    != v2[relfilenode_id])  # relfilenode changed after ALTER
			self.assertTrue(
			    v1[reltoastrelid_id]
			    != v2[reltoastrelid_id])  # reltoastrelid changed after ALTER
			self.assertTrue(v2[relfilenode_id] == v3[relfilenode_id])
			self.assertTrue(v2[reltoastrelid_id] == v3[reltoastrelid_id])

			result = self.retrieve_logical_changes()
			#print(result)

			expected1 = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
			expected2 = f"""'\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:unchanged-toast-datum data_3[bigint]:0\n"""
			expected3 = f"""table public.{o_relname}: INSERT: id[integer]:2 data[text]:'"""
			expected_tail = "' data_3[bigint]:1\nCOMMIT\n"

			start = 0  # start index
			b = 0  # border index for checking large output

			def check(expected):
				nonlocal start
				nonlocal b
				nonlocal result
				b = start + len(expected)
				self.assertEqual(result[start:b], expected)
				start = b

			def check_toast(expected, symbol):
				nonlocal start
				nonlocal b
				nonlocal toast_len
				nonlocal result
				check(expected)
				for i in range(b, b + toast_len):
					self.assertEqual(result[i], symbol)
				start += toast_len

			check_toast(expected1, symbol1)
			check(expected2)
			check_toast(expected3, symbol2)
			check(expected_tail)  # this check is OK

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_TOAST_INSERT_UPDATE_rewrite_rel(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;
		""")

		create_slot(node)

		toast_len = 500000  # len of TOASTed value
		symbol1 = 'A'
		symbol2 = 'B'
		symbol3 = 'C'

		# txn operates with two	iterationns of TOAST index descr & with two versions of relation
		# update TOAST-relation is organized as INSERT+DELETE
		with node.connect() as con:
			con.begin()
			con.execute(
			    f"""INSERT INTO {o_relname}(data) VALUES (repeat(\'{symbol1}\', {toast_len}));"""
			)
			check1 = con.execute(
			    f"""SELECT oid,relfilenode,reltoastrelid FROM pg_class WHERE relname='{o_relname}';"""
			)
			con.execute(f"""ALTER TABLE {o_relname} ADD COLUMN data_3 text;""")
			con.execute(
			    f"""ALTER TABLE {o_relname} ALTER COLUMN data_3 TYPE bigint USING 0::bigint;"""
			)
			check2 = con.execute(
			    f"""SELECT oid,relfilenode,reltoastrelid FROM pg_class WHERE relname='{o_relname}';"""
			)
			con.execute(
			    f"""ALTER TABLE {o_relname} ALTER COLUMN data SET STORAGE EXTERNAL;"""
			)
			con.execute(
			    f"""UPDATE {o_relname} SET data   = repeat(\'{symbol2}\', {toast_len}), data_3 = 1 WHERE id = (SELECT max(id) FROM {o_relname});"""
			)
			check3 = con.execute(
			    f"""SELECT oid,relfilenode,reltoastrelid FROM pg_class WHERE relname='{o_relname}';"""
			)
			con.execute(
			    f"""UPDATE {o_relname} SET data   = repeat(\'{symbol3}\', {toast_len}), data_3 = 1 WHERE id = (SELECT max(id) FROM {o_relname});"""
			)
			con.commit()

			v1 = check1[0]
			v2 = check2[0]
			v3 = check3[0]
			relfilenode_id = 1
			reltoastrelid_id = 2
			self.assertTrue(
			    v1[relfilenode_id]
			    != v2[relfilenode_id])  # relfilenode changed after ALTER
			self.assertTrue(
			    v1[reltoastrelid_id]
			    != v2[reltoastrelid_id])  # reltoastrelid changed after ALTER
			self.assertTrue(v2[relfilenode_id] == v3[relfilenode_id])
			self.assertTrue(v2[reltoastrelid_id] == v3[reltoastrelid_id])

			result = self.retrieve_logical_changes()
			#print(result)

			expected_begin = f"""BEGIN\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:'"""
			expected_added = f"""'\ntable public.{o_relname}: INSERT: id[integer]:1 data[text]:unchanged-toast-datum data_3[bigint]:0\n"""
			expected_update1 = f"""table public.{o_relname}: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 data[text]:'"""
			expected_tail1 = "' data_3[bigint]:1\n"
			expected_update2 = f"""table public.{o_relname}: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 data[text]:'"""
			expected_tail2 = "' data_3[bigint]:1\nCOMMIT\n"

			start = 0  # start index
			b = 0  # border index for checking large output

			def check(expected):
				nonlocal start
				nonlocal b
				nonlocal result
				b = start + len(expected)
				self.assertEqual(result[start:b], expected)
				start = b

			def check_toast(expected, symbol):
				nonlocal start
				nonlocal b
				nonlocal toast_len
				nonlocal result
				check(expected)
				for i in range(b, b + toast_len):
					self.assertEqual(result[i], symbol)
				start += toast_len

			check_toast(expected_begin, symbol1)
			check(expected_added)
			check_toast(expected_update1, symbol2)
			check(expected_tail1)
			check_toast(expected_update2, symbol3)
			check(expected_tail2)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_systrees_versions_index_bridge_01(self):
		o_relname = self.o_relname
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
		    CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
		    CREATE INDEX data_idx ON {o_relname} USING btree(data) WITH (orioledb_index = off);
		""")

		create_slot(node)

		node.safe_psql(
		    'postgres', f'''
				BEGIN;
				INSERT INTO {o_relname}(data) VALUES(100);
				ALTER TABLE {o_relname} RENAME COLUMN data TO data_2;
				ALTER TABLE {o_relname} ADD COLUMN data_3 text;
				INSERT INTO {o_relname}(data_2,data_3) VALUES(300,400);
				COMMIT;
			''')

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, "BEGIN\n"
		    f"""table public.{o_relname}: INSERT: id[integer]:1 data[text]:'100'\n"""
		    f"""table public.{o_relname}: INSERT: id[integer]:2 data_2[text]:'300' data_3[text]:'400'\n"""
		    "COMMIT\n")

	def test_switch_logical_xid_subtxn__mixed_ROLLBACK(self):
		o_relname = self.o_relname

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				create_sql = f"""
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub', tables=[f'{o_relname}'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					con1.begin()
					con1.execute(f"""
						SAVEPOINT s0;
						ALTER TABLE {o_relname} RENAME COLUMN data TO newdata;
						INSERT INTO {o_relname}(newdata) VALUES(100);
						ROLLBACK TO s0;
						INSERT INTO {o_relname}(data) VALUES(200);
					""")
					con1.commit()

				# wait until changes apply on subscriber and check them
				sub.catchup()

				with subscriber.connect() as con:
					output = con.execute(f"""SELECT * FROM {o_relname};""")
					self.assertEqual(output, [(2, '200')])

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT(
	        self):  # COMMIT SAVEPOINT x5
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			SAVEPOINT s5;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
table public.{o_relname}: INSERT: id[integer]:3 data[text]:'30'
table public.{o_relname}: INSERT: id[integer]:4 data[text]:'40'
table public.{o_relname}: INSERT: id[integer]:5 data[text]:'50'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_mixed_01(
	        self):  # COMMIT SAVEPOINT x3
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			CREATE TABLE h_tmp(id serial primary key, data text);
			SAVEPOINT s3;
			INSERT INTO h_tmp(data) VALUES('100');
			COMMIT;
		""")

		self.assertEqual(self.node.execute("SELECT * FROM h_tmp;"),
		                 [(1, '100')])

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
table public.{o_relname}: INSERT: id[integer]:3 data[text]:'30'
table public.h_tmp: INSERT: id[integer]:1 data[text]:'100'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT(
	        self):  # ABORT SAVEPOINT x5
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			SAVEPOINT s5;
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT_mixed_01(
	        self):  # ABORT SAVEPOINT x3
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			CREATE TABLE h_tmp(id serial primary key, data text);
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO h_tmp(data) VALUES('100');
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_0(
	        self):  # COMMIT SAVEPOINT x5 + empty SAVEPOINT
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			SAVEPOINT s0;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			SAVEPOINT s5;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
table public.{o_relname}: INSERT: id[integer]:3 data[text]:'30'
table public.{o_relname}: INSERT: id[integer]:4 data[text]:'40'
table public.{o_relname}: INSERT: id[integer]:5 data[text]:'50'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT_0(
	        self):  # ABORT SAVEPOINT x5 + empty SAVEPOINT
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			SAVEPOINT s0;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			SAVEPOINT s5;
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_0xN(
	        self):  # COMMIT SAVEPOINT x3 + empty SAVEPOINT xN
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			SAVEPOINT s0;
			SAVEPOINT s00;
			SAVEPOINT s000;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			SAVEPOINT s0000;
			SAVEPOINT s00000;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
table public.{o_relname}: INSERT: id[integer]:3 data[text]:'30'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT_0xN(
	        self):  # ABORT SAVEPOINT x3 + empty SAVEPOINT xN
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			SAVEPOINT s0;
			SAVEPOINT s00;
			SAVEPOINT s000;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			SAVEPOINT s0000;
			SAVEPOINT s00000;
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_ROLLBACK_s1(
	        self):  # COMMIT SAVEPOINT x2 ROLLBACK s1
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			CREATE TABLE h_tmp(id serial primary key, data text);
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			ROLLBACK TO s1;
			INSERT INTO h_tmp(data) VALUES('100');
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.h_tmp: INSERT: id[integer]:1 data[text]:'100'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT_ROLLBACK_s1(
	        self):  # ABORT SAVEPOINT x2 ROLLBACK s1
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			ROLLBACK TO s1;
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_ROLLBACK_s2(
	        self):  # COMMIT SAVEPOINT x2 ROLLBACK s2
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			ROLLBACK TO s2;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT_ROLLBACK_s2(
	        self):  # ABORT SAVEPOINT x2 ROLLBACK s2
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			ROLLBACK TO s2;
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_ROLLBACK_s3(
	        self):  # COMMIT SAVEPOINT x2 ROLLBACK s3
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			ROLLBACK TO s3;
			SAVEPOINT s5;
			INSERT INTO {o_relname}(data) VALUES('60');
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
table public.{o_relname}: INSERT: id[integer]:3 data[text]:'30'
table public.{o_relname}: INSERT: id[integer]:6 data[text]:'60'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__ABORT_SAVEPOINT_ROLLBACK_s3(
	        self):  # ABORT SAVEPOINT x2 ROLLBACK s3
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('40');
			SAVEPOINT s4;
			INSERT INTO {o_relname}(data) VALUES('50');
			ROLLBACK TO s3;
			SAVEPOINT s5;
			INSERT INTO {o_relname}(data) VALUES('60');
			ABORT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, "")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_ROLLBACK_s0(
	        self):  # COMMIT SAVEPOINT x2 ROLLBACK s0
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			SAVEPOINT s0;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			ROLLBACK TO s0;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(result, '''BEGIN\nCOMMIT\n''')

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_ROLLBACK_s3_empty(
	        self):  # COMMIT SAVEPOINT x2 ROLLBACK s3 empty
		node = self.node
		o_relname = self.o_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s3;
			SAVEPOINT s4;
			SAVEPOINT s5;
			ROLLBACK TO s3;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
table public.{o_relname}: INSERT: id[integer]:3 data[text]:'30'
COMMIT\n""")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_switch_logical_xid_subtxn__COMMIT_SAVEPOINT_ROLLBACK_h2o(
	        self):  # COMMIT SAVEPOINT x2 ROLLBACK s3 empty
		node = self.node
		o_relname = self.o_relname
		h_relname = self.h_relname
		node_prepare_orel(node, o_relname)

		node.safe_psql(
		    'postgres', f"""
			BEGIN;
			SAVEPOINT s0;
			SAVEPOINT s00;
			SAVEPOINT s000;
			CREATE TABLE {h_relname}(id serial primary key, data text);
			SAVEPOINT s1;
			INSERT INTO {o_relname}(data) VALUES('10');
			SAVEPOINT s2;
			INSERT INTO {o_relname}(data) VALUES('20');
			SAVEPOINT s3;
			INSERT INTO {o_relname}(data) VALUES('30');
			SAVEPOINT s4;
			SAVEPOINT s5;
			SAVEPOINT s6;
			ROLLBACK TO s3;
			COMMIT;
		""")

		result = self.retrieve_logical_changes()
		#print(result)
		self.assertEqual(
		    result, f"""BEGIN
table public.{o_relname}: INSERT: id[integer]:1 data[text]:'10'
table public.{o_relname}: INSERT: id[integer]:2 data[text]:'20'
COMMIT\n""")

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
				wait_ready(subscriber)
				a, *b = (subscriber.execute(
				    'postgres', 'select pg_current_xact_id();\n'))[0]
				xids1 = int(a)
				#print(xids1)
				a, *b = (publisher.execute(
				    'postgres', 'select orioledb_get_current_oxid();\n'))[0]
				oxidp1 = int(a)
				#print(oxidp1)
				a, *b = (subscriber.execute(
				    'postgres', 'select orioledb_get_current_oxid();\n'))[0]
				oxids1 = int(a)
				#print(oxids1)

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
					a, *b = (publisher.execute(
					    'postgres',
					    'select orioledb_get_current_oxid();\n'))[0]
					oxidp2 = int(a)
					#print(oxidp2)
					a, *b = (subscriber.execute(
					    'postgres', 'select pg_current_xact_id();\n'))[0]
					xids2 = int(a)
					#print(xids2)
					a, *b = (subscriber.execute(
					    'postgres',
					    'select orioledb_get_current_oxid();\n'))[0]
					oxids2 = int(a)
					#print(oxids2)
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test1 ORDER BY id'), [(2, '2'),
					                                               (4, 'NO')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test2 ORDER BY id'), [])
					# we had no PG transactions on subscriber, xid is incremented only by pg_current_xact_id()
					self.assertEqual(xids2 - xids1, 1)
					# we had 4 transactions on publisher, plus oxid is incremented by orioledb_get_current_oxid()
					self.assertEqual(oxidp2 - oxidp1, 5)
					# on subscriber one transaction from publisher that modified only o_test2 was not replicated
					self.assertEqual(oxids2 - oxids1, 4)

	def test_logical_subscription_toastable_insert(self):
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
				wait_ready(subscriber)

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

	# Update with changed pkey on a table with TOAST attributes (i.e reinsert)
	def test_logical_subscription_toast_update_pkey(self):
		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test (id int PRIMARY KEY, bid int, junk text
					) USING orioledb;
					CREATE TABLE o_test_ctid (id int, bid int, junk text
					) USING orioledb;
					ALTER TABLE o_test_ctid REPLICA IDENTITY FULL;
					CREATE TABLE o_test_secondary (id int PRIMARY KEY, bid int, junk text
					) USING orioledb;
					CREATE INDEX ON o_test_secondary (bid);
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish(
				    'test_pub',
				    tables=['o_test', 'o_test_ctid', 'o_test_secondary'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "INSERT INTO o_test (id, bid, junk) VALUES (1, 1, repeat(pi()::text,20000));"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid (id, bid, junk) VALUES (1, 1, repeat(pi()::text,20000));"
						)
						con1.execute(
						    "INSERT INTO o_test_secondary (id, bid, junk) VALUES (1, 1, repeat(pi()::text,20000));"
						)
						con2.execute(
						    "INSERT INTO o_test (id, bid) VALUES (2, 2);")
						con2.execute(
						    "INSERT INTO o_test_ctid (id, bid) VALUES (2, 2);")
						con2.execute(
						    "INSERT INTO o_test_secondary (id, bid) VALUES (2, 2);"
						)

						con1.execute("UPDATE o_test SET id = 6 WHERE id = 1;")
						con1.execute(
						    "UPDATE o_test_ctid SET id = 6 WHERE id = 1;")
						con1.execute(
						    "UPDATE o_test_secondary SET id = 6 WHERE id = 1;")

						con1.commit()
						con2.commit()

					self.assertListEqual(
					    publisher.execute(
					        'SELECT id, bid FROM o_test ORDER BY id'),
					    [(2, 2), (6, 1)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT id, bid FROM o_test_ctid ORDER BY id'),
					    [(2, 2), (6, 1)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT id, bid FROM o_test_secondary ORDER BY id'
					    ), [(2, 2), (6, 1)])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					# sub.poll_query_until("SELECT orioledb_recovery_synchronized();", expected=True)
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT id, bid FROM o_test ORDER BY id'),
					    [(2, 2), (6, 1)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT id, bid FROM o_test_ctid ORDER BY id'),
					    [(2, 2), (6, 1)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT id, bid FROM o_test_secondary ORDER BY id'
					    ), [(2, 2), (6, 1)])

	def test_logical_subscription_toastable_update(self):
		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test (
			                        i   int PRIMARY KEY,
						data1 text,
						data2 text,
			                        data3 text
					) USING orioledb;
					CREATE TABLE o_test_ctid (
			                        i   int,
						data1 text,
						data2 text,
			                        data3 text
					) USING orioledb;
					ALTER TABLE o_test_ctid REPLICA IDENTITY FULL;
					CREATE TABLE o_test_secondary (
			                        i   int PRIMARY KEY,
						data1 text,
						data2 text,
			                        data3 text
					) USING orioledb;
					CREATE INDEX ON o_test_secondary (data2);
					CREATE TABLE o_test_ctid_secondary (
			                        i   int,
						data1 text,
						data2 text,
			                        data3 text
					) USING orioledb;
					ALTER TABLE o_test_ctid_secondary REPLICA IDENTITY FULL;
					CREATE INDEX ON o_test_ctid_secondary (data2);
					CREATE TABLE o_test_2 (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE TABLE o_test_ctid_2 (
						data1 text,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					ALTER TABLE o_test_ctid_2 REPLICA IDENTITY FULL;
					CREATE TABLE o_test_secondary_2 (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
                                    i int
					) USING orioledb;
					CREATE INDEX ON o_test_secondary_2 (data2);
					CREATE TABLE o_test_ctid_secondary_2 (
						data1 text,
						data2 text,
			                        data3 text,
                                    i int
					) USING orioledb;
					ALTER TABLE o_test_ctid_secondary_2 REPLICA IDENTITY FULL;
					CREATE INDEX ON o_test_ctid_secondary_2 (data2);

				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish(
				    'test_pub',
				    tables=[
				        'o_test', 'o_test_ctid', 'o_test_secondary',
				        'o_test_ctid_secondary', 'o_test_2', 'o_test_ctid_2',
				        'o_test_secondary_2', 'o_test_ctid_secondary_2'
				    ])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "INSERT INTO o_test VALUES(1, 'foofoo','barbar', 'aaaaaa');"
						)
						con2.execute(
						    "INSERT INTO o_test VALUES(2, 'mmm','nnn', 'ooo');"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid VALUES(1, 'foofoo','barbar', 'aaaaaa');"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid VALUES(2, 'mmm','nnn', 'ooo');"
						)
						con1.execute(
						    "INSERT INTO o_test_secondary VALUES(1, 'foofoo','barbar', 'aaaaaa');"
						)
						con2.execute(
						    "INSERT INTO o_test_secondary VALUES(2, 'mmm','nnn', 'ooo');"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid_secondary VALUES(1, 'foofoo','barbar', 'aaaaaa');"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid_secondary VALUES(2, 'mmm','nnn', 'ooo');"
						)
						con1.execute(
						    "INSERT INTO o_test_2 VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_2 VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid_2 VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid_2 VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_secondary_2 VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_secondary_2 VALUES('mmm','nnn', 'ooo', 2);"
						)
						con1.execute(
						    "INSERT INTO o_test_ctid_secondary_2 VALUES('foofoo','barbar', 'aaaaaa', 1);"
						)
						con2.execute(
						    "INSERT INTO o_test_ctid_secondary_2 VALUES('mmm','nnn', 'ooo', 2);"
						)

						con1.commit()
						con2.commit()

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "UPDATE o_test SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_ctid SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_ctid SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_secondary SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_secondary SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_ctid_secondary SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_ctid_secondary SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_2 SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_2 SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_ctid_2 SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_ctid_2 SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_secondary_2 SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_secondary_2 SET data2 = 'ppp' where data2 = 'nnn';"
						)
						con1.execute(
						    "UPDATE o_test_ctid_secondary_2 SET data2 = 'ssssss' where data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test_ctid_secondary_2 SET data2 = 'ppp' where data2 = 'nnn';"
						)

						con1.commit()
						con2.commit()


#					publisher.safe_psql("CHECKPOINT;")
#					subscriber.execute("SELECT orioledb_get_current_oxid();")
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_secondary ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid_secondary ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid_2 ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test_2 ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_secondary_2 ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_ctid_secondary_2 ORDER BY i'
					    ), [('foofoo', 'ssssss', 'aaaaaa', 1),
					        ('mmm', 'ppp', 'ooo', 2)])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					# sub.poll_query_until("SELECT orioledb_recovery_synchronized();", expected=True)
					#					subscriber.safe_psql("CHECKPOINT;")
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    subscriber.execute('SELECT * FROM o_test ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_secondary ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid_secondary ORDER BY i'),
					    [(1, 'foofoo', 'ssssss', 'aaaaaa'),
					     (2, 'mmm', 'ppp', 'ooo')])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid_2 ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_2 ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_secondary_2 ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_ctid_secondary_2 ORDER BY i'
					    ), [('foofoo', 'ssssss', 'aaaaaa', 1),
					        ('mmm', 'ppp', 'ooo', 2)])

	# Update with non-changed pkey of by-reference type
	def test_logical_subscription_byref_pkey_update(self):
		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()

			with subscriber.start() as subscriber:
				create_sql = """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE o_test (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE TABLE o_test_secondary (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_secondary (data2);
					CREATE TABLE o_test_toast (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE TABLE o_test_toasted_update (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE TABLE o_test_toast_secondary (
						data1 text PRIMARY KEY,
						data2 text,
			                        data3 text,
			                        i   int
					) USING orioledb;
					CREATE INDEX ON o_test_toast_secondary (data2);
				"""
				publisher.safe_psql(create_sql)
				subscriber.safe_psql(create_sql)

				pub = publisher.publish('test_pub',
				                        tables=[
				                            'o_test, o_test_secondary',
				                            'o_test_toast',
				                            'o_test_toast_secondary',
				                            'o_test_toasted_update'
				                        ])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "INSERT INTO o_test VALUES('foofoo','barbar', 'aaaaaa', 1);"
						    "INSERT INTO o_test_secondary VALUES('foofoo','barbar', 'aaaaaa', 1);"
						    "INSERT INTO o_test_toast VALUES('foofoo','barbar', repeat(pi()::text,20000), 1);"
						    "INSERT INTO o_test_toast_secondary VALUES('foofoo','barbar', repeat(pi()::text,20000), 1);"
						    "INSERT INTO o_test_toasted_update VALUES('foofoo','barbar', repeat(pi()::text,20000), 1);"
						)
						con2.execute(
						    "INSERT INTO o_test VALUES('mmm','nnn', 'ooo', 2);"
						    "INSERT INTO o_test_secondary VALUES('mmm','nnn', 'ooo', 2);"
						    "INSERT INTO o_test_toast VALUES('mmm','nnn', repeat(pi()::text,20000), 2);"
						    "INSERT INTO o_test_toast_secondary VALUES('mmm','nnn', repeat(pi()::text,20000), 2);"
						    "INSERT INTO o_test_toasted_update VALUES('mmm','nnn', repeat(pi()::text,20000), 2);"
						)
						con1.commit()
						con2.commit()

				with publisher.connect() as con1:
					with publisher.connect() as con2:
						con1.execute(
						    "UPDATE o_test SET data2 = 'ssssss' where data2 = 'barbar';"
						    "UPDATE o_test_secondary SET data2 = 'ssssss' where data2 = 'barbar';"
						    "UPDATE o_test_toast SET data2 = 'ssssss' where data2 = 'barbar';"
						    "UPDATE o_test_toast_secondary SET data2 = 'ssssss' where data2 = 'barbar';"
						    "UPDATE o_test_toasted_update SET data3 = repeat('123', 20000) where data3 = repeat(pi()::text,20000) and data2 = 'barbar';"
						)
						con2.execute(
						    "UPDATE o_test SET data2 = 'ppp' where data2 = 'nnn';"
						    "UPDATE o_test_secondary SET data2 = 'ppp' where data2 = 'nnn';"
						    "UPDATE o_test_toast SET data2 = 'ppp' where data2 = 'nnn';"
						    "UPDATE o_test_toast_secondary SET data2 = 'ppp' where data2 = 'nnn';"
						    "UPDATE o_test_toasted_update SET data3 = repeat('246', 20000) where data3 = repeat(pi()::text,20000) and data2 = 'nnn';"
						)
						con1.commit()
						con2.commit()

					self.assertListEqual(
					    publisher.execute('SELECT * FROM o_test ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT * FROM o_test_secondary ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT data1, data2, i FROM o_test_toast ORDER BY i'
					    ), [('foofoo', 'ssssss', 1), ('mmm', 'ppp', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT data1, data2, i FROM o_test_toast_secondary ORDER BY i'
					    ), [('foofoo', 'ssssss', 1), ('mmm', 'ppp', 2)])
					self.assertListEqual(
					    publisher.execute(
					        'SELECT data1, data2, left(data3, 20), i  FROM o_test_toasted_update ORDER BY i'
					    ), [('foofoo', 'barbar', '12312312312312312312', 1),
					        ('mmm', 'nnn', '24624624624624624624', 2)])

					# wait until changes apply on subscriber and check them
					sub.catchup()
					self.assertListEqual(
					    subscriber.execute('SELECT * FROM o_test ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT * FROM o_test_secondary ORDER BY i'),
					    [('foofoo', 'ssssss', 'aaaaaa', 1),
					     ('mmm', 'ppp', 'ooo', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT data1, data2, i FROM o_test_toast ORDER BY i'
					    ), [('foofoo', 'ssssss', 1), ('mmm', 'ppp', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT data1, data2, i FROM o_test_toast_secondary ORDER BY i'
					    ), [('foofoo', 'ssssss', 1), ('mmm', 'ppp', 2)])
					self.assertListEqual(
					    subscriber.execute(
					        'SELECT data1, data2, left(data3, 20), i FROM o_test_toasted_update ORDER BY i'
					    ), [('foofoo', 'barbar', '12312312312312312312', 1),
					        ('mmm', 'nnn', '24624624624624624624', 2)])

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
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
