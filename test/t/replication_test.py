#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import time
import os
import re
import subprocess

from .base_test import BaseTest


def catchup_orioledb(replica):
	replica.catchup()
	replica.poll_query_until("SELECT orioledb_recovery_synchronized();",
	                         expected=True)


class ReplicationTest(BaseTest):

	def test_replication_simple(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.execute("CREATE EXTENSION orioledb;")
				master.execute("CREATE TABLE o_test\n"
				               "    (id integer NOT NULL)\n"
				               "USING orioledb;")
				master.execute("INSERT INTO o_test VALUES (1);")

				# wait for synchronization
				catchup_orioledb(replica)
				self.assertEqual(
				    1,
				    replica.execute("SELECT * FROM o_test;")[0][0])
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

	def test_replication_in_progress(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("""CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text,
						PRIMARY KEY (id)
					) USING orioledb;""")
				replica.catchup()

				master.safe_psql("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
				master.safe_psql("""DELETE FROM o_test WHERE id > 5000;""")
				master.safe_psql("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(5001, 7500, 1) id);""")
				replica.catchup()

				# ensure that we do not see in-progress transaction on the replica
				count = replica.execute(
				    """SELECT COUNT(*) FROM o_test;""")[0][0]
				self.assertTrue(count == 0 or count == 10000 or count == 5000
				                or count == 7500)
				count = replica.execute(
				    """SELECT count(*) FROM (SELECT * FROM o_test ORDER BY id DESC) id;"""
				)[0][0]
				self.assertTrue(count == 0 or count == 10000 or count == 5000
				                or count == 7500)
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

	def test_replication_drop(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("""CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text
					) USING orioledb;""")
				master.safe_psql("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
				master.execute("DROP TABLE o_test;")

				# check that DROP executed on master
				self.assertTrue(self.all_tables_dropped(master))

				# wait for synchronization
				catchup_orioledb(replica)

				# check that DROP replicated
				self.assertTrue(self.all_tables_dropped(replica))
				self.assertFalse(
				    replica.execute("SELECT orioledb_has_retained_undo();")[0]
				    [0])

	def test_replication_create_drop_commit(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("CREATE EXTENSION orioledb;")
				con = master.connect()

				con.begin()
				con.execute("""CREATE TABLE o_test (
									id integer NOT NULL,
									val text
							   ) USING orioledb;""")
				con.execute("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
				con.execute("DROP TABLE o_test;")
				con.commit()
				con.close()

				# check that DROP executed on master
				self.assertTrue(self.all_tables_dropped(master))

				# wait for synchronization
				catchup_orioledb(replica)

				# check that DROP replicated
				self.assertTrue(self.all_tables_dropped(replica))
				self.assertFalse(
				    replica.execute("SELECT orioledb_has_retained_undo();")[0]
				    [0])

	def test_replication_create_rollback(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("CREATE EXTENSION orioledb;")
				with master.connect() as con1:
					con1.execute("""CREATE TABLE o_test (
							id integer NOT NULL,
							val text
						) USING orioledb;""")
					con1.execute("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
					con1.rollback()

				# check that table dropped after rollback
				self.assertTrue(self.all_tables_dropped(master))

				# wait for synchronization
				catchup_orioledb(replica)

				# check that drop replicated
				self.assertTrue(self.all_tables_dropped(replica))
				self.assertFalse(
				    replica.execute("SELECT orioledb_has_retained_undo();")[0]
				    [0])

	def test_replication_create_truncate_commit(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("CREATE EXTENSION orioledb;")
				with master.connect() as con:
					con.begin()
					con.execute("""CREATE TABLE o_test (
										id integer NOT NULL,
										val text
								) USING orioledb;""")
					con.execute("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
					con.execute("TRUNCATE o_test;")
					con.execute("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
					con.commit()
				self.assertEqual(1, self.get_tbl_count(master))

				# wait for synchronization
				catchup_orioledb(replica)
				replica.safe_psql("SELECT * FROM o_test;")

				self.assertEqual(1, self.get_tbl_count(replica))
				self.assertTrue(self.has_only_one_relnode(replica))

				self.assertFalse(
				    replica.execute("SELECT orioledb_has_retained_undo();")[0]
				    [0])

	def test_replication_drop_truncate_rollback(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("""CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text
					) USING orioledb;""")

				# wait for synchronization
				replica.catchup()
				replica.safe_psql("SELECT * FROM o_test;")

				with master.connect() as con1:
					con1.execute("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
					con1.execute("DROP TABLE o_test;")
					con1.rollback()

				catchup_orioledb(replica)
				replica.safe_psql("SELECT * FROM o_test;")

				self.assertTrue(self.has_only_one_relnode(replica))
				self.assertEqual(1, self.get_tbl_count(replica))

				with master.connect() as con1:
					con1.execute("TRUNCATE o_test;")
					con1.execute("""INSERT INTO o_test (
									SELECT id, id || 'val'
									FROM
									generate_series(1, 10000, 1) id);""")
					con1.rollback()

				# wait for synchronization
				catchup_orioledb(replica)
				replica.safe_psql("SELECT * FROM o_test;")

				self.assertTrue(self.has_only_one_relnode(replica))
				self.assertEqual(1, self.get_tbl_count(replica))

				self.assertFalse(
				    replica.execute("SELECT orioledb_has_retained_undo();")[0]
				    [0])

	def test_replication_non_transactional_truncate(self):
		node = self.node
		node.start()
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE TABLE o_test_1(
							val_1 int
						) USING orioledb;
						INSERT INTO o_test_1 VALUES (1);
						TRUNCATE TABLE o_test_1;
						TRUNCATE TABLE o_test_1;
						INSERT INTO o_test_1 VALUES (1);
					""")
					con1.commit()
					catchup_orioledb(replica)

	def test_replication_non_root_eviction(self):
		with self.node as master:
			self.node.append_conf(
			    'postgresql.conf', "log_min_messages = notice\n"
			    "max_worker_processes = 64\n"
			    "orioledb.undo_buffers = 256MB\n"
			    "orioledb.recovery_pool_size = 50\n")
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
				    "CREATE TABLE IF NOT EXISTS o_test (\n"
				    "	key integer NOT NULL,\n"
				    "	val integer NOT NULL,\n"
				    "	val2 text NOT NULL,\n"
				    "	PRIMARY KEY (key)\n"
				    ") USING orioledb;\n")

				n = 500000
				con = master.connect()
				con.begin()
				con.execute(
				    "INSERT INTO o_test (SELECT id, %s - id, repeat('x', 1000) FROM generate_series(%s, %s, 1) id);"
				    % (str(n), str(1), str(n)))
				con.commit()
				con.close()

				# wait for synchronization
				catchup_orioledb(replica)

				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)
				self.assertEqual(
				    500000,
				    replica.execute(
				        "SELECT count(*) FROM (SELECT * FROM o_test ORDER BY key) x;"
				    )[0][0])

	def test_replication_root_eviction(self):
		INDEX_NOT_LOADED = "Index o_evicted_pkey: not loaded"
		with self.node as master:
			master.append_conf(
			    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
			    "checkpoint_timeout = 86400\n"
			    "max_wal_size = 1GB\n"
			    "enable_seqscan = off\n"
			    "orioledb.recovery_pool_size = 1\n"
			    "orioledb.debug_disable_bgwriter = true\n")
			master.start()
			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql(
				    'postgres', """
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE IF NOT EXISTS o_test (
						key integer NOT NULL,
						val integer NOT NULL,
						val2 integer NOT NULL,
						val3 integer NOT NULL,
						val4 integer NOT NULL,
						PRIMARY KEY (key)
					) USING orioledb;
					CREATE TABLE IF NOT EXISTS o_evicted (
						key SERIAL NOT NULL,
						val int NOT NULL,
						PRIMARY KEY (key)
					) USING orioledb;
					INSERT INTO o_evicted (val)
						SELECT val id FROM generate_series(1001, 1500, 1) val;
				""")
				self.assertEqual(
				    500,
				    master.execute("SELECT count(*) FROM o_evicted;")[0][0])

				n = 400000
				step = 1000

				self.assertNotEqual(
				    master.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
				    )[0][0].split('\n')[0], INDEX_NOT_LOADED)

				with master.connect('postgres') as con:
					for i in range(1, n, step):
						con.execute("""
							INSERT INTO o_test
								(SELECT id, %s - id, %s - id,
										%s - id, %s - id FROM
									generate_series(%s, %s, 1) id);
						""" % (str(n), str(i), str(i), str(i), str(i), str(i + step - 1)))
					con.commit()

				self.assertEqual(
				    n,
				    master.execute("SELECT count(*) FROM o_test;")[0][0])
				self.assertEqual(
				    n,
				    master.execute("SELECT count(*) FROM o_test;")[0][0])

				self.assertEqual(
				    master.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
				    )[0][0].split('\n')[0], INDEX_NOT_LOADED)

				# wait for synchronization
				catchup_orioledb(replica)
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

				self.assertEqual(
				    500,
				    replica.execute("SELECT count(*) FROM o_evicted;")[0][0])
				self.assertNotEqual(
				    replica.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
				    )[0][0].split('\n')[0], INDEX_NOT_LOADED)

				self.assertEqual(
				    n,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])
				self.assertEqual(
				    n,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])
				self.assertEqual(
				    n,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])
				self.assertEqual(
				    n,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])

				self.assertEqual(
				    replica.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
				    )[0][0].split('\n')[0], INDEX_NOT_LOADED)
				self.assertEqual(
				    500,
				    replica.execute("SELECT count(*) FROM o_evicted;")[0][0])

				self.assertNotEqual(
				    replica.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e');"
				    )[0][0].split('\n')[0], INDEX_NOT_LOADED)

	def test_replica_checkpoint(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.safe_psql("""CREATE EXTENSION orioledb;
					CREATE TABLE o_test (
						id integer NOT NULL,
						val text,
						PRIMARY KEY (id)
					) USING orioledb;""")
				replica.catchup()

				con = master.connect()
				con.begin()
				con.execute("""INSERT INTO o_test (
							   SELECT id, id || 'val'
							   FROM
							   generate_series(1, 10000, 1) id);""")

				replica.catchup()
				replica.safe_psql('CHECKPOINT;')
				replica.stop()
				replica.start()

				con.execute("""INSERT INTO o_test (
							   SELECT id, id || 'val'
							   FROM
							   generate_series(10001, 20000, 1) id);""")
				con.commit()
				con.close()
				replica.catchup()

				# ensure that we do not see in-progress transaction on the replica
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)
				count = replica.execute(
				    """SELECT COUNT(*) FROM o_test;""")[0][0]
				self.assertEqual(count, 20000)

	def test_remote_apply(self):
		with self.node as master:
			master.append_conf(filename='postgresql.conf',
			                   synchronous_commit='remote_apply',
			                   default_table_access_method='orioledb')
			master.start()

			# create a backup
			with self.getReplica() as replica:
				master.safe_psql("""CREATE EXTENSION orioledb;""")
				pgbench = master.pgbench(options=["-i", "-s", "1"],
				                         stdout=subprocess.DEVNULL,
				                         stderr=subprocess.DEVNULL)
				self.assertEqual(pgbench.wait(), 0)
				replica.start()
				master.stop()
				master.append_conf(
				    filename='postgresql.conf',
				    synchronous_standby_names='FIRST 1 (replica)')
				master.start()
				pgbench = master.pgbench(options=[
				    "--client", "16", "--jobs", "4", "--protocol", "prepared",
				    "--progress", "10", "--time", "20"
				],
				                         stdout=subprocess.DEVNULL,
				                         stderr=subprocess.DEVNULL)
				self.assertEqual(pgbench.wait(), 0)

				pgbench = master.pgbench(options=[
				    "--client", "16", "--jobs", "4", "--protocol", "prepared",
				    "--progress", "10", "--time", "20"
				],
				                         stdout=subprocess.DEVNULL,
				                         stderr=subprocess.DEVNULL)
				self.assertEqual(pgbench.wait(), 0)

	def test_replication_column_ddl(self):
		with self.node as master:
			master.start()

			with self.getReplica().start() as replica:
				master.execute("CREATE EXTENSION orioledb;")
				master.execute("CREATE TABLE o_test\n"
				               "    (id integer NOT NULL)\n"
				               "USING orioledb;")
				master.execute("INSERT INTO o_test VALUES (1);")

				master.execute("ALTER TABLE o_test ADD COLUMN val int;")
				master.execute(
				    "ALTER TABLE o_test RENAME COLUMN val TO val_2;")
				master.execute("ALTER TABLE o_test DROP COLUMN val_2")

				# wait for synchronization
				catchup_orioledb(replica)
				self.assertEqual(
				    1,
				    replica.execute("SELECT * FROM o_test;")[0][0])
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

	def test_replication_create_table_add_column_same_trx(self):
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
				""")
				with master.connect() as con:
					con.begin()
					con.execute("""
						CREATE TABLE o_test_1 (
							a int
						) USING orioledb;

						ALTER TABLE o_test_1 ADD COLUMN b serial;

						INSERT INTO o_test_1 VALUES (1), (2), (3);
					""")
					con.commit()

				catchup_orioledb(replica)
				self.assertEqual(master.execute("SELECT * FROM o_test_1"),
				                 [(1, 1), (2, 2), (3, 3)])
				self.assertEqual(replica.execute("SELECT * FROM o_test_1"),
				                 [(1, 1), (2, 2), (3, 3)])

	def test_replication_default_domain(self):
		node = self.node
		node.start()
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;
						CREATE DOMAIN d1 int DEFAULT 1;
						CREATE TABLE o_test_1 (
							val_1 d1 DEFAULT 2
						) USING orioledb;
					""")

					con1.commit()

					self.catchup_orioledb(replica)

	def test_replication_primary_column_after_dropped(self):
		node = self.node
		node.start()
		with self.node as master:
			with self.getReplica() as replica:
				replica.append_conf('orioledb.recovery_pool_size = 1')
				replica.start()
				node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE TABLE o_test_1 (
							val_1 int,
							val_2 int,
							PRIMARY KEY (val_2)
						) USING orioledb;
					""")
					con1.execute("ALTER TABLE o_test_1 DROP COLUMN val_1;")
					con1.execute("INSERT INTO o_test_1 VALUES (1), (3);")
					con1.commit()

				self.assertEqual([(1, ), (3, )],
				                 master.execute("""
									SELECT * FROM o_test_1 ORDER BY val_2;
								 """))

				catchup_orioledb(replica)

				self.assertEqual([(1, ), (3, )],
				                 replica.execute("""
									SELECT * FROM o_test_1 ORDER BY val_2;
								 """))

	def test_replication_workers_synchronize_shutdown_concurrent(self):
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()
					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TEMP TABLE o_test_1 (
							val_1 text COLLATE "C"
						) USING orioledb;

						CREATE INDEX ON o_test_1(val_1);

						INSERT INTO o_test_1
							VALUES ('a'), ('b'), ('c'), ('d');
					""")

					con1.commit()

					self.catchup_orioledb(replica)

	def test_replicate_table_rewrite(self):
		node = self.node
		node.start()
		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""

						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(
							val_1 text NOT NULL COLLATE "C",
							val_2 varchar NOT NULL,
							val_3 integer,
							PRIMARY KEY (val_1)
						)USING orioledb;

						INSERT INTO o_test_1 VALUES ('ABC1', 'ABC2', NULL), ('ABC2', 'ABC4', NULL),
													('ABC3', 'ABC6', NULL);

						ALTER TABLE o_test_1 ALTER val_2 TYPE char
							USING substr(val_2, substr(val_2,4,1)::int / 2, 1);
					""")
					con1.commit()

					catchup_orioledb(replica)

	def test_replication_row_type(self):
		node = self.node
		node.start()
		with self.node as master:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION IF NOT EXISTS orioledb;

					CREATE TYPE o_type_1 AS (
						a int,
						b int
					);

					CREATE TABLE o_test_1(
						val_1 o_type_1 NOT NULL,
						val_2 int NOT NULL DEFAULT 5
					) USING orioledb;

					INSERT INTO o_test_1
						SELECT (id, id * 2)::o_type_1
							FROM generate_series(1, 2) id;

					ALTER TABLE o_test_1 ADD COLUMN val_3 text DEFAULT 'abc';
					CREATE INDEX ind_1 ON o_test_1 (val_3);
					ALTER TABLE o_test_1 ADD PRIMARY KEY (val_1);
				""")

				self.assertEqual(master.execute("TABLE o_test_1"),
				                 [('(1,2)', 5, 'abc'), ('(2,4)', 5, 'abc')])
				self.catchup_orioledb(replica)
				self.assertEqual(replica.execute("TABLE o_test_1"),
				                 [('(1,2)', 5, 'abc'), ('(2,4)', 5, 'abc')])

	def test_replication_table_rewrite(self):
		node = self.node
		node.append_conf('orioledb.recovery_pool_size = 1')
		node.append_conf('orioledb.recovery_idx_pool_size = 1')
		node.start()
		with self.node as master:
			with self.getReplica() as replica:
				replica.start()
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1 (
							val_1 int,
							val_2 boolean
						) USING orioledb;

						CREATE UNIQUE INDEX ind_1
							ON o_test_1 ((val_1::text COLLATE "C"));

						ALTER TABLE o_test_1 ALTER val_2 TYPE TEXT;
					""")

					con1.commit()

					self.catchup_orioledb(replica)

	def test_replication_temp_table_data_cleanup(self):
		node = self.node
		node.append_conf('orioledb.recovery_pool_size = 1')
		node.append_conf('orioledb.recovery_idx_pool_size = 1')
		node.start()
		with self.node as master:
			master.safe_psql("""
				CREATE EXTENSION orioledb;
			""")

			with master.connect() as con1:
				con1.begin()
				con1.execute("""
					CREATE TEMP TABLE o_test (
						f1 int,
						f2 text
					) USING orioledb;
				""")
				con1.execute("""
					INSERT INTO o_test VALUES (1, 'A'), (2, 'B');
				""")
				con1.execute("""
					CREATE UNIQUE INDEX o_test_idx1 ON o_test (f1);
				""")
				con1.execute("""
					ALTER TABLE o_test ALTER f1 TYPE text
						COLLATE "POSIX" USING f1::text || f2;
				""")
				con1.commit()
				master.safe_psql("""
					CHECKPOINT;
				""")
			with self.getReplica() as replica:
				replica.start()
				with master.connect() as con1:
					con1.begin()
					con1.execute("""
						CREATE TEMP TABLE o_test_3 (
							f1 int,
							f2 text
						) USING orioledb;
						INSERT INTO o_test_3 VALUES (1, 'A'), (2, 'B');

						CREATE UNIQUE INDEX o_test_3_idx1 ON o_test_3 (f1);

						SET LOCAL enable_seqscan = off;
						SELECT * FROM o_test_3 ORDER BY f1;

						ALTER TABLE o_test_3 ALTER f1 TYPE text
							COLLATE "POSIX" USING f1::text || f2;

						SELECT * FROM o_test_3 ORDER BY f1;
					""")
					master.execute("""
						CHECKPOINT;
					""")
					self.assertEqual([('1A', 'A'), ('2B', 'B')],
					                 con1.execute("TABLE o_test_3"))

					con1.commit()
					self.assertEqual([('1A', 'A'), ('2B', 'B')],
					                 con1.execute("TABLE o_test_3"))

					self.catchup_orioledb(replica)
					self.assertEqual(
					    master.execute("""
										SELECT c.relname
										FROM orioledb_table ot JOIN
											pg_database db ON db.oid = ot.datoid JOIN
											pg_class c ON c.oid = ot.reloid
										WHERE db.datname = current_database()
										ORDER BY c.relname
									"""), [('o_test_3', )])
					self.assertEqual(
					    replica.execute("""
										SELECT c.relname
										FROM orioledb_table ot JOIN
											pg_database db ON db.oid = ot.datoid JOIN
											pg_class c ON c.oid = ot.reloid
										WHERE db.datname = current_database()
										ORDER BY c.relname
									"""), [])

					master.stop(['-m', 'immediate'])
					master.start()
					master.stop(['-m', 'immediate'])

					master.start()
					master.stop()
					master.start()
					master.stop()
					master.start()
					master.stop()
					master.start()
					self.assertEqual(
					    master.execute("""
											SELECT c.relname
											FROM orioledb_table ot JOIN
												pg_database db ON db.oid = ot.datoid JOIN
												pg_class c ON c.oid = ot.reloid
											WHERE db.datname = current_database()
											ORDER BY c.relname
									"""), [])
					self.assertEqual(
					    replica.execute("""
										SELECT c.relname
										FROM orioledb_table ot JOIN
											pg_database db ON db.oid = ot.datoid JOIN
											pg_class c ON c.oid = ot.reloid
										WHERE db.datname = current_database()
										ORDER BY c.relname
									"""), [])
					master.stop()
					self.assertTrue(self.all_tables_dropped(master))
					self.assertTrue(self.all_tables_dropped(replica))

	def test_replication_temp_table_pkey(self):
		node = self.node
		node.start()

		with self.node as master:
			with self.getReplica().start() as replica:
				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1(val_1 serial)USING orioledb;
						CREATE TABLE o_test_2(val_1 serial, val_2 text)USING orioledb;
						CREATE TABLE o_test_3(val_1 serial)USING orioledb;
						CREATE TABLE o_test_4(val_1 serial, val_2 text)USING orioledb;

						DROP TABLE o_test_1, o_test_2, o_test_3, o_test_4;

						CREATE TEMPORARY TABLE o_test_5(val_1 int PRIMARY KEY, val_2 int) USING orioledb;

						CREATE TABLE o_test_6(val_1 int)USING orioledb;
						INSERT INTO o_test_6 SELECT i FROM generate_series(1,100)i;

						CHECKPOINT;

						DROP TABLE o_test_6;
					""")

					con1.commit()

					self.catchup_orioledb(replica)

					replica.stop()

					replica.start()

	def has_only_one_relnode(self, node):
		orioledb_files = self.get_orioledb_files(node)
		oid_list = [
		    re.match(r'(\d+_\d+).*', x).group(1) for x in orioledb_files
		]
		if len(list(set(oid_list))) != 1:
			print(oid_list)
		return len(list(set(oid_list))) == 1

	def get_tbl_count(self, node):
		return node.execute(
		    'postgres', 'SELECT count(*) FROM orioledb_table_oids();')[0][0]

	def get_orioledb_files(self, node):
		orioledb_dir = node.data_dir + "/orioledb_data"
		all_files = []
		for ff in os.listdir(orioledb_dir):
			dbDir = os.path.join(orioledb_dir, ff)
			if os.path.isdir(dbDir) and int(ff) > 1:
				for f in os.listdir(dbDir):
					m = re.match(r'(\d+).*', f)
					all_files.append(ff + '_' + f)
		return all_files

	def all_tables_dropped(self, node):
		return len(self.get_orioledb_files(node)) == 0
