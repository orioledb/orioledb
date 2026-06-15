#!/usr/bin/env python3
# coding: utf-8

import os
import re
import subprocess
import time
from testgres import NodeStatus

from .base_test import BaseTest, ThreadQueryExecutor, wait_checkpointer_stopevent


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
				self.catchup_orioledb(replica)
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
				self.catchup_orioledb(replica)

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
				self.catchup_orioledb(replica)

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
				self.catchup_orioledb(replica)

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
				self.catchup_orioledb(replica)
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

				self.catchup_orioledb(replica)
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
				self.catchup_orioledb(replica)
				replica.safe_psql("SELECT * FROM o_test;")

				self.assertTrue(self.has_only_one_relnode(replica))
				self.assertEqual(1, self.get_tbl_count(replica))

				self.assertFalse(
				    replica.execute("SELECT orioledb_has_retained_undo();")[0]
				    [0])

	def test_replication_multiple_checkpoints_rollback(self):
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

				con1 = master.connect()
				con1.execute("""INSERT INTO o_test (
								SELECT id, id || 'val'
								FROM
								generate_series(1, 10000, 1) id);""")

				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				replica.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				replica.restart()

				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				replica.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				replica.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				replica.restart()

				con1.rollback()
				self.catchup_orioledb(replica)

				replica.safe_psql("SELECT * FROM o_test;")

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
					self.catchup_orioledb(replica)

	def test_replica_truncate_checkpointer_punch_holes(self):
		with self.node as master:
			self.node.append_conf("log_min_messages = notice")
			self.node.append_conf("orioledb.undo_buffers = 256MB")
			master.start()

			# create a backup
			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION IF NOT EXISTS orioledb;
				    CREATE TABLE IF NOT EXISTS o_test (
				    	key integer NOT NULL
				    ) USING orioledb;
				""")
				replica.append_conf("log_min_messages = notice")
				replica.append_conf("orioledb.enable_stopevents = true")
				replica.start()

				master.execute(
				    "INSERT INTO o_test (SELECT id FROM generate_series(1, 1000, 1) id);"
				)

				# wait for synchronization
				self.catchup_orioledb(replica)

				con1 = replica.connect()
				con2 = replica.connect()

				con2.execute(
				    "SELECT pg_stopevent_set('checkpoint_before_post_process', 'true');"
				)
				master.execute("CHECKPOINT")
				t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")

				t1.start()
				wait_checkpointer_stopevent(replica)

				master.execute("TRUNCATE o_test;")
				self.catchup_orioledb(replica)

				con2.execute(
				    "SELECT pg_stopevent_reset('checkpoint_before_post_process');"
				)
				t1.join()

				self.assertEqual(con2.execute("TABLE o_test"), [])

	def test_replication_simple_truncate(self):
		with self.node as master:
			master.start()
			test_data = (
			    ("CREATE TABLE o_test (id int) USING orioledb;",
			     "ctid as primary key"),
			    ("CREATE TABLE o_test (id int primary key) USING orioledb;",
			     "explicit primary key"),
			    ("CREATE TEMPORARY TABLE o_test (id int primary key) USING orioledb;",
			     "temporary"),
			)

			check_stmt = "SELECT COUNT(*) FROM o_test;"

			master.execute("CREATE EXTENSION orioledb;")

			# create a backup
			with self.getReplica().start() as replica:
				con = master.connect(autocommit=True)
				for test in test_data:
					con.execute("DROP TABLE IF EXISTS o_test;" + test[0])
					con.execute("INSERT INTO o_test VALUES(1);")

					# wait for synchronization
					self.catchup_orioledb(replica)
					replica.poll_query_until(
					    "SELECT orioledb_has_retained_undo();", expected=False)

					if test[1] != 'temporary':
						self.assertEqual(1, replica.execute(check_stmt)[0][0])

					con.execute("TRUNCATE o_test;")

					self.catchup_orioledb(replica)
					replica.poll_query_until(
					    "SELECT orioledb_has_retained_undo();", expected=False)

					self.assertEqual(0,
					                 con.execute(check_stmt)[0][0],
					                 "Empty on master: " + test[1])

					if test[1] != 'temporary':
						self.assertEqual(0,
						                 replica.execute(check_stmt)[0][0],
						                 "Empty on replica:" + test[1])

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
				self.catchup_orioledb(replica)

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
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e', 0);"
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

				match = False
				for i in range(0, 20):
					self.assertEqual(
					    n,
					    master.execute("SELECT count(*) FROM o_test;")[0][0])
					if i >= 1:
						if master.execute(
						    "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e', 0);"
						)[0][0].split('\n')[0] == INDEX_NOT_LOADED:
							match = True
							break
				self.assertTrue(match)

				# wait for synchronization
				self.catchup_orioledb(replica)
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

				self.assertEqual(
				    500,
				    replica.execute("SELECT count(*) FROM o_evicted;")[0][0])
				self.assertNotEqual(
				    replica.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e', 0);"
				    )[0][0].split('\n')[0], INDEX_NOT_LOADED)

				match = False
				for i in range(0, 20):
					self.assertEqual(
					    n,
					    replica.execute("SELECT count(*) FROM o_test;")[0][0])
					if i >= 1:
						if replica.execute(
						    "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e', 0);"
						)[0][0].split('\n')[0] == INDEX_NOT_LOADED:
							match = True
							break
				self.assertTrue(match)

				self.assertEqual(
				    500,
				    replica.execute("SELECT count(*) FROM o_evicted;")[0][0])

				self.assertNotEqual(
				    replica.execute(
				        "SELECT orioledb_tbl_structure('o_evicted'::regclass, 'e', 0);"
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
				self.catchup_orioledb(replica)
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

				self.catchup_orioledb(replica)
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

				self.catchup_orioledb(replica)

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

					self.catchup_orioledb(replica)

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
		node.append_conf('orioledb.recovery_idx_pool_size = 3')
		node.append_conf('log_min_messages = DEBUG1')
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

					with open(replica.pg_log_file) as f:
						replica_log = f.readlines()

						self.assertTrue(
						    any("DEBUG:  parallel index build uses 2 recovery workers"
						        in line for line in replica_log))
						self.assertTrue(
						    any("DEBUG:  worker joined parallel index build: table"
						        in line for line in replica_log))
						self.assertTrue(
						    any("DEBUG:  parallel index build: table" in line
						        for line in replica_log))

	def test_replication_serial_index_build(self):
		node = self.node
		node.append_conf('orioledb.recovery_pool_size = 1')
		node.append_conf('orioledb.recovery_idx_pool_size = 1')
		node.append_conf('log_min_messages = DEBUG1')
		node.start()
		with self.node as master:
			with self.getReplica() as replica:
				replica.start()

				with master.connect() as con1:
					con1.begin()

					con1.execute("""
						CREATE EXTENSION IF NOT EXISTS orioledb;

						CREATE TABLE o_test_1 (
							val_1 int
						) USING orioledb;

						CREATE UNIQUE INDEX ind_1
							ON o_test_1 (val_1);
					""")

					con1.commit()
					self.catchup_orioledb(replica)

					with open(replica.pg_log_file) as f:
						replica_log = f.readlines()

						self.assertTrue(
						    any("DEBUG:  serial index build: table" in line
						        for line in replica_log))
						self.assertFalse(
						    any("DEBUG:  parallel index build: table" in line
						        for line in replica_log))

	def test_replication_temp_table_data_cleanup(self):
		node = self.node
		node.append_conf('orioledb.recovery_pool_size = 1')
		node.append_conf('orioledb.recovery_idx_pool_size = 1')
		node.append_conf('autovacuum_naptime = 1s')
		node.append_conf('autovacuum_vacuum_threshold = 0')
		node.append_conf('autovacuum_vacuum_scale_factor = 0')

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

					master.poll_query_until(
					    "SELECT count(*) = 0 FROM pg_class WHERE relname LIKE 'o_test%%'"
					)

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
						CREATE TABLE o_test_5(val_1 serial)USING orioledb;
						CREATE TABLE o_test_6(val_1 serial, val_2 text)USING orioledb;

						DROP TABLE o_test_1, o_test_2, o_test_3, o_test_4, o_test_5, o_test_6;

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

	def test_tablespace_replication(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.execute("CREATE EXTENSION orioledb;")

				con1 = master.connect(autocommit=True)
				con1.execute("""
					SET allow_in_place_tablespaces = true;
				""")
				con1.execute("""
					CREATE TABLESPACE regress_tblspace LOCATION '';
				""")
				con1.close()

				master.safe_psql("""
					CREATE TABLE foo (
						i int
					) USING orioledb TABLESPACE regress_tblspace;
					INSERT INTO foo VALUES (3), (8), (94), (15);
				""")
				master.safe_psql("""
					CREATE INDEX foo_ix1 ON foo (i) TABLESPACE regress_tblspace;
				""")

				con2 = master.connect()
				con2.execute("SET enable_seqscan = off;")
				self.assertEqual([(3, ), (8, ), (15, ), (94, )],
				                 con2.execute("SELECT * FROM foo ORDER BY i;"))
				con2.close()

				# wait for synchronization
				self.catchup_orioledb(replica)
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

				con3 = replica.connect()
				con3.execute("SET enable_seqscan = off;")
				self.assertEqual([(3, ), (8, ), (15, ), (94, )],
				                 con3.execute("SELECT * FROM foo ORDER BY i;"))
				con3.close()

	def test_replication_hot_read(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica().start() as replica:
				master.execute("CREATE EXTENSION orioledb;")
				master.execute("CREATE TABLE o_test\n"
				               "    (id integer primary key NOT NULL)\n"
				               "USING orioledb;")
				master.execute("INSERT INTO o_test VALUES (1);")
				master.execute("""CREATE OR REPLACE FUNCTION test_repl()
									RETURNS VOID AS $$
									DECLARE
										max_val INTEGER;
										initial_max_val INTEGER;
									BEGIN
										SELECT * INTO initial_max_val FROM o_test ORDER BY id DESC LIMIT 1;
										LOOP
											SELECT * INTO max_val FROM o_test ORDER BY id DESC LIMIT 1;
											EXIT WHEN max_val > initial_max_val;
										END LOOP;
										RETURN;
									END;
									$$ LANGUAGE plpgsql;""")

				# wait for synchronization
				self.catchup_orioledb(replica)

				con_master = master.connect(autocommit=True)
				con_repl = replica.connect(autocommit=True)

				t_master = ThreadQueryExecutor(
				    con_master,
				    "INSERT INTO o_test SELECT x from generate_series(2, 5000000) x;",
				)
				t_repl = ThreadQueryExecutor(con_repl, "SELECT test_repl();")

				try:
					t_repl.start()
					t_master.start()

					t_repl.join()
					t_master.join()
				except Exception as e:
					print(f"Test not done: {e}")

				self.catchup_orioledb(replica)
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

				self.assertEqual(
				    master.execute("SELECT COUNT(*) FROM o_test;")[0][0],
				    5000000, "master check")

				self.assertEqual(
				    replica.execute("SELECT COUNT(*) FROM o_test;")[0][0],
				    5000000, "replica check")

	def test_replica_checkpoint_temp_files(self):
		with self.node as master:
			master.start()

			# create a backup
			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION IF NOT EXISTS orioledb;
					CREATE TABLE IF NOT EXISTS test (
						key integer NOT NULL
					);
				""")
				replica.append_conf("orioledb.enable_stopevents = true")
				replica.start()

				master.execute(
				    "INSERT INTO test (SELECT id FROM generate_series(1, 1000, 1) id);"
				)

				# wait for synchronization
				self.catchup_orioledb(replica)

				con1 = replica.connect()
				con2 = replica.connect()

				con2.execute(
				    "SELECT pg_stopevent_set('checkpoint_before_start', 'true', 'r');"
				)
				master.execute("CHECKPOINT")
				t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")

				t1.start()
				wait_checkpointer_stopevent(replica)

				master.stop()
				replica.stop(wait=False)

				try:
					t1.join()
				except Exception:
					pass

				con1.close()
				con2.close()

				while replica.status() == NodeStatus.Running:
					time.sleep(0.1)

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

	def test_replication_add_bridge_index(self):
		with self.node as master:
			master.append_conf('postgresql.conf',
			                   "orioledb.recovery_pool_size = 1\n")
			master.start()

			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;

					CREATE TABLE o_test (
						id int primary key,
						val text,
						p point
					) USING orioledb;

					INSERT INTO o_test
						SELECT id, 'val' || id, point(id, id)
						FROM generate_series(1, 100) id;
				""")
				self.catchup_orioledb(replica)
				self.assertEqual(
				    100,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])

				# Adding a GiST index triggers add_bridge_index()
				# and table rewrite via rebuild_indices()
				master.safe_psql("""
					CREATE INDEX o_test_gist ON o_test USING gist (p);
				""")
				self.catchup_orioledb(replica)

				# Verify all data is present on replica after rewrite
				self.assertEqual(
				    100,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])
				self.assertEqual(
				    'val1',
				    replica.execute("SELECT val FROM o_test WHERE id = 1;")[0]
				    [0])

				# Verify data accessible via bridge index on replica
				result = replica.execute(
				    "SET enable_indexonlyscan = off; "
				    "SELECT count(*) FROM o_test "
				    "WHERE p <@ '((0,0),(100,100))'::box;")
				self.assertEqual(100, result[0][0])

	def test_replication_rebuild_pk_after_checkpoint(self):
		"""
		Test that index rebuild on replica preserves all data when
		the replica restarts from a checkpoint that predates the
		rebuild.

		Scenario:
		1. Insert batch 1, CHECKPOINT on both master and replica
		2. Insert batch 2 (after checkpoint)
		3. ALTER TABLE DROP/ADD primary key (triggers rebuild_indices)
		4. Replica catches up, then crashes (immediate stop)
		5. Replica restarts from checkpoint (step 1), replays:
		   - batch 2 inserts (re-populates old tree)
		   - index rebuild (must see batch 1 + batch 2)
		6. Verify all data present and old tree files removed
		"""
		with self.node as master:
			master.append_conf('postgresql.conf',
			                   "orioledb.recovery_pool_size = 1\n")
			master.start()

			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;

					CREATE TABLE o_test (
						id int primary key,
						val text
					) USING orioledb;

					INSERT INTO o_test
						SELECT id, 'batch1_' || id
						FROM generate_series(1, 50) id;
				""")

				# Step 1: checkpoint on both sides
				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				replica.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				self.assertEqual(
				    50,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])

				# Remember old PK index relnode before rebuild
				old_pk_relnode = master.execute(
				    "SELECT relfilenode FROM pg_class "
				    "WHERE relname = 'o_test_pkey';")[0][0]
				old_datoid = master.execute(
				    "SELECT oid FROM pg_database "
				    "WHERE datname = current_database();")[0][0]

				# Verify old PK data file exists on master
				old_tree_path = os.path.join(master.data_dir, "orioledb_data",
				                             str(old_datoid),
				                             str(old_pk_relnode))
				self.assertTrue(
				    os.path.exists(old_tree_path),
				    f"Old PK data file {old_tree_path} should exist on master before rebuild"
				)

				# Step 2: insert batch 2 after checkpoint
				master.safe_psql("""
					INSERT INTO o_test
						SELECT id, 'batch2_' || id
						FROM generate_series(51, 100) id;
				""")

				# Step 3: rebuild primary key
				master.safe_psql("""
					ALTER TABLE o_test DROP CONSTRAINT o_test_pkey;
					ALTER TABLE o_test ADD PRIMARY KEY (id);
				""")

				new_pk_relnode = master.execute(
				    "SELECT relfilenode FROM pg_class "
				    "WHERE relname = 'o_test_pkey';")[0][0]
				self.assertNotEqual(old_pk_relnode, new_pk_relnode)

				# Step 4: replica catches up, then crash
				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				replica.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				self.assertEqual(
				    100,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])

				# Verify old PK data files are removed on master
				self.assertFalse(
				    os.path.exists(old_tree_path),
				    f"Old PK data file {old_tree_path} should not exist on master"
				)

				# Verify old PK data files are removed on replica
				old_tree_path_replica = os.path.join(replica.data_dir,
				                                     "orioledb_data",
				                                     str(old_datoid),
				                                     str(old_pk_relnode))
				self.assertFalse(
				    os.path.exists(old_tree_path_replica),
				    f"Old PK data file {old_tree_path_replica} should not exist on replica"
				)

				# Step 5: crash replica without checkpoint
				replica.stop(['-m', 'immediate'])
				replica.start()
				self.catchup_orioledb(replica)

				# Step 6: verify all data after recovery
				self.assertEqual(
				    100,
				    replica.execute("SELECT count(*) FROM o_test;")[0][0])
				self.assertEqual(
				    'batch1_1',
				    replica.execute("SELECT val FROM o_test WHERE id = 1;")[0]
				    [0])
				self.assertEqual(
				    'batch2_51',
				    replica.execute("SELECT val FROM o_test WHERE id = 51;")[0]
				    [0])

				# Verify old PK data files still removed after recovery
				self.assertFalse(
				    os.path.exists(old_tree_path_replica),
				    f"Old PK data file {old_tree_path_replica} should not exist on replica after recovery"
				)

	def test_replication_pk_column_order_differs_from_table_order(self):
		"""
		Regression test for B-tree corruption on replica when a table's PRIMARY
		KEY column order differs from the table's column definition order and
		all table columns are part of the primary key.

		Root cause: o_table_tupdesc() leaves tupdesc->tdtypeid = RECORDOID
		during WAL replay (no active transaction).  tts_orioledb_getsomeattrs()
		uses tdtypeid == RECORDOID as a signal that the tuple is stored in
		index-column order and applies a pk_tbl_field_map remap.  For a table
		where nfields == nNonLeafFields (all columns are key columns), this
		accidentally fires, scrambling the slot values and making the key-bound
		comparison use table-column order instead of key order.  Items are then
		routed to the wrong B-tree leaf page, causing corruption.

		The table below mirrors the TPC-C new_order table:
		  column order : (no_o_id, no_d_id, no_w_id)
		  primary key  : (no_w_id, no_d_id, no_o_id)
		We insert rows in key order (w, d, o) so that every district group
		starts o_id over from 1, crossing the split boundary created by the
		previous district.  With the bug each row from district >= 2 gets a
		wrong key bound (the scrambled slot puts o_id into the w_id slot), so
		comparisons route it to the wrong page.
		"""
		W = 1  # warehouses
		D = 3  # districts per warehouse
		O = 50  # orders per district

		with self.node as master:
			master.start()

			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;

					CREATE TABLE new_order (
						no_o_id  INTEGER NOT NULL,
						no_d_id  INTEGER NOT NULL,
						no_w_id  INTEGER NOT NULL,
						PRIMARY KEY (no_w_id, no_d_id, no_o_id)
					) USING orioledb;

					INSERT INTO new_order
						SELECT o, d, w
						FROM generate_series(1, {W}) w,
						     generate_series(1, {D}) d,
						     generate_series(1, {O}) o;
				""".format(W=W, D=D, O=O))

				# Checkpoint on both sides to flush B-tree pages to disk
				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				replica.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				# Restart replica to recover from checkpointed (possibly
				# corrupted) state, then verify data integrity
				replica.stop(['-m', 'immediate'])
				replica.start()
				self.catchup_orioledb(replica)

				# All rows must be present
				self.assertEqual(
				    W * D * O,
				    replica.execute("SELECT COUNT(*) FROM new_order;")[0][0])

				# Rows returned by an index scan (ORDER BY pk) must come out
				# in strict key order: (w, d, o) — not scrambled by the bug.
				rows = replica.execute("""
					SELECT no_w_id, no_d_id, no_o_id
					FROM new_order
					ORDER BY no_w_id, no_d_id, no_o_id;
				""")
				expected = [(w, d, o) for w in range(1, W + 1)
				            for d in range(1, D + 1) for o in range(1, O + 1)]
				self.assertEqual(expected, rows)

	def test_tablespace_pk_replication(self):
		with self.node as master:
			master.append_conf('postgresql.conf',
			                   "allow_in_place_tablespaces = true\n")
			master.start()

			master.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

			con1 = master.connect(autocommit=True)
			con1.execute("CREATE TABLESPACE regress_tblspace LOCATION '';")
			con1.close()

			# Table in default tablespace, no explicit PK.
			master.safe_psql("""
				CREATE TABLE foo (d int) USING orioledb;
				INSERT INTO foo SELECT g FROM generate_series(1, 100) g;
				CHECKPOINT;
			""")

			with self.getReplica().start() as replica:
				# Add a PK placed in a different tablespace.  On the replica the
				# WAL_REC_O_TABLES_META_UNLOCK record is replayed with
				# reachedConsistency=true, so handle_o_tables_meta_unlock() runs
				# the rebuild path.  tbl_data_exists() must check the *old*
				# table's tablespace (pg_default) – the bug passes the *new* PK's
				# tablespace (regress_tblspace) instead, so the old data is not
				# found and the rebuild is silently skipped, leaving the table
				# empty on the replica.
				master.safe_psql("ALTER TABLE foo ADD PRIMARY KEY (d) "
				                 "USING INDEX TABLESPACE regress_tblspace;")

				self.catchup_orioledb(replica)

				count = replica.execute("SELECT COUNT(*) FROM foo;")[0][0]
				self.assertEqual(100, count)
