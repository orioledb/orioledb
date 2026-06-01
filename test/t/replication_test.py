#!/usr/bin/env python3
# coding: utf-8

import os
import re
import signal
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
			    "max_parallel_workers_per_gather = 0\n"
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

	def test_recreate_o_table_version_replication(self):
		# Adding a bridged secondary index to a freshly-created orioledb
		# partition (BRIN here) drives o_define_index ->
		# add_bridge_index -> recreate_o_table.  recreate_o_table calls
		# o_tables_add(), which writes the sys-tree entry at version 0
		# but leaves the in-memory o_table->version unchanged.  The
		# subsequent o_tables_update() inside add_bridge_index then
		# computes table->version + 1 from that stale value, so the
		# sys-tree history of the new relnode goes 0 -> 2 with version 1
		# missing.  On the standby, handle_o_tables_meta_unlock()
		# replaying the trailing meta-unlock record (oldRelnode = 0)
		# does o_tables_get_extended(oids, new_o_table->version - 1) to
		# locate old_o_table -- with a hole in the version chain that
		# lookup returns NULL and the startup process aborts on
		# Assert(old_o_table).
		with self.node as master:
			master.start()

			with self.getReplica().start() as replica:
				master.safe_psql("CREATE EXTENSION orioledb;")
				master.safe_psql("""
					CREATE TABLE tab1 (
						c text,
						a int PRIMARY KEY,
						b text
					) PARTITION BY LIST (a);
					CREATE INDEX tab1_c_brin_idx ON tab1 USING brin (c);
					CREATE TABLE tab1_2_2 PARTITION OF tab1
						FOR VALUES IN (4, 6) USING orioledb;
				""")

				self.catchup_orioledb(replica)
				replica.poll_query_until(
				    "SELECT orioledb_has_retained_undo();", expected=False)

				self.assertEqual(NodeStatus.Running, replica.status())
				self.assertEqual(
				    0,
				    replica.execute("SELECT count(*) FROM tab1_2_2;")[0][0])

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

	def test_recovery_finish_aborts_propagate_to_replica(self):
		"""
		Issue #876: in-flight orioledb txns that the primary aborts in
		memory at end-of-crash-recovery must be advertised to streaming
		standbys via WAL_REC_ROLLBACK.  Otherwise the standby holds the
		oxid INPROGRESS forever, and any later replayed modify against a
		row touched by that txn spins forever in
		o_btree_modify_handle_conflicts.

		Repro recipe:
		  1. Long-running INSERT on the primary that overflows the 8 KB
		     local_wal buffer many times — its modify records stream to
		     the standby while the txn is still INPROGRESS.
		  2. Wait for the standby to apply the streamed records.
		  3. Force-crash the primary (-m immediate): the txn never
		     reaches COMMIT or ROLLBACK; its oxid stays INPROGRESS on
		     the standby.
		  4. Restart the primary.  recovery_finish() aborts the in-flight
		     oxid in memory; with the fix it also flushes a stand-alone
		     WAL_REC_ROLLBACK so the standby can resolve it.
		  5. UPDATE a row the aborted txn had inserted.  Streams to the
		     standby; without the fix the standby's recovery worker
		     livelocks; with the fix the conflict resolves immediately.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		# Track every postgres pid we ever see under the master so we can
		# reap stragglers in case the test fails mid-flight; otherwise a
		# pre-fix livelock leaves spinning recovery workers across runs.
		dangling_pids = set()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test_recovery_abort (
						id int NOT NULL,
						val text NOT NULL,
						PRIMARY KEY (id)
					) USING orioledb;
				""")
				self.catchup_orioledb(replica)

				# Open a long-running tx whose INSERTs overflow the
				# 8 KB local_wal buffer enough times to guarantee the
				# modify records reach the standby before commit.
				con = master.connect()
				con.begin()
				con.execute("""
					INSERT INTO o_test_recovery_abort (id, val)
					SELECT g, repeat('x', 200)
					FROM generate_series(1, 2000) g;
				""")

				# Force the in-flight records onto disk before the kill
				# (pg_switch_wal also wakes the WAL sender, so the standby
				# is guaranteed to receive them).  Without this, the records
				# can sit in the master's WAL buffer at SIGKILL time, so
				# crash recovery doesn't see the in-flight oxid -- there's
				# nothing for recovery_finish() to abort, and the bug never
				# fires.
				master.safe_psql("SELECT pg_switch_wal();")

				# Wait for the standby's redo to apply at least up to
				# the eagerly-flushed records (last LSN visible to the
				# master right now).
				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				replica.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= "
				    f"'{master_lsn}'::pg_lsn",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)

				# MVCC must not expose the in-flight rows yet.
				self.assertEqual(
				    0,
				    replica.execute(
				        "SELECT count(*) FROM o_test_recovery_abort;")[0][0])

				# But the standby's on-disk btree must contain them
				# (otherwise the bug repro has nothing to conflict
				# against).  Verify via orioledb_relation_size,
				# which reports physical bytes regardless of MVCC.
				# orioledb_tbl_structure was the natural fit here but
				# crashes on this kind of tree state in some builds —
				# the bug it trips is orthogonal to this test, see
				# the postmaster log on a debug build for
				# `invalid memory alloc request size`.
				rel_size = replica.execute(
				    "SELECT orioledb_relation_size("
				    "  'o_test_recovery_abort'::regclass);")[0][0]
				self.assertGreater(
				    rel_size, 0, "standby's on-disk btree should contain the "
				    "in-flight rows already, so the post-crash "
				    "conflict has something to resolve against")

				# Force-crash the master.  SIGKILL every postgres child
				# of the master postmaster + the postmaster itself.
				# pg_ctl stop -m immediate (SIGQUIT to backends) and
				# plain SIGKILL of postmaster alone both let backends
				# run AbortTransaction + wal_rollback before exiting,
				# emitting the WAL_REC_ROLLBACK whose absence the fix
				# repairs -- so the bug would never repro without
				# killing every backend instantly.
				pid_file = os.path.join(master.data_dir, 'postmaster.pid')
				with open(pid_file) as f:
					master_pid = int(f.readline().strip())
				child_pids = [
				    int(x) for x in subprocess.run(
				        ['pgrep', '-P', str(master_pid)],
				        capture_output=True,
				        text=True,
				        check=False).stdout.split()
				]
				dangling_pids.update([master_pid] + child_pids)
				for p in child_pids:
					try:
						os.kill(p, signal.SIGKILL)
					except ProcessLookupError:
						pass
				try:
					os.kill(master_pid, signal.SIGKILL)
				except ProcessLookupError:
					pass
				# Wait for the postmaster *and* every child to actually
				# exit -- otherwise a lingering child still owns the
				# listen socket and master.start() below fails with
				# "Address already in use".
				deadline = time.time() + 30
				while time.time() < deadline:
					alive = False
					for p in [master_pid] + child_pids:
						try:
							os.kill(p, 0)
							alive = True
							break
						except ProcessLookupError:
							continue
					if not alive:
						break
					time.sleep(0.05)
				master.is_started = False
				try:
					con.close()
				except Exception:
					pass
				master.start()

				# Run conflicting INSERTs on the recovered master.  Each
				# row streams to the standby where the recovery worker
				# must resolve the conflict against the dangling
				# INPROGRESS oxid.  Pre-fix the worker spins forever on
				# the first one; post-fix the standby has already seen
				# WAL_REC_ROLLBACK and resolves cleanly.
				master.safe_psql("""
					INSERT INTO o_test_recovery_abort (id, val)
					SELECT g, 'after-' || g
					FROM generate_series(1, 200) g;
				""")

				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				# 30 s ceiling: pre-fix this poll exhausts attempts and
				# the test fails; post-fix it catches up immediately.
				replica.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= "
				    f"'{master_lsn}'::pg_lsn",
				    expected=True,
				    sleep_time=0.5,
				    max_attempts=60)

				self.assertEqual(
				    'after-1',
				    replica.execute("SELECT val FROM o_test_recovery_abort "
				                    "WHERE id = 1;")[0][0])
				self.assertEqual(
				    200,
				    replica.execute(
				        "SELECT count(*) FROM o_test_recovery_abort;")[0][0])
		finally:
			try:
				master.stop()
			except Exception:
				pass
			# Reap any postgres process we ever saw under master.  If the
			# test hit the pre-fix livelock and aborted out of poll, the
			# orphaned recovery worker would otherwise keep spinning at
			# 100% CPU between test runs.
			for p in dangling_pids:
				try:
					os.kill(p, signal.SIGKILL)
				except ProcessLookupError:
					pass

	def test_recovery_finish_rollback_does_not_regress_replica_xmin(self):
		"""
		Issue #889: recovery_finish() rolls back in-flight oxids in
		memory and emits WAL_REC_ROLLBACK for each one in a deferred
		after-checkpoint hook.  Under the pre-fix horizon design the
		standby could end up livelocked: o_btree_modify_handle_conflicts
		would wait forever on an oxid it still saw as IN_PROGRESS,
		because the standby's runXmin / globalXmin had regressed below
		writtenXmin and oxid_get_csn() mis-read legitimately FROZEN
		xidmap slots as IN_PROGRESS.

		The bug surfaces as a livelocked recovery worker that cannot
		shut down -- which is what this test actually asserts.

		The horizon checks are intentionally loose.  A bounded lag
		between primary and replica xmins is legitimate bookkeeping
		bloat (globalXmin <= writtenXmin, deferred runXmin sync, etc.),
		not the bug.  The bounds catch a catastrophic pin (lag growing
		without bound as the primary keeps advancing) while tolerating
		ordinary post-recovery bloat.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		dangling_pids = set()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test_xmin_no_regress (
						k int PRIMARY KEY,
						v int
					) USING orioledb;
					INSERT INTO o_test_xmin_no_regress
						SELECT g, 0 FROM generate_series(1, 100) g;
				""")
				self.catchup_orioledb(replica)

				# Long-running tx whose single small UPDATE never
				# overflows the 8 KB local_wal buffer.  Without an
				# overflow, neither WAL_REC_XID(T_long) nor any
				# modify record reaches the standby pre-crash, so
				# T_long is invisible to the standby's hash.
				t_long = master.connect()
				t_long.begin()
				t_long.execute(
				    "UPDATE o_test_xmin_no_regress SET v = -1 WHERE k = 1;")

				# Bump nextXid past T_long by many short commits, so
				# post-restart runXmin = nextXid is *far* above
				# T_long's oxid.  This is what made the malformed
				# xmin observable in the bug report.
				for i in range(200):
					master.safe_psql(
					    "UPDATE o_test_xmin_no_regress "
					    f"SET v = v + 1 WHERE k = {2 + (i % 50)};")

				# Checkpoint primary: its xids file now records
				# T_long as in-flight.  Standby's xids file does
				# *not* (that file is local to the primary).
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				# SIGKILL the postmaster *and* every backend so
				# nobody gets a chance to wal_rollback() T_long.
				pid_file = os.path.join(master.data_dir, 'postmaster.pid')
				with open(pid_file) as f:
					master_pid = int(f.readline().strip())
				child_pids = [
				    int(x) for x in subprocess.run(
				        ['pgrep', '-P', str(master_pid)],
				        capture_output=True,
				        text=True,
				        check=False).stdout.split()
				]
				dangling_pids.update([master_pid] + child_pids)
				for p in child_pids + [master_pid]:
					try:
						os.kill(p, signal.SIGKILL)
					except ProcessLookupError:
						pass
				deadline = time.time() + 30
				while time.time() < deadline:
					alive = False
					for p in [master_pid] + child_pids:
						try:
							os.kill(p, 0)
							alive = True
							break
						except ProcessLookupError:
							continue
					if not alive:
						break
					time.sleep(0.05)
				master.is_started = False
				try:
					t_long.close()
				except Exception:
					pass
				master.start()

				# Trigger more WAL so the after-recovery
				# WAL_REC_ROLLBACK(T_long) is shipped, then catch up.
				master.safe_psql(
				    "UPDATE o_test_xmin_no_regress SET v = 99 WHERE k = 1;")
				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				replica.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= "
				    f"'{master_lsn}'::pg_lsn",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)
				replica.poll_query_until(
				    "SELECT orioledb_recovery_synchronized();",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)

				# Force one more checkpoint round on the primary so
				# anything still pending on the standby drains.
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)
				replica.poll_query_until(
				    "SELECT orioledb_recovery_synchronized();",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)

				master_runxmin, master_globalxmin = master.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]
				replica_runxmin, replica_globalxmin = replica.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]

				# Loose horizon bounds: pre-fix pin to T_long produced
				# lag = (nextXid - T_long.oxid), unbounded as the
				# primary keeps advancing.  Ordinary bloat from
				# writtenXmin bookkeeping and deferred runXmin sync
				# stays within a small constant.  The cap is generous
				# enough to absorb that bloat while still catching a
				# catastrophic pin.
				self.assertLess(
				    master_runxmin - replica_runxmin, 1000,
				    f"replica runXmin {replica_runxmin} catastrophically "
				    f"lags primary {master_runxmin}")
				self.assertLess(
				    master_globalxmin - replica_globalxmin, 1000,
				    f"replica globalXmin {replica_globalxmin} "
				    f"catastrophically lags primary {master_globalxmin}")

				# Replica must shut down cleanly.  The pre-fix
				# livelock would block fast shutdown indefinitely on
				# the recovery worker.  Generous timeout for valgrind
				# builds, which slow process teardown substantially.
				t0 = time.time()
				replica.stop(['-m', 'fast', '-t', '180'])
				self.assertLess(
				    time.time() - t0, 180,
				    "replica did not shut down within 180s "
				    "(potential livelock)")
		finally:
			try:
				master.stop()
			except Exception:
				pass
			for p in dangling_pids:
				try:
					os.kill(p, signal.SIGKILL)
				except ProcessLookupError:
					pass

	def test_replica_restart_seeds_runxmin_from_checkpoint_floor(self):
		"""
		read_xids() pushes the replica's runXmin to nextXid when its
		on-disk xids file is empty -- which it routinely is when the
		master's only in-flight oxid X has all its modify records
		buffered in the backend's private local_wal and none of them
		have reached the wire yet.  The master's xids file records X
		(its vxids[] still has the oxid), but the streaming standby
		has never seen a WAL_REC_XID for X, so its own restartpoint
		writes an xids file with no X in it.

		Pre-fix:
		  * Master checkpoint -> master's xids file has X.oxid
		    (LOW); master's checkpointRetainXmin = X.oxid.
		  * Replica restartpoint -> replica's xids file is empty.
		  * Restart the replica.  read_xids() finds an empty file,
		    update_run_xmin() falls into the empty-heap branch,
		    `Min(xmin, recovery_xmin = InvalidOXid)` -> runXmin =
		    nextXid (HIGH, master's nextXid at the checkpoint).
		  * A subsequent advance_oxids() in the apply path then
		    reads that HIGH runXmin and pushes globalXmin past the
		    real floor.
		  * When X eventually rolls back, the standby's xmin walk
		    drags globalXmin *backwards*.

		Post-fix:
		  * recovery_xmin is seeded with
		    xid_meta->checkpointRetainXmin (LOW) *before* read_xids()
		    so its first update_run_xmin() caps xmin at the floor.
		  * runXmin / globalXmin stay at the checkpoint-era floor
		    until a WAL commit/rollback explicitly bumps them.

		The user-visible failure that motivated this fix is still
		not pinned down (the upstream issue is "globalXmin moves
		backwards, exact corruption unclear"), so this test asserts
		the symptom directly: the replica's runXmin must not jump
		above master's runXmin while X is still in-flight.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test_xidfloor (
						k int PRIMARY KEY,
						v int
					) USING orioledb;
					INSERT INTO o_test_xidfloor
						SELECT g, 0 FROM generate_series(1, 100) g;
				""")
				self.catchup_orioledb(replica)

				# Long-running tx X.  A single small UPDATE
				# easily fits in the 8 KB local_wal buffer so the
				# modify record never reaches the wire (no
				# overflow flush, no commit yet).  This is the
				# crucial asymmetry: master's vxids[] holds X,
				# but the standby's recovery xid hash never
				# learns of it.
				t_long = master.connect()
				t_long.begin()
				t_long.execute(
				    "UPDATE o_test_xidfloor SET v = -1 WHERE k = 1;")
				t_long_oxid = t_long.execute(
				    "SELECT orioledb_get_current_oxid();")[0][0]

				# Drive nextXid well past X.oxid so the gap
				# between the checkpoint-era floor and nextXid is
				# observable (the pre-fix bug pushes runXmin all
				# the way up to nextXid).
				for i in range(200):
					master.safe_psql(
					    "UPDATE o_test_xidfloor "
					    f"SET v = v + 1 WHERE k = {2 + (i % 50)};")

				# Master CHECKPOINT.  finish_write_xids() scans
				# oProcData[].vxids[] and writes X.oxid to the
				# master's xids file.  master's
				# checkpointRetainXmin = X.oxid.
				master.safe_psql("CHECKPOINT;")

				# Wait for the standby to digest the checkpoint
				# WAL and run its own restartpoint.  Two
				# checkpoints + a synchronization barrier coax
				# the standby into rotating its xids file: only
				# *after* the second master checkpoint can the
				# replica know the first checkpoint is committed
				# and persist a fresh xids file for it.
				self.catchup_orioledb(replica)
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				# Master state we'll compare against.  X is still
				# in-flight, so master's runXmin is pinned at
				# (around) X.oxid.
				master_runxmin_before = master.execute(
				    "SELECT runxmin FROM orioledb_get_xid_meta();")[0][0]

				# Restart the standby.  Its restartpoint xids
				# file does not contain X (no WAL ever mentioned
				# it); pre-fix update_run_xmin() now sails
				# runXmin past X.oxid.
				replica.stop(['-m', 'fast', '-t', '60'])
				replica.start()
				replica.poll_query_until(
				    "SELECT orioledb_recovery_synchronized();",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)

				replica_runxmin, replica_globalxmin = replica.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]

				# The headline assertion.  Master's runXmin is
				# bounded above by X.oxid (X holds it down).
				# Replica must observe a horizon no higher than
				# master's: a runaway replica runXmin past
				# X.oxid is the pre-fix bug.  Bound is generous
				# enough to absorb a small bookkeeping bump on
				# either side but tight enough to catch the bug,
				# which blows the gap open by ~250 (the number
				# of post-X commits we ran).
				self.assertLessEqual(
				    replica_runxmin, master_runxmin_before + 50,
				    f"replica runXmin {replica_runxmin} jumped past "
				    f"master's runXmin {master_runxmin_before} after "
				    f"restart while X (oxid {t_long_oxid}) was still "
				    "in-flight")
				self.assertLessEqual(
				    replica_globalxmin, master_runxmin_before + 50,
				    f"replica globalXmin {replica_globalxmin} jumped "
				    f"past master's runXmin {master_runxmin_before} "
				    f"after restart while X (oxid {t_long_oxid}) was "
				    "still in-flight")

				# Finish X cleanly so resources unwind.  Not part
				# of the assertion -- just keeping the teardown
				# honest.
				t_long.rollback()
				t_long.close()
		finally:
			try:
				master.stop()
			except Exception:
				pass

	def test_meta_lock_autonomous_restore_strands_oxid(self):
		"""
		CREATE INDEX + ROLLBACK exercises o_tables_meta_lock (adds
		WAL_REC_XID without setting has_material_changes) followed
		by autonomous-tx sys-tree updates.  Checks the standby's
		runXmin >= globalXmin invariant holds.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_ml (
						id int PRIMARY KEY,
						val text NOT NULL
					) USING orioledb;
					INSERT INTO o_ml
						SELECT g, repeat('x', 30)
						FROM generate_series(1, 200) g;
				""")
				self.catchup_orioledb(replica)

				# CREATE INDEX + ROLLBACK in a fresh tx each time.
				for i in range(5):
					con = master.connect()
					con.begin()
					con.execute(f"CREATE INDEX o_ml_idx_{i} "
					            f"ON o_ml (val);")
					con.execute("ROLLBACK;")
					con.close()

				# Advance recovery_xmin on the standby.
				for i in range(300):
					master.safe_psql("UPDATE o_ml SET val = "
					                 f"'u{i}' WHERE id = {(i % 200) + 1};")
				master.safe_psql("CHECKPOINT;")
				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				try:
					replica.poll_query_until(
					    f"SELECT pg_last_wal_replay_lsn() >= "
					    f"'{master_lsn}'::pg_lsn",
					    expected=True,
					    sleep_time=0.1,
					    max_attempts=300)
				except Exception:
					pass

				self.assertEqual(
				    NodeStatus.Running, replica.status(),
				    "standby crashed: runXmin >= globalXmin invariant violated"
				)

				replica_runxmin, replica_globalxmin = replica.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]
				self.assertGreaterEqual(
				    replica_runxmin, replica_globalxmin,
				    f"replica runXmin {replica_runxmin} < "
				    f"globalXmin {replica_globalxmin}")
		finally:
			try:
				master.stop()
			except Exception:
				pass

	def test_bridge_erase_autonomous_restore_strands_oxid(self):
		"""
		VACUUM on a bridge-indexed table exercises
		add_bridge_erase_wal_record (adds WAL_REC_XID without
		setting has_material_changes) interleaved with
		autonomous-tx class_cache TOAST inserts on a wide table.
		Checks the standby's runXmin >= globalXmin invariant holds.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		try:
			with self.getReplica().start() as replica:
				# Wide table -> TOAST'd class_cache entry, so any
				# first access from a backend triggers autonomous.
				master.safe_psql("CREATE EXTENSION orioledb;")
				cols = ", ".join(f"c{i} text DEFAULT 'd'" for i in range(250))
				master.safe_psql(
				    f"CREATE TABLE o_br_wide (id int PRIMARY KEY, {cols}) "
				    "USING orioledb;")
				# Bridge index = a non-orioledb btree on an
				# orioledb table; vacuum drives add_bridge_erase.
				master.safe_psql("""
					CREATE TABLE o_br (
						id int PRIMARY KEY,
						val text NOT NULL
					) USING orioledb;
					INSERT INTO o_br
						SELECT g, repeat('x', 30)
						FROM generate_series(1, 1000) g;
					CREATE INDEX o_br_val_idx ON o_br USING btree (val) WITH (orioledb_index=off);
				""")
				self.catchup_orioledb(replica)

				# Generate dead tuples + interleave a wide-table
				# access that triggers the class_cache TOAST tx.
				for i in range(5):
					master.safe_psql(
					    "BEGIN; "
					    "DELETE FROM o_br "
					    f"WHERE id BETWEEN {i*50+1} AND {i*50+50}; "
					    f"INSERT INTO o_br_wide (id) VALUES ({i + 1}); "
					    "ROLLBACK;")

				master.safe_psql("VACUUM o_br;")
				for i in range(300):
					master.safe_psql("UPDATE o_br SET val = "
					                 f"'u{i}' WHERE id = {(i % 1000) + 501};")
				master.safe_psql("CHECKPOINT;")
				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				try:
					replica.poll_query_until(
					    f"SELECT pg_last_wal_replay_lsn() >= "
					    f"'{master_lsn}'::pg_lsn",
					    expected=True,
					    sleep_time=0.1,
					    max_attempts=300)
				except Exception:
					pass

				self.assertEqual(
				    NodeStatus.Running, replica.status(),
				    "standby crashed: runXmin >= globalXmin invariant violated"
				)

				replica_runxmin, replica_globalxmin = replica.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]
				self.assertGreaterEqual(
				    replica_runxmin, replica_globalxmin,
				    f"replica runXmin {replica_runxmin} < "
				    f"globalXmin {replica_globalxmin}")
		finally:
			try:
				master.stop()
			except Exception:
				pass

	def test_runxmin_never_below_globalxmin_under_stuck_oxids(self):
		"""
		Composite stress for the standby's runXmin >= globalXmin
		invariant: autonomous-TOAST inserts via wide tables, two
		overlapping long-running INSERTs (WAL_REC_XID on the standby
		arrives out of oxid order), SIGKILL master, restart,
		savepoints, CHECKPOINTs.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		dangling_pids = set()
		try:
			with self.getReplica().start() as replica:
				# Wide tables -> class_cache entries take the
				# TOAST autonomous-tx path on first access.
				master.safe_psql("CREATE EXTENSION orioledb;")
				cols = ", ".join(f"c{i} text DEFAULT 'd'" for i in range(250))
				master.safe_psql(
				    f"CREATE TABLE o_wide_a (id int PRIMARY KEY, {cols}) "
				    "USING orioledb;")
				master.safe_psql(
				    f"CREATE TABLE o_wide_b (id int PRIMARY KEY, {cols}) "
				    "USING orioledb;")
				master.safe_psql("""
					CREATE TABLE o_fp (
						id int PRIMARY KEY,
						val text NOT NULL
					) USING orioledb;
					INSERT INTO o_fp
						SELECT g, repeat('x', 50)
						FROM generate_series(1, 100) g;
				""")
				self.catchup_orioledb(replica)

				# Two concurrent backends: wide-table access ->
				# autonomous TOAST, savepoint, second wide-table
				# access -> second autonomous, overflow INSERT,
				# live ROLLBACK.
				t_low = master.connect()
				t_high = master.connect()
				t_low.begin()
				t_high.begin()
				t_high.execute("INSERT INTO o_wide_a (id) VALUES (1);")
				t_high.execute("SAVEPOINT sp_h;")
				t_high.execute("INSERT INTO o_wide_b (id) VALUES (1);")
				t_low.execute("INSERT INTO o_wide_a (id) VALUES (2);")
				t_low.execute("SAVEPOINT sp_l;")
				t_low.execute("INSERT INTO o_wide_b (id) VALUES (2);")
				t_high.execute("""
					INSERT INTO o_fp (id, val)
					SELECT g, repeat('h', 200)
					FROM generate_series(101, 1500) g;
				""")
				t_low.execute("""
					INSERT INTO o_fp (id, val)
					SELECT g, repeat('l', 200)
					FROM generate_series(1501, 3000) g;
				""")
				t_high.execute("ROLLBACK;")
				t_low.execute("ROLLBACK;")
				try:
					t_high.close()
				except Exception:
					pass
				try:
					t_low.close()
				except Exception:
					pass

				# Post-rollback commits to advance recovery_xmin.
				for i in range(200):
					master.safe_psql("BEGIN; "
					                 f"UPDATE o_fp SET val = 'u{i}' "
					                 f"WHERE id = {(i % 100) + 1}; "
					                 "SAVEPOINT sp; "
					                 f"UPDATE o_fp SET val = 's{i}' "
					                 f"WHERE id = {(i % 100) + 1}; "
					                 "ROLLBACK TO SAVEPOINT sp; "
					                 "COMMIT;")
				master.safe_psql("CHECKPOINT;")
				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				try:
					replica.poll_query_until(
					    f"SELECT pg_last_wal_replay_lsn() >= "
					    f"'{master_lsn}'::pg_lsn",
					    expected=True,
					    sleep_time=0.1,
					    max_attempts=300)
				except Exception:
					pass

				self.assertEqual(
				    NodeStatus.Running, replica.status(),
				    "standby crashed: runXmin >= globalXmin invariant violated"
				)

				# Invariant check (also catches non-cassert builds).
				replica_runxmin, replica_globalxmin = replica.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]
				self.assertGreaterEqual(
				    replica_runxmin, replica_globalxmin,
				    f"replica runXmin {replica_runxmin} < "
				    f"globalXmin {replica_globalxmin}")
		finally:
			try:
				master.stop()
			except Exception:
				pass
			for p in dangling_pids:
				try:
					os.kill(p, signal.SIGKILL)
				except ProcessLookupError:
					pass

	def test_parallel_worker_rollback_undo_lost_to_compaction(self):
		"""
		Parallel-worker undo-vs-compaction race introduced by the
		runXmin rework.

		T_stop deletes k=1 on o_compact.  The recovery worker
		handling T_stop's btree (W_K1) applies the DELETE and then
		hits a before_apply_undo stopevent at the start of its
		rollback undo -- so the tombstone-deletion record is still
		visible on the page while undo is suspended.

		While W_K1 is parked, the main recovery process keeps
		processing WAL.  Post-rollback transactions on the master
		emit WAL_REC_COMMIT records carrying xmin > T_stop.oxid,
		because T_stop has already finished there.  The rework's
		update_run_xmin tracks recovery_xmin directly, so the
		standby's runXmin slides past T_stop.oxid.

		Concurrent recovery workers (W_B etc.) applying modifies on
		the same densely packed page touch k=1's tombstone; with the
		advanced runXmin xid_is_finished_for_everybody(T_stop.oxid)
		returns true, and the tombstone is dropped during
		perform_page_compaction.  When the stopevent is released and
		W_K1 finally runs its undo, there is nothing left on the page
		to undo -- the row is gone for good.

		Pre-rework the standby's xmin_queue held runXmin floored at
		T_stop.oxid until W_K1 finished, so compaction never fired.
		"""
		master = self.node
		master.append_conf("orioledb.enable_stopevents = true\n"
		                   "orioledb.recovery_pool_size = 2\n")
		master.start()
		replica = None
		try:
			replica = self.getReplica()
			replica.append_conf("orioledb.enable_stopevents = true\n"
			                    "orioledb.recovery_pool_size = 2\n")
			replica.start()

			# Pack many narrow rows into the same page so the
			# leftmost leaf holds k=1 along with k=2..N.  Use a
			# wide text column whose post-rollback UPDATEs *grow*
			# the tuples; this is what makes the worker's
			# perform_page_compaction path fire on the page that
			# holds k=1's tombstone.
			master.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE o_compact (
					k int PRIMARY KEY,
					v text
				) USING orioledb;
				INSERT INTO o_compact
					SELECT g, 'short' FROM generate_series(1, 100) g;
			""")
			master.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)

			# Park whichever recovery worker runs the abort-path
			# undo for T_stop.  Main runs apply_undo_stack too as
			# part of recovery_finish_current_oxid, but its
			# $backendType is "startup" and is excluded by the
			# filter -- only worker-side abort undo (which is what
			# actually applies the page-level tombstone revert) is
			# captured.
			replica.safe_psql(
			    "SELECT pg_stopevent_set('before_apply_undo', "
			    "'$.commit == false "
			    "&& $backendType == \"orioledb recovery worker\"');")

			# T_stop: delete k=1 and roll back.
			t_stop = master.connect()
			t_stop.begin()
			t_stop.execute("DELETE FROM o_compact WHERE k = 1;")
			t_stop.execute("ROLLBACK;")
			t_stop.close()

			# Post-rollback commits.  Master's runXmin has advanced
			# past T_stop.oxid, so each WAL_REC_COMMIT carries
			# xmin > T_stop.oxid.  On the standby the rework's
			# update_run_xmin slides runXmin past T_stop.oxid while
			# the worker holding T_stop's undo is still parked.
			# Growing each row's `v` forces the leftmost page (the
			# one that holds k=1's tombstone) into the
			# compaction-required regime: the next UPDATE on that
			# page calls perform_page_compaction, which now sees
			# xid_is_finished_for_everybody(T_stop.oxid) returning
			# true (T_stop.oxid < advanced runXmin) and drops the
			# tombstone for k=1.
			big = "x" * 500
			for i in range(2, 90):
				master.safe_psql("UPDATE o_compact "
				                 f"SET v = '{big}' WHERE k = {i};")

			# Let the standby's main process digest those commits.
			# This does NOT wait for the parked worker.
			time.sleep(1.0)

			# Release the stopevent.  W_K1 wakes up and runs undo;
			# pre-rework the tombstone is still on the page and gets
			# reverted; post-rework the page has been compacted and
			# k=1 cannot be restored.
			replica.safe_psql(
			    "SELECT pg_stopevent_reset('before_apply_undo');")

			master_lsn = master.execute("SELECT pg_current_wal_lsn();")[0][0]
			replica.poll_query_until(
			    f"SELECT pg_last_wal_replay_lsn() >= "
			    f"'{master_lsn}'::pg_lsn",
			    expected=True,
			    sleep_time=0.1,
			    max_attempts=300)
			replica.poll_query_until(
			    "SELECT orioledb_recovery_synchronized();",
			    expected=True,
			    sleep_time=0.1,
			    max_attempts=300)

			# T_stop's DELETE was rolled back: k=1 must still be
			# present on the replica.  Pre-rework: passes.
			# Post-rework: fails -- row was compacted away.
			count = replica.execute(
			    "SELECT count(*) FROM o_compact WHERE k = 1;")[0][0]
			self.assertEqual(
			    1, count, "T_stop's DELETE was rolled back on master, but "
			    "k=1 is missing on the replica -- parallel-worker "
			    "undo was lost to page compaction (replica's "
			    "runXmin advanced past T_stop.oxid while the undo "
			    "worker was parked)")
		finally:
			if replica is not None:
				try:
					replica.safe_psql(
					    "SELECT pg_stopevent_reset('before_apply_undo');")
				except Exception:
					pass
				try:
					replica.stop(['-m', 'immediate', '-t', '30'])
				except Exception:
					pass
			try:
				master.stop()
			except Exception:
				pass

	def test_crash_recovery_drops_fastpath_aborted_xids_silently(self):
		"""
		Master crashes with checkpoint-only fast-path-aborted oxids in
		its xids file, after recovery_xmin has already streamed past
		them.  Without the update_run_xmin() drainer the master's
		crash recovery would consider them in-flight and ship a
		spurious WAL_REC_XID + WAL_REC_ROLLBACK pair for each one --
		landing oxids below the standby's already-advanced
		globalXmin into xmin_queue and tripping
		Assert(xmin >= globalXmin) on the next update_run_xmin().

		Setup:
		  1. Open N concurrent backends; each one calls
		     orioledb_get_current_oxid() so it sits in vxids[] with a
		     low oxid and no material WAL.
		  2. CHECKPOINT the master -- its xids file now records all
		     N oxids.
		  3. Close every backend without COMMIT/ROLLBACK on the wire:
		     PostgreSQL aborts them locally; orioledb's wal_rollback()
		     fast-path returns without writing a single byte to WAL.
		  4. Run many short COMMITs to push recovery_xmin (the xmin
		     stamped on every WAL_REC_COMMIT) well above the N oxids.
		     No second CHECKPOINT, so the xids file still lists the
		     fast-path-aborted oxids.
		  5. SIGKILL master and every backend.
		  6. Restart master.

		Master crash recovery now reads its old xids file, sees N
		entries below recovery_xmin (driven by step 4's WAL), and
		the drainer must (a) drop them off xmin_queue, (b) mark
		state->csn = COMMITSEQNO_ABORTED, and (c) hash_search-REMOVE
		so recovery_finish() never re-considers them.  If any one of
		those is skipped, the master emits a stand-alone
		WAL_REC_ROLLBACK for the oxid; the standby is then expected
		either to crash on the Assert or, in non-cassert builds, to
		end up with replica.runXmin < replica.globalXmin.
		"""
		master = self.node
		master.append_conf("wal_keep_size = '128MB'\n")
		master.start()
		dangling_pids = set()
		try:
			with self.getReplica().start() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE o_test_fp_abort (
						k int PRIMARY KEY,
						v int
					) USING orioledb;
					INSERT INTO o_test_fp_abort
						SELECT g, 0 FROM generate_series(1, 50) g;
				""")
				self.catchup_orioledb(replica)

				# Open N backends that each acquire an oxid without
				# any material WAL -- orioledb_get_current_oxid() is
				# exactly the SQL handle for get_current_oxid().
				N = 8
				stuck = []
				for _ in range(N):
					con = master.connect()
					con.begin()
					con.execute("SELECT orioledb_get_current_oxid();")
					stuck.append(con)

				# Checkpoint records every open oxid in the xids file.
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				# Close every stuck backend.  No COMMIT or ROLLBACK on
				# the wire -- PG's AbortTransaction runs locally;
				# orioledb's wal_rollback() takes the
				# !has_material_changes fast path and returns without
				# touching WAL.  The oxids stay in the master's xids
				# file (no second CHECKPOINT) but their backends are
				# gone, so master.runXmin can advance past them.
				for con in stuck:
					try:
						con.close()
					except Exception:
						pass

				# Push recovery_xmin (the xmin stamped on every
				# WAL_REC_COMMIT) well above the N stuck oxids, then
				# wait for the standby to apply that horizon.
				for i in range(300):
					master.safe_psql(f"UPDATE o_test_fp_abort SET v = v + 1 "
					                 f"WHERE k = {(i % 50) + 1};")

				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				replica.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= "
				    f"'{master_lsn}'::pg_lsn",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)

				# Standby globalXmin is now well above any fast-path-
				# aborted oxid.  Record it; we want to be sure it does
				# not regress after the master restart.
				replica_globalxmin_pre = replica.execute(
				    "SELECT globalxmin "
				    "FROM orioledb_get_xid_meta();")[0][0]

				# SIGKILL master + all backends.  No clean shutdown
				# means no second CHECKPOINT; the xids file still
				# records the N fast-path-aborted oxids as in-flight.
				pid_file = os.path.join(master.data_dir, 'postmaster.pid')
				with open(pid_file) as f:
					master_pid = int(f.readline().strip())
				child_pids = [
				    int(x) for x in subprocess.run(
				        ['pgrep', '-P', str(master_pid)],
				        capture_output=True,
				        text=True,
				        check=False).stdout.split()
				]
				dangling_pids.update([master_pid] + child_pids)
				for p in child_pids + [master_pid]:
					try:
						os.kill(p, signal.SIGKILL)
					except ProcessLookupError:
						pass
				deadline = time.time() + 30
				while time.time() < deadline:
					alive = False
					for p in [master_pid] + child_pids:
						try:
							os.kill(p, 0)
							alive = True
							break
						except ProcessLookupError:
							continue
					if not alive:
						break
					time.sleep(0.05)
				master.is_started = False
				master.start()

				# Drive enough WAL through the recovered master so any
				# deferred WAL_REC_ROLLBACK from
				# o_after_checkpoint_cleanup_hook() would have been
				# shipped, then wait for the standby to catch up.
				master.safe_psql(
				    "UPDATE o_test_fp_abort SET v = 99 WHERE k = 1;")
				master.safe_psql("CHECKPOINT;")
				master_lsn = master.execute(
				    "SELECT pg_current_wal_lsn();")[0][0]
				replica.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= "
				    f"'{master_lsn}'::pg_lsn",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)
				replica.poll_query_until(
				    "SELECT orioledb_recovery_synchronized();",
				    expected=True,
				    sleep_time=0.1,
				    max_attempts=300)

				# Without the fix, replaying the spurious
				# WAL_REC_ROLLBACK records on the standby trips
				# Assert(xmin >= globalXmin) in update_run_xmin(),
				# and the standby crashes.
				self.assertEqual(
				    NodeStatus.Running, replica.status(),
				    "standby crashed: drainer left fast-path-aborted "
				    "state stranded and a spurious WAL_REC_ROLLBACK "
				    "violated runXmin >= globalXmin")

				replica_runxmin, replica_globalxmin = replica.execute(
				    "SELECT runxmin, globalxmin "
				    "FROM orioledb_get_xid_meta();")[0]
				self.assertGreaterEqual(
				    replica_runxmin, replica_globalxmin,
				    f"replica runXmin {replica_runxmin} < "
				    f"globalXmin {replica_globalxmin}")
				self.assertGreaterEqual(
				    replica_globalxmin, replica_globalxmin_pre,
				    f"replica globalXmin regressed from "
				    f"{replica_globalxmin_pre} to "
				    f"{replica_globalxmin} -- the master's spurious "
				    f"WAL_REC_ROLLBACK pulled the horizon backwards")
		finally:
			try:
				master.stop()
			except Exception:
				pass
			for p in dangling_pids:
				try:
					os.kill(p, signal.SIGKILL)
				except ProcessLookupError:
					pass
