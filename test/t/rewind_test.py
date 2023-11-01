import copy
from itertools import chain
from collections import Counter
import os
from shutil import rmtree
import subprocess
from tempfile import mkdtemp
from typing import List
from unittest import result
import unittest
import testgres
from testgres.defaults import default_dbname
from testgres.utils import file_tail, get_bin_path, options_string
from testgres.consts import PG_AUTO_CONF_FILE, TMP_DUMP
from testgres.enums import NodeStatus

from .base_test import BaseTest, generate_string

class RewindTest(BaseTest):
	def pg_rewind(self, target, source_port, verbose=False,
				  rewind_log_file=None):
		pg_rewind_params = [
			get_bin_path("pg_rewind"),
			# TODO: change to orioledb or orioledb.so
			"--extensions", "pg_rewind_orioledb.so",
			"--target-pgdata", target,
			"--source-server", f"port={source_port} dbname={default_dbname()}"
		]  # yapf: disable

		if verbose == True:
			pg_rewind_params.extend(["--progress", "--debug"])

		if rewind_log_file != None:
			os.environ["ORIOLEDB_REWIND_LOG"] = str(rewind_log_file)

		# start psql process
		process = subprocess.Popen(
			pg_rewind_params,
			stdin=subprocess.PIPE,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE)

		# wait until it finishes and get stdout and stderr
		out, err = process.communicate()
		if (verbose == True) or (process.returncode != 0):
			print(out.decode("utf-8"))
			print(err.decode("utf-8"))
		self.assertEqual(process.returncode, 0)
		return process.returncode, out, err

	def pg_rewind_master(self, master: testgres.PostgresNode,
						 new_master_port: int,
						 master_slot: str, archive_dir: str,
						 verbose=False,
						 rewind_log_file=None):
		if master.status() == NodeStatus.Running:
			master.stop()
		master_cleanup_command = f'pg_archivecleanup {archive_dir} %r'
		master.append_conf(recovery_target_timeline = 'latest',
			archive_cleanup_command = master_cleanup_command,
			restore_command = f'cp {archive_dir}/%f %p',
			primary_slot_name = master_slot)
		if rewind_log_file == None:
			rewind_log_file = master.pg_log_file
		self.pg_rewind(master.data_dir, new_master_port, verbose,
					   rewind_log_file)

	def start_master_as_standby(self, master: testgres.PostgresNode,
								replica: testgres.PostgresNode,
								primary_conninfo: dict,
								master_slot: str,
								start_params=[]):
		master._assign_master(replica)
		master.append_conf(port=master.port)
		master.append_conf(primary_conninfo =
								options_string(**primary_conninfo),
							primary_slot_name=master_slot,
							filename=PG_AUTO_CONF_FILE)
		signal_name = os.path.join(master.data_dir, "standby.signal")
		with open(signal_name, 'a'):
			os.utime(signal_name, None)
		master.start(start_params)

	def test_rewind_remove_rows(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							   f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							   archive_command = archive_command,
							   wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer,
						y integer DEFAULT 1
					) USING orioledb;
					CREATE INDEX test_table_ix1 ON test_table (y);
					INSERT INTO test_table (x)
						SELECT id FROM generate_series(1, 100) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				replica.append_conf(primary_slot_name = 'replica')
				replica.start()
				master.safe_psql("""INSERT INTO test_table (x, y)
										SELECT id, 2 FROM
											generate_series(1, 100) id;""")
				master.safe_psql("CHECKPOINT")
				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					200)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				master.safe_psql("""INSERT INTO test_table (x, y)
										SELECT id, 3 FROM
											generate_series(1, 100) id;""")
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					300)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					200)

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					200)
			rmtree(archive_dir, ignore_errors=True)

	def test_rewind_apply_new_rows(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							   f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							   archive_command = archive_command,
							   wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					CREATE INDEX test_table_ix1 ON test_table (y);
					INSERT INTO test_table (x, y)
						SELECT id, 1 FROM generate_series(1, 2) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()
				master.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 2 FROM generate_series(3, 4) id""")
				master.safe_psql("CHECKPOINT")
				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					4)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				master.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 3 FROM generate_series(5, 8) id""")
				master.safe_psql("""INSERT INTO test_table (x, y)
					VALUES (10, 3)""")
				master.safe_psql("""UPDATE test_table SET y = 5 WHERE x < 4""")
				master.safe_psql("""DELETE FROM test_table WHERE x < 2""")
				replica.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 4 FROM generate_series(5, 6) id""")
				replica.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 4 FROM generate_series(11, 12) id""")
				replica.safe_psql("""UPDATE test_table SET y = 7 WHERE x = 5""")
				replica.safe_psql("""DELETE FROM test_table WHERE x = 2""")
				self.assertEqual(
					master.execute("SELECT * FROM test_table ORDER BY x"),
					[(2, 5), (3, 5), (4, 2), (5, 3), (6, 3),
					 (7, 3), (8, 3), (10, 3)])
				self.assertEqual(
					replica.execute("SELECT * FROM test_table ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4), (11, 4), (12, 4)])

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				self.assertEqual(
					master.execute("SELECT * FROM test_table ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4), (11, 4), (12, 4)])
				replica.safe_psql("""UPDATE test_table
									 SET y = 6 WHERE x = 1""")
				self.catchup_orioledb(master)
				self.assertEqual(
					master.execute("SELECT * FROM test_table ORDER BY x"),
					[(1, 6), (3, 2), (4, 2), (5, 7), (6, 4), (11, 4), (12, 4)])
			rmtree(archive_dir, ignore_errors=True)

	def test_rewind_multiple_relations(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							   f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							   archive_command = archive_command,
							   wal_log_hints = "on")
			rewind_file = os.path.join(master.data_dir, "orioledb_data",
									   "rewind")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test1 (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					CREATE TABLE test2 (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					CREATE TABLE test3 (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					CREATE INDEX test1_ix1 ON test1 (y);
					CREATE INDEX test2_ix1 ON test2 (y);
					CREATE INDEX test3_ix1 ON test3 (y);
					INSERT INTO test1 (x, y)
						SELECT id, 1 FROM generate_series(1, 2) id;
					INSERT INTO test2 (x, y)
						SELECT id, 1 FROM generate_series(1, 2) id;
					INSERT INTO test3 (x, y)
						SELECT id, 1 FROM generate_series(1, 2) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()

				master.safe_psql("""
					INSERT INTO test1 (x, y)
						SELECT id, 2 FROM generate_series(3, 4) id;
					INSERT INTO test2 (x, y)
						SELECT id, 2 FROM generate_series(3, 4) id;
					INSERT INTO test3 (x, y)
						SELECT id, 2 FROM generate_series(3, 4) id;
				""")
				master.safe_psql("CHECKPOINT")

				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT * FROM test1"),
					[(1, 1), (2, 1), (3, 2), (4, 2)])
				self.assertEqual(
					replica.execute("SELECT * FROM test2"),
					[(1, 1), (2, 1), (3, 2), (4, 2)])
				self.assertEqual(
					replica.execute("SELECT * FROM test3"),
					[(1, 1), (2, 1), (3, 2), (4, 2)])
				replica.promote()
				replica.safe_psql("CHECKPOINT;")

				master.safe_psql("""
					INSERT INTO test1 (x, y)
						SELECT id, 3 FROM generate_series(5, 8) id;
					INSERT INTO test2 (x, y)
						SELECT id, 3 FROM generate_series(5, 8) id;
					INSERT INTO test3 (x, y)
						SELECT id, 3 FROM generate_series(5, 8) id;
				""")

				replica.safe_psql("""
				INSERT INTO test1 (x, y)
					SELECT id, 4 FROM generate_series(5, 6) id;
				INSERT INTO test2 (x, y)
					SELECT id, 4 FROM generate_series(5, 6) id;
				INSERT INTO test3 (x, y)
					SELECT id, 4 FROM generate_series(5, 6) id;""")

				replica.safe_psql("""
					UPDATE test1 SET y = 7 WHERE x = 5;
					UPDATE test2 SET y = 7 WHERE x = 5;
					UPDATE test3 SET y = 7 WHERE x = 5;
				""")
				replica.safe_psql("""
					DELETE FROM test1 WHERE x = 2;
					DELETE FROM test2 WHERE x = 2;
					DELETE FROM test3 WHERE x = 2;
				""")
				self.assertEqual(
					replica.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					replica.execute("SELECT * FROM test2 ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					replica.execute("SELECT * FROM test3 ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4)])

				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 1), (2, 1), (3, 2), (4, 2),
					 (5, 3), (6, 3), (7, 3), (8, 3)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY x"),
					[(1, 1), (2, 1), (3, 2), (4, 2),
					 (5, 3), (6, 3), (7, 3), (8, 3)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY x"),
					[(1, 1), (2, 1), (3, 2), (4, 2),
					 (5, 3), (6, 3), (7, 3), (8, 3)])

				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY y"),
					[(1, 1), (2, 1), (3, 2), (4, 2),
					 (5, 3), (6, 3), (7, 3), (8, 3)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY y"),
					[(1, 1), (2, 1), (3, 2), (4, 2),
					 (5, 3), (6, 3), (7, 3), (8, 3)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY y"),
					[(1, 1), (2, 1), (3, 2), (4, 2),
					 (5, 3), (6, 3), (7, 3), (8, 3)])

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}

				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.assertTrue(os.path.isfile(rewind_file))
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				self.catchup_orioledb(master)
				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY x"),
					[(1, 1), (3, 2), (4, 2), (5, 7), (6, 4)])

				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY y"),
					[(1, 1), (3, 2), (4, 2), (6, 4), (5, 7)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY y"),
					[(1, 1), (3, 2), (4, 2), (6, 4), (5, 7)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY y"),
					[(1, 1), (3, 2), (4, 2), (6, 4), (5, 7)])

				replica.safe_psql("""
					UPDATE test1 SET y = 6 WHERE x = 1;
					UPDATE test2 SET y = 6 WHERE x = 1;
					UPDATE test3 SET y = 6 WHERE x = 1;
				""")

				self.catchup_orioledb(master)
				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 6), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY x"),
					[(1, 6), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY x"),
					[(1, 6), (3, 2), (4, 2), (5, 7), (6, 4)])

				replica.safe_psql("""
					UPDATE test1 SET y = 7 WHERE x = 1;
					UPDATE test2 SET y = 7 WHERE x = 1;
					UPDATE test3 SET y = 7 WHERE x = 1;
				""")

				self.catchup_orioledb(master)
				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4)])
				master.stop(["-m", 'immediate'])
				self.assertTrue(os.path.isfile(rewind_file))
				# check that rewind running again if no checkpoint
				master.start()

				self.catchup_orioledb(master)
				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4)])

				replica.safe_psql("""
					INSERT INTO test1 VALUES (8, 8);
					INSERT INTO test2 VALUES (8, 8);
					INSERT INTO test3 VALUES (8, 8);
				""")
				self.catchup_orioledb(master)
				master.stop()
				self.assertFalse(os.path.isfile(rewind_file))
				master.start()
				self.assertEqual(
					master.execute("SELECT * FROM test1 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4), (8, 8)])
				self.assertEqual(
					master.execute("SELECT * FROM test2 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4), (8, 8)])
				self.assertEqual(
					master.execute("SELECT * FROM test3 ORDER BY x"),
					[(1, 7), (3, 2), (4, 2), (5, 7), (6, 4), (8, 8)])
			rmtree(archive_dir, ignore_errors=True)

	def test_rewind_in_progress_target_trx(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							archive_command = archive_command,
							wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					INSERT INTO test_table (x, y)
						SELECT id, 1 FROM generate_series(1, 5) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				master.safe_psql("CHECKPOINT;")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()
				con1 = master.connect()
				con2 = master.connect()
				con3 = master.connect()

				con2.begin()
				con2.execute("""INSERT INTO test_table (x, y)
					SELECT id, 10 FROM generate_series(4001, 4010) id""")

				con1.begin()
				con1.execute("""INSERT INTO test_table (x, y)
					SELECT id, 2 FROM generate_series(6, 1000) id""")
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				con1.execute("""INSERT INTO test_table (x, y)
					SELECT id, 3 FROM generate_series(1001, 2000) id""")
				con1.execute("SELECT orioledb_flush_local_wal()")
				master.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 4 FROM generate_series(2001, 3000) id""")
				con3.execute("""INSERT INTO test_table (x, y)
					SELECT id, 5 FROM generate_series(3001, 3500) id""")
				con1.commit()
				con3.rollback()
				con2.commit()
				con1.close()
				con2.close()
				con3.close()
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					3010)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
			rmtree(archive_dir, ignore_errors=True)

	def test_rewind_in_progress_target_trx_undo(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							archive_command = archive_command,
							wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					INSERT INTO test_table (x, y)
						SELECT id, 1 FROM generate_series(1, 5) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				master.safe_psql("CHECKPOINT;")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()
				con1 = master.connect()
				con2 = master.connect()
				con3 = master.connect()

				con2.begin()
				con2.execute("""INSERT INTO test_table (x, y)
					SELECT id, 10 FROM generate_series(4001, 4010) id""")
				con2.execute("""SAVEPOINT s1;""")
				con2.execute("""UPDATE test_table SET y = 10 WHERE x = 5""")
				con2.execute("""SAVEPOINT s2;""")
				con2.execute("""UPDATE test_table SET y = 11 WHERE x = 5""")
				con2.execute("""SAVEPOINT s3;""")
				con2.execute("""UPDATE test_table SET y = 12 WHERE x = 5""")
				con2.execute("""SAVEPOINT s4;""")
				con2.execute("""UPDATE test_table SET y = 13 WHERE x = 5""")
				con2.execute("""SAVEPOINT s5;""")
				con2.execute("""UPDATE test_table SET y = 14 WHERE x = 5""")

				con1.begin()
				con1.execute("""INSERT INTO test_table (x, y)
					SELECT id, 2 FROM generate_series(6, 1000) id""")
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				con1.execute("""INSERT INTO test_table (x, y)
					SELECT id, 3 FROM generate_series(1001, 2000) id""")
				con1.execute("SELECT orioledb_flush_local_wal()")
				master.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 4 FROM generate_series(2001, 3000) id""")
				con3.execute("""INSERT INTO test_table (x, y)
					SELECT id, 5 FROM generate_series(3001, 3500) id""")
				con1.commit()
				con3.rollback()
				con2.commit()
				con1.close()
				con2.close()
				con3.close()
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					3010)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
			rmtree(archive_dir, ignore_errors=True)

	def test_in_progress_trx_replication(self):
		with self.node as master:
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					INSERT INTO test_table (x, y)
						SELECT id, 1 FROM generate_series(1, 5) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				master.safe_psql("CHECKPOINT;")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()

				con1 = master.connect()
				con1.begin()
				con1.execute("""INSERT INTO test_table (x, y)
					SELECT id, 3 FROM generate_series(7, 15) id""")
				con1.execute("SELECT orioledb_flush_local_wal()")

				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)

				con1.commit()
				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)

	def test_rewind_in_progress_replica_trx(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							   f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							archive_command = archive_command,
							wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer NOT NULL PRIMARY KEY,
						y integer
					) USING orioledb;
					INSERT INTO test_table (x, y)
						SELECT id, 1 FROM generate_series(1, 5) id;
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				master.safe_psql("CHECKPOINT;")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()

				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")

				master.safe_psql("""INSERT INTO test_table (x, y)
					SELECT id, 2 FROM generate_series(6, 12) id""")
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					12)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				con1 = replica.connect()
				con1.begin()
				con1.execute("""INSERT INTO test_table (x, y)
					SELECT id, 3 FROM generate_series(7, 15) id""")
				con1.execute("""UPDATE test_table SET y = 4 WHERE x = 9""")
				con1.execute("""DELETE FROM test_table WHERE x = 10""")
				con1.execute("""SAVEPOINT s1;""")
				con1.execute("""UPDATE test_table SET y = 5 WHERE x = 9""")
				con1.execute("""SAVEPOINT s2;""")
				con1.execute("""INSERT INTO test_table VALUES (10, 5)""")
				con1.execute("""UPDATE test_table SET y = 6 WHERE x = 8""")
				con1.execute("""SAVEPOINT s3;""")
				con1.execute("""UPDATE test_table SET y = 8 WHERE x = 8""")
				con1.execute("""DELETE FROM test_table WHERE x = 10""")
				con1.execute("""SAVEPOINT s4;""")
				con1.execute("""UPDATE test_table SET y = 8 WHERE x = 9""")
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				# con1.execute("SELECT orioledb_flush_local_wal()")
				con2 = replica.connect()
				master.catchup()
				synchronized = False
				sync_con = master.connect()
				while not synchronized:
					synchronized = sync_con.execute("""
						SELECT orioledb_recovery_synchronized();
					""")[0][0]
				sync_con.close()
				self.catchup_orioledb(master)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					5)
				con1.execute("""ROLLBACK TO SAVEPOINT s2;""")
				con1.execute("""DELETE FROM test_table WHERE y = 8""")
				con1.commit()
				con1.close()
				self.catchup_orioledb(master)
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					13)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					13)
			rmtree(archive_dir, ignore_errors=True)

	def test_rewind_toast(self):
		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							   f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							   archive_command = archive_command,
							   wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE TABLE test_table (
						x integer NOT NULL PRIMARY KEY,
						y text
					) USING orioledb;
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				master.safe_psql("CHECKPOINT;")

				for i in range(0, 10):
					master.safe_psql("""
						INSERT INTO test_table VALUES (%d, '%s')
					""" % (i + 1, generate_string(10000, i)))

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()
				for i in range(10, 20):
					master.safe_psql("""
						INSERT INTO test_table VALUES (%d, '%s')
					""" % (i + 1, generate_string(10000, i)))
				self.catchup_orioledb(replica)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					20)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				for i in range(20, 30):
					master.safe_psql("""
						INSERT INTO test_table VALUES (%d, '%s')
					""" % (i + 1, generate_string(10000, i)))
				replica_rows = []
				for i in range(20, 25):
					replica_rows += [(i + 1, generate_string(10000, i + 10))]
					replica.safe_psql("""
						INSERT INTO test_table VALUES (%d, '%s')
					""" % replica_rows[-1])
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					30)
				self.assertEqual(
					replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
					25)

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				master.execute("SELECT * FROM test_table")
				self.assertEqual(
					master.execute("SELECT COUNT(*) FROM test_table")[0][0],
					25)

				for i in range(20, 25):
					self.assertEqual(
						master.execute("""
							SELECT * FROM test_table WHERE x = %d
						""" % (i + 1))[0], replica_rows[i - 20])
			rmtree(archive_dir, ignore_errors=True)

	def _substract_lists(self, list1: list, list2: list) -> list:
		remaining = Counter(list2)
		result = []
		for val in list1:
			if remaining[val]:
				remaining[val] -= 1
			else:
				result.append(val)
		return sorted(result)

	def _merge_dicts(self, obj1, obj2):
		if isinstance(obj1, list):
			if isinstance(obj2, list):
				obj1.extend(obj2)
			else:
				obj1.append(obj2)
		elif isinstance(obj1, dict):
			if isinstance(obj2, dict):
				for key in obj2:
					if key in obj1:
						obj1[key] = self._merge_dicts(obj1[key], obj2[key])
					else:
						obj1[key] = obj2[key]
		return obj1

	class _RewindTest:
		def __init__(self, test_case: BaseTest):
			self.test_case = test_case
			self.master = test_case.node
			self.replica = test_case.replica
			pass

		def subtest(self):
			return self.test_case.subTest(self.__class__.__name__)

		def before_promote(self):
			pass

		def after_promote(self):
			pass

		def before_rewind(self):
			pass

		def after_rewind(self):
			pass

	class _SecondaryIndexRevivalTest(_RewindTest):
		def before_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test1 (
					x integer NOT NULL PRIMARY KEY,
					y integer
				) USING orioledb;
				CREATE INDEX test1_ix1 ON test1 (y);
				INSERT INTO test1 (x, y)
					SELECT id, (id + 2) % 5 + 1
						FROM generate_series(1, 5) id;
			""")
			return {'tables':{'+':['test1']},
					'indices':{'+':['test1_pkey', 'test1_ix1', 'toast']}}

		def after_promote(self):
			self.master.safe_psql("""
				DROP INDEX test1_ix1;
			""")

			self.replica.safe_psql("""
				INSERT INTO test1 (x, y)
					SELECT id, (id + 2) % 5 + 1
						FROM generate_series(6, 10) id;
			""")

			return {'master': {'indices':{'-':['test1_ix1']}}}

		def before_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1 ORDER BY x"""
				),
				[(1, 4), (2, 5), (3, 1), (4, 2), (5, 3)])
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1 ORDER BY y"""
				),
				[(3, 1), (4, 2), (5, 3), (1, 4), (2, 5)])
			self.test_case.assertEqual(
				self.replica.execute("""
					SELECT * FROM test1 ORDER BY x"""
				),
				[(1, 4), (2, 5), (3, 1), (4, 2), (5, 3),
				(6, 4), (7, 5), (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
				self.replica.execute("""
					SELECT * FROM test1 ORDER BY y, x"""
				),
				[(3, 1), (8, 1), (4, 2), (9, 2), (5, 3), (10, 3),
				(1, 4), (6, 4), (2, 5), (7, 5)])

		def after_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1 ORDER BY x"""
				),
				[(1, 4), (2, 5), (3, 1), (4, 2), (5, 3),
				(6, 4), (7, 5), (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1 ORDER BY y, x"""
				),
				[(3, 1), (8, 1), (4, 2), (9, 2), (5, 3), (10, 3),
				(1, 4), (6, 4), (2, 5), (7, 5)])

	class _PrimaryIndexRevivalTest(_RewindTest):
		def before_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test1b (
					x integer NOT NULL PRIMARY KEY,
					y integer
				) USING orioledb;
				CREATE INDEX test1b_ix1 ON test1 (y);
				INSERT INTO test1b (x, y)
					SELECT id, (id + 2) % 5 + 1
						FROM generate_series(1, 5) id;
			""")
			return {'tables':{'+':['test1b']},
					'indices':{'+':['test1b_pkey', 'test1b_ix1', 'toast']}}

		def after_promote(self):
			self.master.safe_psql("""
				ALTER TABLE test1b DROP CONSTRAINT test1b_pkey;
			""")

			self.replica.safe_psql("""
				INSERT INTO test1b (x, y)
					SELECT id, (id + 2) % 5 + 1
						FROM generate_series(6, 10) id;
			""")

			return {'master': {'indices':{'-':['test1b_pkey'],
										  '+':['ctid_primary']}}}

		def before_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1b ORDER BY x"""
				),
				[(1, 4), (2, 5), (3, 1), (4, 2), (5, 3)])
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1b ORDER BY y"""
				),
				[(3, 1), (4, 2), (5, 3), (1, 4), (2, 5)])
			self.test_case.assertEqual(
				self.replica.execute("""
					SELECT * FROM test1b ORDER BY x"""
				),
				[(1, 4), (2, 5), (3, 1), (4, 2), (5, 3),
				(6, 4), (7, 5), (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
				self.replica.execute("""
					SELECT * FROM test1b ORDER BY y, x"""
				),
				[(3, 1), (8, 1), (4, 2), (9, 2), (5, 3), (10, 3),
				(1, 4), (6, 4), (2, 5), (7, 5)])

		def after_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1b ORDER BY x"""
				),
				[(1, 4), (2, 5), (3, 1), (4, 2), (5, 3),
				(6, 4), (7, 5), (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test1b ORDER BY y, x"""
				),
				[(3, 1), (8, 1), (4, 2), (9, 2), (5, 3), (10, 3),
				(1, 4), (6, 4), (2, 5), (7, 5)])

	class _TableRevivalTest(_RewindTest):
		def before_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test1a (
					x integer NOT NULL PRIMARY KEY,
					y integer
				) USING orioledb;
				INSERT INTO test1a (x, y)
					SELECT id, 10 FROM generate_series(1, 5) id;
			""")
			return {'tables':{'+':['test1a']},
					'indices':{'+':['test1a_pkey','toast']}}

		def after_promote(self):
			self.master.safe_psql("""
				DROP TABLE test1a;
			""")
			return {'master':{'tables':{'-':['test1a']},
							  'indices':{'-':['test1a_pkey','toast']}}}

		def after_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("SELECT COUNT(*) FROM test1a")[0][0],
				5)

	class _RewindSysTreesInProgressTest(_RewindTest):
		def before_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test2 (
					x integer NOT NULL,
					y integer
				) USING orioledb;
				INSERT INTO test2 (x, y)
					SELECT id, 2 FROM generate_series(1, 5) id;
			""")
			self.master.safe_psql("""
				ALTER TABLE test2 ADD PRIMARY KEY (x);
			""")
			return {'tables':{'+':['test2']},
					'indices':{'+':['test2_pkey', 'toast']}}

		def after_promote(self):
			replica_changes = {'tables': {'+': [], '-': []},
							   'indices': {'+': [], '-': []}}
			self.master.safe_psql("""
				DROP TABLE test2;
			""")
			master_changes = {'tables': {'-': ['test2']},
							  'indices': {'-': ['test2_pkey', 'toast']}}
			self.con1 = self.replica.connect()
			con1 = self.con1
			con1.begin()
			con1.execute("""
				ALTER INDEX test2_pkey RENAME TO test2_renamed;
			""")
			con1.execute("SAVEPOINT s0;")
			con1.execute("TRUNCATE test2;")
			con1.execute("""
				INSERT INTO test2 (x, y)
					SELECT id, 210 FROM generate_series(1, 6) id;
			""")
			con1.execute("SAVEPOINT s1;")
			con1.execute("""
				ALTER TABLE test2 DROP CONSTRAINT test2_renamed;
			""")
			con1.execute("""
				INSERT INTO test2 (x, y)
					SELECT id, 220 FROM generate_series(7, 10) id;
			""")
			con1.execute("SAVEPOINT s2;")
			con1.execute("TRUNCATE test2;")
			con1.execute("""
				INSERT INTO test2 (x, y)
					SELECT id, 230 FROM generate_series(1, 20) id;
			""")
			# con1.execute("SELECT orioledb_flush_local_wal()")
			return {'master': master_changes,
					'replica': replica_changes}

		def after_rewind(self):
			self.con1.rollback()
			self.test_case.catchup_orioledb(self.master)
			self.test_case.assertEqual(
				self.master.execute("SELECT COUNT(*) FROM test2")[0][0],
									5)
			return {}

	class _ReplicaTableDropTest(_RewindTest):
		def before_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test2a (
					x integer NOT NULL PRIMARY KEY,
					y integer
				) USING orioledb;
				INSERT INTO test2a (x, y)
					SELECT id, 20 FROM generate_series(1, 5) id;
			""")
			return {'tables':{'+':['test2a']},
					'indices': {'+': ['test2a_pkey', 'toast']}}

		def after_promote(self):
			self.replica.safe_psql("""
				DROP TABLE test2a;
			""")
			return {'replica':{'tables':{'-':['test2a']},
							   'indices': {'-': ['test2a_pkey', 'toast']}}}
	class _NewTablesOnTargetAfterPromoteTest(_RewindTest):
		def after_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test3 (
					x integer,
					y integer
				) USING orioledb;
				CREATE TABLE test4 (
					x integer NOT NULL PRIMARY KEY,
					y integer
				) USING orioledb;
				INSERT INTO test3 (x, y)
					SELECT id, 3 FROM generate_series(1, 5) id;
				INSERT INTO test4 (x, y)
					SELECT id, 4 FROM generate_series(1, 5) id;
			""")
			self.test_case.assertEqual(
				self.master.execute("SELECT COUNT(*) FROM test3")[0][0],
									5)
			self.test_case.assertEqual(
				self.master.execute("SELECT COUNT(*) FROM test4")[0][0],
									5)
			return {'master':{'tables':{'+':['test3', 'test4']},
							  'indices':{'+':['ctid_primary', 'test4_pkey',
							  				  'toast', 'toast']}}}

	class _ToastAfterSysTreeRevivalTest(_RewindTest):
		def before_promote(self):
			self.master.safe_psql("""
				CREATE TABLE test5 (
					x integer NOT NULL PRIMARY KEY,
					y text
				) USING orioledb;
			""")

			self.master_rows = []
			for i in range(0, 10):
				self.master_rows += [(i + 1, generate_string(10000, i))]
			self.replica_rows = self.master_rows.copy()

			for row in self.master_rows:
				self.master.safe_psql("""
					INSERT INTO test5 VALUES (%d, '%s')
				""" % row)

			return {'tables':{'+':['test5']},
					'indices':{'+':['test5_pkey', 'toast']}}

		def after_promote(self):
			self.master.safe_psql("""
				TRUNCATE test5;
			""")

			self.master_rows = []
			for i in range(0, 15):
				self.master_rows += [(i + 1, generate_string(10000, i))]

			for row in self.master_rows:
				self.master.safe_psql("""
					INSERT INTO test5 VALUES (%d, '%s')
				""" % row)

		def before_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test5 ORDER BY x"""
				),
				self.master_rows)
			self.test_case.assertEqual(
				self.replica.execute("""
					SELECT * FROM test5 ORDER BY x"""
				),
				self.replica_rows)

		def after_rewind(self):
			self.test_case.assertEqual(
				self.master.execute("""
					SELECT * FROM test5 ORDER BY x"""
				),
				self.replica_rows)

	def _process_changes(self, changes, master_changes, replica_changes):
		if isinstance(changes, dict):
			if 'master' in changes:
				self._merge_dicts(master_changes,
									changes['master'])
			if 'replica' in changes:
				self._merge_dicts(replica_changes,
									changes['replica'])

		master_tables = [(x,) for x in self._substract_lists(
							master_changes["tables"]['+'],
							master_changes["tables"]['-'])]
		master_indices = [(x,) for x in self._substract_lists(
							master_changes["indices"]['+'],
							master_changes["indices"]['-'])]

		replica_tables = [(x,) for x in self._substract_lists(
							replica_changes["tables"]['+'],
							replica_changes["tables"]['-'])]
		replica_indices = [(x,) for x in self._substract_lists(
							replica_changes["indices"]['+'],
							replica_changes["indices"]['-'])]
		return [master_tables, master_indices, replica_tables, replica_indices]

	def test_rewind_sys_trees(self):
		master_changes = {'tables':{'+':[],'-':[]},
						  'indices':{'+':[],'-':[]}}
		replica_changes = {'tables':{'+':[],'-':[]},
						   'indices':{'+':[],'-':[]}}

		with self.node as master:
			archive_dir = mkdtemp(prefix=TMP_DUMP)
			archive_command = (f'test ! -f {archive_dir}/%f ' +
							   f'&& cp %p {archive_dir}/%f')
			master.append_conf(archive_mode = "on",
							   archive_command = archive_command,
							   wal_log_hints = "on")
			master.start()

			with self.getReplica() as replica:
				tests: List[self._RewindTest] = [
					self._SecondaryIndexRevivalTest(self),
					self._PrimaryIndexRevivalTest(self),
					self._TableRevivalTest(self),
					# self._RewindSysTreesInProgressTest(self),
					self._ReplicaTableDropTest(self),
					self._NewTablesOnTargetAfterPromoteTest(self),
					self._ToastAfterSysTreeRevivalTest(self)
					]

				master.safe_psql("""
					CREATE EXTENSION orioledb;
					CREATE VIEW db_o_tables AS (
						SELECT c.relname, ot.reloid, ot.relnode
						FROM orioledb_table ot JOIN
							pg_database db ON db.oid = ot.datoid JOIN
							pg_class c ON c.oid = ot.reloid
						WHERE db.datname = current_database()
					);
					select * from
						pg_create_physical_replication_slot('replica');
				""")
				for test in tests:
					with test.subtest():
						changes = test.before_promote()
						self._merge_dicts(master_changes, changes)
				master.safe_psql("CHECKPOINT;")

				replica.append_conf(primary_slot_name = 'replica')
				replica.start()

				replica_changes = copy.deepcopy(master_changes)
				self.catchup_orioledb(replica)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")

				for test in tests:
					with test.subtest():
						changes = test.after_promote()
						new_changes = self._process_changes(changes,
															master_changes,
															replica_changes)
						(master_tables, master_indices, replica_tables,
						 replica_indices) = new_changes

				self.assertEqual(
					master.execute("""
						SELECT relname FROM db_o_tables
							ORDER BY relname COLLATE "C"
					"""),
					master_tables
				)
				self.assertEqual(
					master.execute("""
						SELECT name FROM orioledb_index
							ORDER BY name COLLATE "C"
					"""),
					master_indices
				)

				self.assertEqual(
					replica.execute("""
						SELECT relname FROM db_o_tables
							ORDER BY relname COLLATE "C"
					"""),
					replica_tables
				)
				self.assertEqual(
					replica.execute("""
						SELECT name FROM orioledb_index
							ORDER BY name COLLATE "C"
					"""),
					replica_indices
				)

				for test in tests:
					with test.subtest():
						changes = test.before_rewind()
						new_changes = self._process_changes(changes,
															master_changes,
															replica_changes)
						(master_tables, master_indices, replica_tables,
						 replica_indices) = new_changes

				master_slot = 'origin'
				replica.safe_psql(f"""
					select
						pg_create_physical_replication_slot('{master_slot}');
				""")
				primary_conninfo = {
					"host": replica.host,
					"port": replica.port,
					"gssencmode": 'disable',
					"target_session_attrs": 'any'
				}
				self.pg_rewind_master(master, replica.port,
									  master_slot, archive_dir)
				self.start_master_as_standby(master, replica, primary_conninfo,
											 master_slot)
				for test in tests:
					with test.subtest():
						changes = test.after_rewind()
						new_changes = self._process_changes(changes,
															master_changes,
															replica_changes)
						(master_tables, master_indices, replica_tables,
						 replica_indices) = new_changes

				self.assertEqual(
					master.execute("""
						SELECT relname FROM db_o_tables
							ORDER BY relname COLLATE "C"
					"""),
					replica_tables
				)
				self.assertEqual(
					master.execute("""
						SELECT name FROM orioledb_index
							ORDER BY name COLLATE "C"
					"""),
					replica_indices
				)
			rmtree(archive_dir, ignore_errors=True)