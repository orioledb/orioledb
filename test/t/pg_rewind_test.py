import copy
from itertools import chain
from collections import Counter
import os
import re
from shutil import rmtree
import socket
import subprocess
import time
from tempfile import mkdtemp
from typing import List
from unittest import result
import unittest
import glob
import psycopg2
import testgres
from testgres.defaults import default_dbname
from testgres.utils import file_tail, get_bin_path, options_string
from testgres.consts import PG_AUTO_CONF_FILE
from testgres.enums import NodeStatus
from sys import platform

from .base_test import BaseTest, generate_string

REWIND_KEYS_WARNING = "Target and source servers don't have the OrioleDB rewind keys"


class PgRewindTest(BaseTest):

	def pg_rewind(self,
	              target,
	              source_port,
	              verbose=False,
	              rewind_log_file=None,
	              extension_option="--extension",
	              source_pgdata=None,
	              expect_error=False):
		if platform == "darwin":
			dlsuffix = 'dylib'
		elif platform == "win32":
			dlsuffix = 'dll'
		else:
			dlsuffix = 'so'

		pg_rewind_params = [
		 get_bin_path("pg_rewind"),
		 extension_option, f"pg_rewind_orioledb.{dlsuffix}",
		 "--target-pgdata", target,
		]  # yapf: disable

		if source_pgdata is not None:
			pg_rewind_params.extend(["--source-pgdata", source_pgdata])
		else:
			pg_rewind_params.extend([
			    "--source-server",
			    f"port={source_port} dbname={default_dbname()}"
			])

		if verbose == True:
			pg_rewind_params.extend(["--progress", "--debug"])

		if rewind_log_file != None:
			os.environ["ORIOLEDB_REWIND_LOG"] = str(rewind_log_file)

		# start psql process
		process = subprocess.Popen(pg_rewind_params,
		                           stdin=subprocess.PIPE,
		                           stdout=subprocess.PIPE,
		                           stderr=subprocess.PIPE)

		# wait until it finishes and get stdout and stderr
		out, err = process.communicate()
		if (verbose == True):
			with open(rewind_log_file, "a") as f:
				f.write(out.decode("utf-8"))
				f.write(err.decode("utf-8"))
		elif (process.returncode != 0):
			print(out.decode("utf-8"))
			print(err.decode("utf-8"))
		if expect_error:
			self.assertNotEqual(process.returncode, 0)
		else:
			self.assertEqual(process.returncode, 0)
		return process.returncode, out, err

	def pg_rewind_master(self,
	                     master: testgres.PostgresNode,
	                     new_master_port: int,
	                     master_slot: str,
	                     verbose=False,
	                     rewind_log_file=None,
	                     **rewind_args):
		if master.status() == NodeStatus.Running:
			master.stop()
		master_cleanup_command = f'pg_archivecleanup {self.archive_dir} %r'
		master.append_conf(recovery_target_timeline='latest',
		                   archive_cleanup_command=master_cleanup_command,
		                   restore_command=f'cp {self.archive_dir}/%f %p',
		                   primary_slot_name=master_slot)
		if rewind_log_file == None:
			rewind_log_file = master.pg_log_file
		return self.pg_rewind(master.data_dir, new_master_port, verbose,
		                      rewind_log_file, **rewind_args)

	def start_master_as_standby(self,
	                            master: testgres.PostgresNode,
	                            replica: testgres.PostgresNode,
	                            primary_conninfo: dict,
	                            master_slot: str,
	                            start_params=[]):
		master._assign_master(replica)
		master.append_conf(port=master.port)
		master.append_conf(primary_conninfo=options_string(**primary_conninfo),
		                   primary_slot_name=master_slot,
		                   filename=PG_AUTO_CONF_FILE)
		signal_name = os.path.join(master.data_dir, "standby.signal")
		with open(signal_name, 'a'):
			os.utime(signal_name, None)
		master.start(start_params)

	def enable_archive(self, node):
		self.archive_dir = os.path.join(node.base_dir, 'archive')
		node.os_ops.makedirs(self.archive_dir)
		archive_command = (f'test ! -f {self.archive_dir}/%f ' +
		                   f'&& cp %p {self.archive_dir}/%f')
		node.append_conf(archive_mode="on",
		                 archive_command=archive_command,
		                 wal_log_hints="on")

	def spawn_standby_of(self, node: testgres.PostgresNode, name):
		# A standby of node on a port of its own: the test's two ports are
		# taken by the master and the replica
		base_dir = os.path.join(os.path.dirname(node.base_dir),
		                        f"{self.myName}_{name}")
		if os.path.exists(base_dir):
			rmtree(base_dir)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
			s.bind(("127.0.0.1", 0))
			port = s.getsockname()[1]
		standby = testgres.get_new_node(name, base_dir=base_dir, port=port)
		subprocess.run([
		    get_bin_path("pg_basebackup"), "-D", standby.data_dir, "-h",
		    node.host, "-p",
		    str(node.port), "-X", "stream", "-R", "-c", "fast"
		],
		               check=True)
		standby.append_conf(port=port, primary_slot_name='')
		standby._assign_master(node)
		return standby

	@staticmethod
	def divergence_keys(node, promoted=None):
		# Return the OrioleDB rewind keys the promotion logged at the
		# beginning of its timeline, as the node sees them in its WAL.  The
		# promoted node is the one that was promoted: the node itself by
		# default.  None for a node that was not promoted.
		promoted = promoted or node
		promoted.safe_psql("CHECKPOINT")
		tli = promoted.execute(
		    "SELECT timeline_id FROM pg_control_checkpoint()")[0][0]
		if tli == 1:
			return None
		history = promoted.execute(
		    f"SELECT pg_read_file('pg_wal/{tli:08X}.history')")[0][0]
		switchpoint = history.strip().splitlines()[-1].split('\t')[1]
		keys = node.execute(
		    f"SELECT orioledb_pg_rewind_divergence_keys('{switchpoint}'::pg_lsn)"
		)[0][0]
		return None if keys is None else bytes(keys)

	def test_pg_rewind_remove_rows(self):
		with self.node as master:
			self.enable_archive(master)
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
				replica.append_conf(primary_slot_name='replica')
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
				self.pg_rewind_master(master, replica.port, master_slot)
				self.start_master_as_standby(master, replica, primary_conninfo,
				                             master_slot)
				self.assertEqual(
				    master.execute("SELECT COUNT(*) FROM test_table")[0][0],
				    200)

	@staticmethod
	def run_sql(node, sql):
		for query in ([sql] if isinstance(sql, str) or callable(sql) else sql):
			if callable(query):
				query(node)
			else:
				node.safe_psql(query)

	@staticmethod
	def table_digest(node, table):
		return node.execute(f"""
            SELECT count(*), coalesce(sum(hashtext(t::text)), 0)
                FROM {table} t
        """)[0]

	@staticmethod
	def create_table_sql(table, rows, access_method='orioledb'):
		return f"""
            CREATE TABLE {table} (
                id integer NOT NULL PRIMARY KEY,
                val text,
                pad text
            ) USING {access_method};
            INSERT INTO {table}
                SELECT id, 'orig', repeat('x', 60)
                    FROM generate_series(1, {rows}) id;
        """

	@staticmethod
	def divergence_sql(*tables):
		"""
        Changes for the promoted replica and for the old master.  Every
        transaction changes all the tables the same way.  Part of the master
        changes is checkpointed, the rest is only in WAL.
        """

		def each(statements):
			return "".join(statements.format(t=t) for t in tables)

		replica_sql = [
		    each("UPDATE {t} SET val = 'replica' WHERE id % 100 = 0;")
		]
		master_sql = [
		    each("UPDATE {t} SET val = 'master' WHERE id % 100 = 1;"
		         "DELETE FROM {t} WHERE id % 100 = 2;"), "CHECKPOINT;",
		    each("UPDATE {t} SET val = 'master2' WHERE id % 100 = 3;")
		]
		return replica_sql, master_sql

	def rewind_diverged(self,
	                    master: testgres.PostgresNode,
	                    replica: testgres.PostgresNode,
	                    setup_sql,
	                    replica_sql,
	                    master_sql,
	                    source_pgdata=False,
	                    **rewind_args):
		"""
        Run setup_sql on the master and stream it to the replica, promote the
        replica, diverge the nodes with replica_sql and master_sql, then
        pg_rewind the master and start it as a standby of the replica.
        The master has caught up with the replica when this returns.
        """
		master.safe_psql("""
            CREATE EXTENSION orioledb;
            SELECT pg_create_physical_replication_slot('replica');
        """)
		self.run_sql(master, setup_sql)
		replica.append_conf(primary_slot_name='replica')
		replica.start()
		master.safe_psql("CHECKPOINT;")
		self.catchup_orioledb(replica)
		replica.promote()
		replica.safe_psql("CHECKPOINT;")
		self.run_sql(replica, replica_sql)
		self.run_sql(master, master_sql)
		self.rewind_and_follow(master, replica, source_pgdata, **rewind_args)

	def rewind_in_progress_trx(self,
	                           master: testgres.PostgresNode,
	                           replica: testgres.PostgresNode,
	                           setup_sql,
	                           trx_sql,
	                           committed_sql=None,
	                           before_rewind=None):
		"""
        Run setup_sql on the master and stream it to the replica.  Then run
        trx_sql in a transaction that is still in progress when the replica
        is promoted: its changes reach the WAL before the last common
        checkpoint, the promoted replica aborts it, and the master commits it
        after the split.  committed_sql runs and commits meanwhile, before the
        last common checkpoint, so both nodes keep it.  Then pg_rewind the
        master and start it as a standby of the replica.  The master has
        caught up with the replica when this returns.  Returns what
        pg_rewind() does.
        """
		master.safe_psql("""
            CREATE EXTENSION orioledb;
            SELECT pg_create_physical_replication_slot('replica');
        """)
		self.run_sql(master, setup_sql)
		replica.append_conf(primary_slot_name='replica')
		replica.start()
		con = master.connect()
		con.begin()
		con.execute(trx_sql)
		con.execute("SELECT orioledb_flush_local_wal()")
		if committed_sql:
			master.safe_psql(committed_sql)
		master.safe_psql("CHECKPOINT;")
		self.catchup_orioledb(replica)
		replica.promote()
		replica.safe_psql("CHECKPOINT;")
		con.commit()
		con.close()
		if before_rewind:
			before_rewind()
		return self.rewind_and_follow(master, replica)

	def rewind_and_follow(self,
	                      master: testgres.PostgresNode,
	                      replica: testgres.PostgresNode,
	                      source_pgdata=False,
	                      **rewind_args):
		master_slot = 'origin'
		replica.safe_psql(
		    f"SELECT pg_create_physical_replication_slot('{master_slot}');")
		if source_pgdata:
			# --source-pgdata needs a cleanly shut down source
			replica.stop()
			rewind_args['source_pgdata'] = replica.data_dir
		result = self.pg_rewind_master(master, replica.port, master_slot,
		                               **rewind_args)
		if source_pgdata:
			replica.start()
		primary_conninfo = {
		    "host": replica.host,
		    "port": replica.port,
		    "gssencmode": 'disable',
		    "target_session_attrs": 'any'
		}
		self.start_master_as_standby(master, replica, primary_conninfo,
		                             master_slot)
		self.catchup_orioledb(master)
		return result

	def test_pg_rewind_short_option(self):
		"""
        Check this option works as well as full version '--extension'
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				self.rewind_diverged(master,
				                     replica,
				                     self.create_table_sql('o_test', 1000),
				                     replica_sql,
				                     master_sql,
				                     extension_option="-e")
				self.assertEqual(self.table_digest(master, 'o_test'),
				                 self.table_digest(replica, 'o_test'))

	def test_pg_rewind_removes_stale_slot(self):
		"""
        The master had the slot before switchover.  pg_rewind must remove
        the target's replication slot contents, so the rewound master
        has not slots.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				self.rewind_diverged(master, replica,
				                     self.create_table_sql('o_test', 1000),
				                     replica_sql, master_sql)
				self.assertEqual(
				    master.execute(
				        "SELECT slot_name FROM pg_replication_slots"), [])

	def test_pg_rewind_source_pgdata(self):
		"""
        Rewind from the data directory of a cleanly shut down source.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				self.rewind_diverged(master,
				                     replica,
				                     self.create_table_sql('o_test', 1000),
				                     replica_sql,
				                     master_sql,
				                     source_pgdata=True)
				self.assertEqual(self.table_digest(master, 'o_test'),
				                 self.table_digest(replica, 'o_test'))

	def test_pg_rewind_tablespace(self):
		"""
        OrioleDB keeps the data under tablespace at pg_tblspc/<oid>/<version>/orioleb_data,
        outside the top-level orioledb_data directory, so pg_rewind should handle
        it separately.
        """
		with self.node as master:
			master.append_conf(allow_in_place_tablespaces=True)
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				self.rewind_diverged(master, replica, [
				    "CREATE TABLESPACE o_ts LOCATION '';",
				    "SET default_tablespace = o_ts;" +
				    self.create_table_sql('o_test', 10000)
				], replica_sql, master_sql)
				self.assertNotEqual(
				    glob.glob(
				        os.path.join(replica.data_dir, 'pg_tblspc', '*', '*',
				                     'orioledb_data', '*', '*')), [])
				self.assertEqual(self.table_digest(master, 'o_test'),
				                 self.table_digest(replica, 'o_test'))

	def test_pg_rewind_tablespace_created_on_source(self):
		"""
        Just-promoted replica creates a tablespace and table in it; hence,
        the master must get both from the replica.
        """
		with self.node as master:
			master.append_conf(allow_in_place_tablespaces=True)
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				replica_sql += [
				    "CREATE TABLESPACE o_ts LOCATION '';",
				    "SET default_tablespace = o_ts;" +
				    self.create_table_sql('o_new', 10000), "CHECKPOINT;"
				]
				self.rewind_diverged(master, replica,
				                     self.create_table_sql('o_test', 10000),
				                     replica_sql, master_sql)
				for table in ['o_test', 'o_new']:
					self.assertEqual(self.table_digest(master, table),
					                 self.table_digest(replica, table))

	def test_pg_rewind_tablespace_created_on_target(self):
		"""
        Master creates a tablespace and a table in it;  hence, the pg_rewind
        must remove both.
        """
		with self.node as master:
			master.append_conf(allow_in_place_tablespaces=True)
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				master_sql += [
				    "CREATE TABLESPACE o_ts LOCATION '';",
				    "SET default_tablespace = o_ts;" +
				    self.create_table_sql('o_new', 10000), "CHECKPOINT;"
				]
				self.rewind_diverged(master, replica,
				                     self.create_table_sql('o_test', 10000),
				                     replica_sql, master_sql)
				self.assertEqual(self.table_digest(master, 'o_test'),
				                 self.table_digest(replica, 'o_test'))
				self.assertEqual(
				    master.execute("""
                        SELECT to_regclass('o_new'),
                            (SELECT count(*) FROM pg_tablespace
                                WHERE spcname = 'o_ts')
                    """), [(None, 0)])

	def test_pg_rewind_apply_new_rows(self):
		"""
        Both nodes insert, update and delete rows, partly the same keys:
        5 and 6 are inserted on both with different values, and 2 is
        updated on the master but deleted on the replica.  After the rewind
        the master must have exactly the replica's rows.
        """
		with self.node as master:
			self.enable_archive(master)
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

				replica.append_conf(primary_slot_name='replica')
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
				replica.safe_psql(
				    """UPDATE test_table SET y = 7 WHERE x = 5""")
				replica.safe_psql("""DELETE FROM test_table WHERE x = 2""")
				self.assertEqual(
				    master.execute("SELECT * FROM test_table ORDER BY x"),
				    [(2, 5), (3, 5), (4, 2), (5, 3), (6, 3), (7, 3), (8, 3),
				     (10, 3)])
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

				self.pg_rewind_master(master, replica.port, master_slot)

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

	def test_pg_rewind_multiple_relations(self):
		"""
        Like test_pg_rewind_apply_new_rows, for three tables with a
        secondary index each, so the rewind file holds several trees.
        """
		with self.node as master:
			self.enable_archive(master)
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

				replica.append_conf(primary_slot_name='replica')
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
				self.assertEqual(replica.execute("SELECT * FROM test1"),
				                 [(1, 1), (2, 1), (3, 2), (4, 2)])
				self.assertEqual(replica.execute("SELECT * FROM test2"),
				                 [(1, 1), (2, 1), (3, 2), (4, 2)])
				self.assertEqual(replica.execute("SELECT * FROM test3"),
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
				    master.execute("SELECT * FROM test1 ORDER BY x"), [(1, 1),
				                                                       (2, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 3),
				                                                       (6, 3),
				                                                       (7, 3),
				                                                       (8, 3)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY x"), [(1, 1),
				                                                       (2, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 3),
				                                                       (6, 3),
				                                                       (7, 3),
				                                                       (8, 3)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY x"), [(1, 1),
				                                                       (2, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 3),
				                                                       (6, 3),
				                                                       (7, 3),
				                                                       (8, 3)])

				self.assertEqual(
				    master.execute("SELECT * FROM test1 ORDER BY y"), [(1, 1),
				                                                       (2, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 3),
				                                                       (6, 3),
				                                                       (7, 3),
				                                                       (8, 3)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY y"), [(1, 1),
				                                                       (2, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 3),
				                                                       (6, 3),
				                                                       (7, 3),
				                                                       (8, 3)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY y"), [(1, 1),
				                                                       (2, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 3),
				                                                       (6, 3),
				                                                       (7, 3),
				                                                       (8, 3)])

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

				self.pg_rewind_master(master, replica.port, master_slot)
				self.assertTrue(os.path.isfile(rewind_file))
				self.start_master_as_standby(master, replica, primary_conninfo,
				                             master_slot)
				self.catchup_orioledb(master)
				self.assertEqual(
				    master.execute("SELECT * FROM test1 ORDER BY x"), [(1, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY x"), [(1, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY x"), [(1, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])

				self.assertEqual(
				    master.execute("SELECT * FROM test1 ORDER BY y"), [(1, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (6, 4),
				                                                       (5, 7)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY y"), [(1, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (6, 4),
				                                                       (5, 7)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY y"), [(1, 1),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (6, 4),
				                                                       (5, 7)])

				replica.safe_psql("""
                    UPDATE test1 SET y = 6 WHERE x = 1;
                    UPDATE test2 SET y = 6 WHERE x = 1;
                    UPDATE test3 SET y = 6 WHERE x = 1;
                """)

				self.catchup_orioledb(master)
				self.assertEqual(
				    master.execute("SELECT * FROM test1 ORDER BY x"), [(1, 6),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY x"), [(1, 6),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY x"), [(1, 6),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])

				replica.safe_psql("""
                    UPDATE test1 SET y = 7 WHERE x = 1;
                    UPDATE test2 SET y = 7 WHERE x = 1;
                    UPDATE test3 SET y = 7 WHERE x = 1;
                """)

				self.catchup_orioledb(master)
				self.assertEqual(
				    master.execute("SELECT * FROM test1 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				master.stop(["-m", 'immediate'])
				self.assertTrue(os.path.isfile(rewind_file))
				# check that rewind running again if no checkpoint
				master.start()

				self.catchup_orioledb(master)
				self.assertEqual(
				    master.execute("SELECT * FROM test1 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4)])

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
				    master.execute("SELECT * FROM test1 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4),
				                                                       (8, 8)])
				self.assertEqual(
				    master.execute("SELECT * FROM test2 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4),
				                                                       (8, 8)])
				self.assertEqual(
				    master.execute("SELECT * FROM test3 ORDER BY x"), [(1, 7),
				                                                       (3, 2),
				                                                       (4, 2),
				                                                       (5, 7),
				                                                       (6, 4),
				                                                       (8, 8)])

	def test_pg_rewind_in_progress_target_trx(self):
		with self.node as master:
			self.enable_archive(master)
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

				replica.append_conf(primary_slot_name='replica')
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
				self.pg_rewind_master(master, replica.port, master_slot)
				self.start_master_as_standby(master, replica, primary_conninfo,
				                             master_slot)
				self.assertEqual(
				    master.execute("SELECT COUNT(*) FROM test_table")[0][0], 5)

	def test_pg_rewind_tablespace_dropped_on_source(self):
		# The promoted replica drops a tablespace and the table in it, while
		# the master keeps changing that table, so the rewind must remove
		# both.
		with self.node as master:
			master.append_conf(allow_in_place_tablespaces=True)
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				replica_sql += ["DROP TABLE o_old;", "DROP TABLESPACE o_ts;"]
				master_sql += [
				    "UPDATE o_old SET val = 'master' WHERE id % 100 = 5;",
				    "CHECKPOINT;"
				]
				self.rewind_diverged(master, replica, [
				    self.create_table_sql('o_test', 10000),
				    "CREATE TABLESPACE o_ts LOCATION '';",
				    "SET default_tablespace = o_ts;" +
				    self.create_table_sql('o_old', 10000)
				], replica_sql, master_sql)
				self.assertEqual(self.table_digest(master, 'o_test'),
				                 self.table_digest(replica, 'o_test'))
				self.assertEqual(
				    master.execute("""
                        SELECT to_regclass('o_old'),
                            (SELECT count(*) FROM pg_tablespace
                                WHERE spcname = 'o_ts')
                    """), [(None, 0)])

	def test_pg_rewind_heap_and_orioledb(self):
		# Every transaction changes a heap table and an OrioleDB table the
		# same way, so after the rewind both must match the source and each
		# other.
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql(
				    'o_test', 'h_test')
				self.rewind_diverged(
				    master, replica,
				    self.create_table_sql('o_test', 10000) +
				    self.create_table_sql('h_test', 10000, 'heap'),
				    replica_sql, master_sql)
				for table in ['o_test', 'h_test']:
					self.assertEqual(self.table_digest(master, table),
					                 self.table_digest(replica, table))
				self.assertEqual(
				    master.execute("""
                        SELECT count(*) FROM o_test o FULL JOIN h_test h USING (id)
                            WHERE o.val IS DISTINCT FROM h.val
                    """), [(0, )])

	def check_in_progress_trx_rewind(self,
	                                 trx_sql,
	                                 committed_sql=None,
	                                 orig_rows=1000,
	                                 recovery_pool_size=None):
		# One transaction changes an OrioleDB table and a heap table the same
		# way.  It is in progress after common checkpoint: the promoted replica
		# aborts it, the master commits it.  After the rewind both tables must
		# have the same rows.
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				tables = ['o_test', 'h_test']

				if recovery_pool_size is not None:
					replica.append_conf(
					    f'orioledb.recovery_pool_size = {recovery_pool_size}')

				def each(sql):
					return sql and "".join(sql.format(t=t) for t in tables)

				_, _, err = self.rewind_in_progress_trx(
				    master, replica,
				    self.create_table_sql('o_test', 1000) +
				    self.create_table_sql('h_test', 1000, 'heap') +
				    "CREATE INDEX o_test_val_idx ON o_test (val);" +
				    "CREATE INDEX h_test_val_idx ON h_test (val);",
				    each(trx_sql), each(committed_sql))
				self.assertNotIn(REWIND_KEYS_WARNING, err.decode())
				result = {}
				for name, node in [('replica', replica), ('master', master)]:
					for table in tables:
						result[(name, table)] = node.execute(f"""
                            SELECT count(*),
                                   count(*) FILTER (WHERE val = 'orig'),
                                   sum(id)
                                FROM {table}
                        """)[0]
				self.assertEqual(
				    result, {key: (1000, orig_rows, 500500)
				             for key in result})
				for node in (replica, master):
					con = node.connect()
					con.execute("SET enable_seqscan = off")
					for table in tables:
						plan = con.execute(
						    f"EXPLAIN SELECT * FROM {table} WHERE val = 'orig'"
						)
						self.assertTrue(any('Index' in row[0] for row in plan))
					con.close()

	def test_pg_rewind_in_progress_target_trx_update(self):
		"""
        The same rows are updated twice, so replica records each key twice: the
        duplicates must be collapsed by pg_rewind keys sort.
        """
		self.check_in_progress_trx_rewind(
		    "UPDATE {t} SET val = 'trx1' WHERE mod(id, 10) = 1;"
		    "UPDATE {t} SET val = 'trx2' WHERE mod(id, 10) = 1;")

	def test_pg_rewind_in_progress_target_trx_delete(self):
		"""
        Rows the master just deleted comes back, because this transaction was
        rolled back in replica.
        """
		self.check_in_progress_trx_rewind(
		    "DELETE FROM {t} WHERE mod(id, 10) = 2;")

	def test_pg_rewind_in_progress_primary_key_update_single_worker(self):
		self.check_in_progress_trx_rewind(
		    "UPDATE {t} SET id = id + 10000 WHERE mod(id, 10) = 4;",
		    recovery_pool_size=1)

	def test_pg_rewind_in_progress_toast_update(self):
		self.check_in_progress_trx_rewind(
		    "UPDATE {t} SET val = repeat('unfinished', 2000) "
		    "WHERE mod(id, 10) = 5;")

	def test_pg_rewind_more_than_four_mb_of_keys(self):
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				self.rewind_in_progress_trx(
				    master, replica, """
                        CREATE TABLE o_large_keys (
                            id text PRIMARY KEY,
                            val text
                        ) USING orioledb;
                        INSERT INTO o_large_keys
                            SELECT lpad(id::text, 2000, 'x'), 'orig'
                                FROM generate_series(1, 2500) id;
                    """, "UPDATE o_large_keys SET val = 'unfinished'")
				expected = [(2500, 2500)]
				query = """
                    SELECT count(*), count(*) FILTER (WHERE val = 'orig')
                        FROM o_large_keys
                """
				self.assertEqual(master.execute(query), expected)
				self.assertEqual(replica.execute(query), expected)

	def test_pg_rewind_in_progress_target_trx_drop(self):
		"""
        The master commits DROP TABLE, but the promoted replica rolls it back.
        The table must come back with all its rows.
        """
		self.check_in_progress_trx_rewind("DROP TABLE {t};")

	def test_pg_rewind_in_progress_target_trx_keeps_committed(self):
		"""
        A transaction that commits while the rewound one is in progress
        stays on both nodes
        """
		self.check_in_progress_trx_rewind(
		    "UPDATE {t} SET val = 'trx' WHERE mod(id, 10) = 1;",
		    "UPDATE {t} SET val = 'committed' WHERE mod(id, 10) = 3;",
		    orig_rows=900)

	def test_pg_rewind_zero_entry_file(self):
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    CREATE TABLE o_test (id integer PRIMARY KEY) USING orioledb;
                    INSERT INTO o_test VALUES (1);
                    SELECT pg_create_physical_replication_slot('replica');
                """)
				replica.append_conf(primary_slot_name='replica')
				replica.start()
				master.safe_psql("CHECKPOINT")
				self.catchup_orioledb(replica)
				replica.promote()
				replica.safe_psql("CHECKPOINT")

				self.assertEqual(self.divergence_keys(replica), b'')

				replica.safe_psql("INSERT INTO o_test VALUES (2)")
				master.safe_psql("INSERT INTO o_test VALUES (3)")
				self.rewind_and_follow(master, replica)
				self.assertEqual(
				    master.execute("SELECT * FROM o_test ORDER BY id"),
				    [(1, ), (2, )])

	def test_pg_rewind_zero_entry_file_without_replay(self):
		with self.node as master:
			self.enable_archive(master)
			master.start()
			master.safe_psql("CREATE EXTENSION orioledb;")
			master.stop()

			# A clean shutdown checkpoint needs no WAL replay. PostgreSQL
			# therefore skips RmgrCleanup() when this node enters standby and
			# is promoted immediately.
			signal_name = os.path.join(master.data_dir, "standby.signal")
			with open(signal_name, 'a'):
				os.utime(signal_name, None)
			master.start()
			master.promote()

			self.assertEqual(self.divergence_keys(master), b'')

	def check_rewind_keys_promotion_crash(self, event):
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica.append_conf("orioledb.enable_stopevents = true\n"
				                    "orioledb.restart_after_crash = off")
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    CREATE TABLE o_test (
                        id integer PRIMARY KEY,
                        val text
                    ) USING orioledb;
                    INSERT INTO o_test SELECT id, 'orig'
                        FROM generate_series(1, 100) id;
                    SELECT pg_create_physical_replication_slot('replica');
                """)
				replica.append_conf(primary_slot_name='replica')
				replica.start()
				self.catchup_orioledb(replica)

				con = master.connect()
				con.begin()
				con.execute("UPDATE o_test SET val = 'unfinished'")
				con.execute("SELECT orioledb_flush_local_wal()")
				master.safe_psql("CHECKPOINT")
				self.catchup_orioledb(replica)
				replica.safe_psql(
				    f"SELECT pg_stopevent_set('{event}', 'true')")
				subprocess.run([
				    get_bin_path('pg_ctl'), '-D', replica.data_dir, '-W',
				    'promote'
				],
				               check=True)
				replica.poll_query_until(
				    "SELECT coalesce(array_length(waiter_pids, 1), 0) > 0 "
				    "FROM pg_stopevents() "
				    f"WHERE stopevent = '{event}'",
				    expected=True)
				replica.stop(['-m', 'immediate'])
				replica.start()
				if replica.execute("SELECT pg_is_in_recovery()")[0][0]:
					replica.promote()
				con.commit()
				con.close()

				self.assertTrue(self.divergence_keys(replica))
				self.assertEqual(
				    replica.execute("""
                    SELECT count(*), count(*) FILTER (WHERE val = 'orig')
                        FROM o_test
                """), [(100, 100)])

	def test_pg_rewind_keys_crash_after_pending(self):
		self.check_rewind_keys_promotion_crash('pg_rewind_keys_after_pending')

	def test_pg_rewind_keys_crash_after_wal_flush(self):
		self.check_rewind_keys_promotion_crash(
		    'pg_rewind_keys_after_wal_flush')

	def test_pg_rewind_missing_rewind_keys_fails(self):
		"""
        The keys of the promotion are gone with the WAL around its
        switchpoint: pg_rewind must stop instead of taking it for a promotion
        without keys.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    CREATE TABLE h_test (id int);
                """)
				replica.append_conf(wal_keep_size='0')
				replica.start()
				self.catchup_orioledb(replica)
				replica.promote()
				for i in range(4):
					replica.safe_psql("INSERT INTO h_test VALUES (1);")
					replica.safe_psql("SELECT pg_switch_wal();")
					replica.safe_psql("CHECKPOINT;")
				master.safe_psql("INSERT INTO h_test VALUES (2);")

				replica.safe_psql(
				    "SELECT pg_create_physical_replication_slot('origin')")
				_, _, err = self.pg_rewind_master(master,
				                                  replica.port,
				                                  'origin',
				                                  expect_error=True)
				self.assertIn('could not read the OrioleDB rewind keys',
				              err.decode())

	def test_pg_rewind_in_progress_trx_promoted_target(self):
		"""
        The promoted replica rolls back a transaction the master commits
        afterwards, then the replica is rewound back onto the master.  The
        replica has the keys of the rows it rolled back itself.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    SELECT pg_create_physical_replication_slot('replica');
                """)
				self.run_sql(master, self.create_table_sql('o_test', 1000))
				replica.append_conf(primary_slot_name='replica')
				replica.start()
				self.catchup_orioledb(replica)

				con = master.connect()
				con.begin()
				con.execute("""
                    UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;
                    DELETE FROM o_test WHERE mod(id, 10) = 2;
                    INSERT INTO o_test SELECT id, 'trx', 'x'
                        FROM generate_series(1001, 1100) id;
                """)
				con.execute("SELECT orioledb_flush_local_wal()")
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				con.commit()
				con.close()

				master_slot = 'origin'
				master.safe_psql(
				    f"SELECT pg_create_physical_replication_slot('{master_slot}');"
				)
				_, _, err = self.pg_rewind_master(replica, master.port,
				                                  master_slot)
				self.assertNotIn(REWIND_KEYS_WARNING, err.decode())
				# Stay on the master's timeline, not the replica's own one
				replica.append_conf(recovery_target_timeline='current')
				primary_conninfo = {
				    "host": master.host,
				    "port": master.port,
				    "gssencmode": 'disable',
				    "target_session_attrs": 'any'
				}
				self.start_master_as_standby(replica, master, primary_conninfo,
				                             master_slot)
				self.catchup_orioledb(replica)

				query = "SELECT count(*), count(*) FILTER (WHERE val = 'trx') FROM o_test"
				self.assertEqual(master.execute(query), [(1000, 200)])
				self.assertEqual(replica.execute(query), [(1000, 200)])

	def test_pg_rewind_in_progress_trx_standby_follows_promoted(self):
		"""
        A standby of the replica holds the transaction in progress just like
        the replica does.  The replica is promoted and rolls the transaction
        back; the standby gets the rollback and the rewind keys through WAL.
        It replays the keys without applying them, keeps replaying what the
        promoted node does next, and has the keys in its own WAL.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    SELECT pg_create_physical_replication_slot('replica');
                """)
				self.run_sql(master, self.create_table_sql('o_test', 1000))
				replica.append_conf(primary_slot_name='replica')
				replica.start()
				self.catchup_orioledb(replica)

				con = master.connect()
				con.begin()
				con.execute("""
                    UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;
                    DELETE FROM o_test WHERE mod(id, 10) = 2;
                    INSERT INTO o_test SELECT id, 'trx', 'x'
                        FROM generate_series(1001, 1100) id;
                """)
				con.execute("SELECT orioledb_flush_local_wal()")
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				standby = self.spawn_standby_of(replica, 'standby')
				standby.start()
				lsn = replica.execute("SELECT pg_last_wal_replay_lsn()")[0][0]
				standby.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn",
				    expected=True)

				replica.promote()
				replica.safe_psql("CHECKPOINT;")
				con.commit()
				con.close()

				lsn = replica.execute("SELECT pg_current_wal_lsn()")[0][0]
				standby.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn",
				    expected=True)
				query = "SELECT count(*), count(*) FILTER (WHERE val = 'trx') FROM o_test"
				self.assertEqual(replica.execute(query), [(1000, 0)])
				self.assertEqual(standby.execute(query), [(1000, 0)])

				keys = self.divergence_keys(replica)
				self.assertTrue(keys)
				self.assertEqual(self.divergence_keys(standby, replica), keys)

				# What the promoted node does next reaches the standby
				replica.safe_psql("INSERT INTO o_test VALUES (2000, 'new', 'x')")
				lsn = replica.execute("SELECT pg_current_wal_lsn()")[0][0]
				standby.poll_query_until(
				    f"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn",
				    expected=True)
				self.assertEqual(standby.execute(query), [(1001, 0)])

	@staticmethod
	def horizon_workload(node,
	                     count,
	                     tag,
	                     table='o_test',
	                     rollback_every=None):
		# One transaction per iteration, like a real client
		con = node.connect()
		for i in range(count):
			con.begin()
			con.execute(f"""
                UPDATE {table} SET val = '{tag}{i}'
                    WHERE mod(id, 50) = {i % 50};
                DELETE FROM {table} WHERE id = {1000 + i % 100};
                INSERT INTO {table} VALUES ({1000 + i % 100}, '{tag}', 'x');
            """)
			if rollback_every and i % rollback_every == 0:
				con.rollback()
			else:
				con.commit()
			if i % 40 == 39:
				con.execute("CHECKPOINT")
		con.close()

	@staticmethod
	def xid_meta(node):
		names = [
		    'nextxid', 'runxmin', 'globalxmin', 'lastxidwhenupdatedglobalxmin',
		    'writeinprogressxmin', 'writtenxmin', 'checkpointretainxmin',
		    'checkpointretainxmax', 'cleanedxmin'
		]
		return dict(
		    zip(names,
		        node.execute(f"SELECT {', '.join(names)} "
		                     "FROM orioledb_get_xid_meta()")[0]))

	def check_horizons_order(self, node):
		# The xid horizons never pass the next oxid, and the global one
		# never passes the running one
		meta = self.xid_meta(node)
		for name, value in meta.items():
			self.assertLessEqual(value, meta['nextxid'], f"{name}: {meta}")
		self.assertLessEqual(meta['globalxmin'], meta['runxmin'], str(meta))
		self.assertLessEqual(meta['checkpointretainxmin'],
		                     meta['checkpointretainxmax'], str(meta))

	@staticmethod
	def big_undo_trx(node, rounds, tag):
		# About 1.5 MB of rows rewritten again and again in one transaction:
		# far more undo than a small undo buffer holds
		con = node.connect()
		con.begin()
		for i in range(rounds):
			con.execute(f"UPDATE o_test SET val = '{tag}{i}', "
			            f"pad = repeat('{tag[0]}', 500)")
		con.commit()
		con.close()

	@staticmethod
	def undo_written(node):
		return node.execute("""
            SELECT writtenlocation FROM orioledb_get_undo_meta()
             WHERE undo_type = 'row'
        """)[0][0]

	def check_undo_order(self, node):
		# The undo locations keep the order documented at UndoMeta
		rows = node.execute("""
            SELECT undo_type, cleanedlocation, minprocretainlocation,
                   minproctransactionretainlocation, minprocreservedlocation,
                   writtenlocation, writeinprogresslocation, lastusedlocation,
                   advancereservedlocation, checkpointretainstartlocation,
                   checkpointretainendlocation
              FROM orioledb_get_undo_meta()
        """)
		self.assertTrue(rows)
		for (undo_type, cleaned, retain, trx_retain, reserved, written,
		     write_in_progress, last_used, advance_reserved, chkp_start,
		     chkp_end) in rows:
			where = f"{undo_type}: {rows}"
			chain = [cleaned, retain, trx_retain, reserved]
			self.assertEqual(chain, sorted(chain), where)
			chain = [cleaned, written, write_in_progress, reserved]
			self.assertEqual(chain, sorted(chain), where)
			chain = [reserved, last_used, advance_reserved]
			self.assertEqual(chain, sorted(chain), where)
			self.assertLessEqual(chkp_start, chkp_end, where)

	def check_tbl(self, node):
		# It takes an exclusive lock: primaries only
		with node.connect() as con:
			self.assertTrue(
			    con.execute("""
                    SELECT orioledb_tbl_check('o_test'::regclass, true)
                """)[0][0])

	def assert_same_tables(self, master, replica, tables=('o_test', )):
		for table in tables:
			self.assertEqual(self.table_digest(master, table),
			                 self.table_digest(replica, table))

	def exercise_rewound(self, master, replica, tables=('o_test', )):
		"""
        The rewind leaves the undo metadata of the target as it is and only
        reseeds its xid horizons.  The rewound master follows the source:
        check it stays consistent through the source's new transactions (their
        oxids may be below what the target used), a restartpoint, a crash
        restart, a promotion with rollbacks and another crash restart.
        """
		with open(master.pg_log_file, 'r') as log_file:
			self.assertIn('orioledb rewind ended', log_file.read())
		self.assert_same_tables(master, replica, tables)
		self.check_tbl(replica)
		self.check_horizons_order(master)
		self.check_undo_order(master)

		self.horizon_workload(replica, 100, 'after', rollback_every=7)
		self.catchup_orioledb(master)
		master.safe_psql("CHECKPOINT")
		self.assert_same_tables(master, replica, tables)
		self.check_horizons_order(master)
		self.check_undo_order(master)
		self.assertLessEqual(self.xid_meta(master)['nextxid'],
		                     self.xid_meta(replica)['nextxid'])

		master.stop(['-m', 'immediate'])
		master.start()
		self.horizon_workload(replica, 50, 'crash')
		self.catchup_orioledb(master)
		self.assert_same_tables(master, replica, tables)
		self.check_horizons_order(master)
		self.check_undo_order(master)

		# The rewound node becomes the primary: its undo log and oxids must
		# be usable, rollbacks included
		master.promote()
		self.horizon_workload(master, 150, 'promoted', rollback_every=5)
		self.check_tbl(master)
		self.check_horizons_order(master)
		self.check_undo_order(master)
		digest = self.table_digest(master, 'o_test')
		master.stop(['-m', 'immediate'])
		master.start()
		self.assertEqual(self.table_digest(master, 'o_test'), digest)
		self.check_tbl(master)
		self.check_horizons_order(master)
		self.check_undo_order(master)
		self.check_undo_order(replica)

	def test_pg_rewind_state_after_rewind(self):
		"""
        The target runs many transactions and checkpoints past the divergence
        point, so its undo log holds records of oxids the source reuses.  A
        transaction in progress at the divergence point commits on the
        target.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:

				def diverge():
					self.horizon_workload(master, 200, 'master')
					self.horizon_workload(replica, 120, 'replica')

				self.rewind_in_progress_trx(
				    master, replica,
				    self.create_table_sql('o_test', 1000) +
				    "CREATE INDEX o_test_val_idx ON o_test (val);",
				    "UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;",
				    before_rewind=diverge)
				self.exercise_rewound(master, replica)

	def check_horizons(self, master_work, replica_work):
		# master_work and replica_work are what each node does after the
		# divergence point: a number of transactions on an OrioleDB table and
		# on a heap table
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:

				def work(spec):

					def run(node):
						orioledb_count, heap_count, tag, *rest = spec
						# Transactions on a temporary table take oxids and log
						# nothing: the first oxid in the node's WAL is above
						# the ones it used
						if rest:
							before = self.xid_meta(node)['nextxid']
							con = node.connect()
							con.execute(
							    "CREATE TEMP TABLE t_silent "
							    "(id integer PRIMARY KEY) USING orioledb")
							for i in range(rest[0]):
								con.begin()
								con.execute(
								    f"INSERT INTO t_silent VALUES ({i})")
								con.commit()
							con.close()
							self.assertGreaterEqual(
							    self.xid_meta(node)['nextxid'] - before,
							    rest[0])
						# Some transactions roll back: a rolled back one must
						# not turn into a committed one below the xid floor
						if orioledb_count:
							self.horizon_workload(node,
							                      orioledb_count,
							                      tag,
							                      rollback_every=4)
						if heap_count:
							self.horizon_workload(node,
							                      heap_count,
							                      tag,
							                      table='h_test',
							                      rollback_every=4)
						node.safe_psql("CHECKPOINT")

					return run

				self.rewind_diverged(
				    master, replica,
				    self.create_table_sql('o_test', 1000) +
				    self.create_table_sql('h_test', 1000, 'heap') +
				    "CREATE INDEX o_test_val_idx ON o_test (val);",
				    work(replica_work), work(master_work))
				self.exercise_rewound(master, replica, ('o_test', 'h_test'))

	def test_pg_rewind_horizons_target_busy_source_idle(self):
		"""
        The source has no transactions after the divergence point, it only
        starts them after the rewind, from oxids the busy target passed.
        """
		self.check_horizons((300, 0, 'master'), (0, 0, 'replica'))

	def test_pg_rewind_horizons_target_idle_source_busy(self):
		"""The target has no OrioleDB transactions to take the xid floor from."""
		self.check_horizons((0, 0, 'master'), (300, 0, 'replica'))

	def test_pg_rewind_horizons_both_idle(self):
		self.check_horizons((0, 0, 'master'), (0, 0, 'replica'))

	def test_pg_rewind_horizons_target_heap_only(self):
		"""The target changes heap only, the source OrioleDB."""
		self.check_horizons((0, 200, 'master'), (200, 0, 'replica'))

	def test_pg_rewind_horizons_source_heap_only(self):
		"""The source changes heap only, the target OrioleDB."""
		self.check_horizons((200, 0, 'master'), (0, 200, 'replica'))

	def test_pg_rewind_horizons_source_busier(self):
		"""The source passes the oxids the target used by far."""
		self.check_horizons((20, 0, 'master'), (400, 0, 'replica'))

	def test_pg_rewind_horizons_target_silent_oxids(self):
		"""
        The target takes oxids for transactions that log nothing before its
        first change: the first oxid in its WAL is above the first one the
        source uses.
        """
		self.check_horizons((20, 0, 'master', 100), (100, 0, 'replica'))

	def test_pg_rewind_horizons_target_unflushed_oxid(self):
		"""
        The target has a transaction in progress that changed a row and never
        reached WAL, so the source never sees its oxid.  The source's first
        oxid after the promotion is the oxid the target used for it; the
        target aborts it when it is shut down for the rewind, and the first
        oxid in its WAL is above that one.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    SELECT pg_create_physical_replication_slot('replica');
                """)
				self.run_sql(master, self.create_table_sql('o_test', 1000))
				replica.append_conf(primary_slot_name='replica')
				replica.start()
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				open_con = master.connect()
				open_con.begin()
				open_con.execute(
				    "INSERT INTO o_test VALUES (5000, 'open', 'x')")
				replica.promote()
				replica.safe_psql("CHECKPOINT;")

				# Rollbacks on the source: one below the target's floor must
				# stay rolled back
				self.horizon_workload(replica, 60, 'replica', rollback_every=3)
				self.horizon_workload(master, 60, 'master')
				self.rewind_and_follow(master, replica)
				try:
					open_con.close()
				except Exception:
					pass
				self.exercise_rewound(master, replica)

	def test_pg_rewind_horizons_target_crashed_oxid(self):
		"""
        The target crashes with a transaction in progress that never reached
        WAL, and is restarted and shut down cleanly for the rewind.  The
        oxid the transaction took is in no WAL at all, neither as a change
        nor as an abort.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    SELECT pg_create_physical_replication_slot('replica');
                """)
				self.run_sql(master, self.create_table_sql('o_test', 1000))
				replica.append_conf(primary_slot_name='replica')
				replica.start()
				master.safe_psql("CHECKPOINT;")
				self.catchup_orioledb(replica)

				open_con = master.connect()
				open_con.begin()
				open_con.execute(
				    "INSERT INTO o_test VALUES (5000, 'open', 'x')")
				replica.promote()
				replica.safe_psql("CHECKPOINT;")

				master.stop(['-m', 'immediate'])
				master.start()
				self.horizon_workload(replica, 60, 'replica', rollback_every=3)
				self.horizon_workload(master, 60, 'master')
				self.rewind_and_follow(master, replica)
				self.exercise_rewound(master, replica)

	def test_pg_rewind_undo_after_rewind(self):
		"""
        Both nodes run transactions with far more undo than the undo buffers
        hold, so the undo log is spilled to files, and the target commits
        one past the divergence point.  The undo locations the rewound node
        reserves and retains must stay in order, and the node keeps working.
        """
		with self.node as master:
			self.enable_archive(master)
			master.append_conf("orioledb.undo_buffers = 2MB")
			master.start()
			with self.getReplica() as replica:

				def diverge():
					self.big_undo_trx(master, 8, 'master')
					self.big_undo_trx(replica, 8, 'replica')
					self.check_undo_order(master)
					self.check_undo_order(replica)
					# The undo log outgrew its buffer and was written out
					for node in (master, replica):
						used, written = node.execute("""
                            SELECT lastusedlocation, writtenlocation
                              FROM orioledb_get_undo_meta()
                             WHERE undo_type = 'row'
                        """)[0]
						self.assertGreater(used, 4 * 1024 * 1024)
						self.assertGreater(written, 0)

				self.rewind_in_progress_trx(
				    master, replica,
				    "CREATE TABLE o_test (id integer NOT NULL PRIMARY KEY, "
				    "val text, pad text) USING orioledb;"
				    "INSERT INTO o_test SELECT id, 'orig', repeat('x', 500) "
				    "FROM generate_series(1, 3000) id;",
				    "UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;",
				    before_rewind=diverge)
				self.exercise_rewound(master, replica)

	def test_pg_rewind_undo_snapshot_on_rewound_standby(self):
		"""
        A snapshot on the rewound standby needs the old versions of the rows
        the source rewrites later.  The undo for them is written by the
        rewound node's own recovery, past the undo location it had at the
        rewind, and the small undo buffer pushes it out to files while the
        snapshot is still open.  The snapshot must keep seeing its rows.
        """
		query = "SELECT count(*), sum(hashtext(t::text)) FROM o_test t"
		with self.node as master:
			self.enable_archive(master)
			master.append_conf("orioledb.undo_buffers = 2MB")
			master.start()
			with self.getReplica() as replica:

				def diverge():
					self.big_undo_trx(master, 4, 'master')
					self.big_undo_trx(replica, 4, 'replica')

				self.rewind_in_progress_trx(
				    master, replica,
				    "CREATE TABLE o_test (id integer NOT NULL PRIMARY KEY, "
				    "val text, pad text) USING orioledb;"
				    "INSERT INTO o_test SELECT id, 'orig', repeat('x', 500) "
				    "FROM generate_series(1, 3000) id;",
				    "UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;",
				    before_rewind=diverge)
				self.assert_same_tables(master, replica)

				reader = master.connect()
				reader.begin('repeatable read')
				before = reader.execute(query)[0]
				written = self.undo_written(master)

				# Far more undo than the buffer holds is replayed under the
				# open snapshot
				self.big_undo_trx(replica, 8, 'late')
				self.catchup_orioledb(master)
				self.assertGreater(self.undo_written(master), written)
				undo_dir = os.path.join(master.data_dir, 'orioledb_undo')
				self.assertTrue(
				    any(
				        os.path.getsize(os.path.join(undo_dir, name)) > 0
				        for name in os.listdir(undo_dir)))
				self.check_undo_order(master)

				self.assertEqual(reader.execute(query)[0], before)
				reader.commit()
				reader.close()

				# The snapshot was not just looking at unchanged data
				self.assertNotEqual(master.execute(query)[0], before)
				self.assert_same_tables(master, replica)
				master.safe_psql("CHECKPOINT")
				self.check_undo_order(master)
				self.exercise_rewound(master, replica)

	def test_pg_rewind_stale_undo_is_reclaimed(self):
		"""
        The rewind leaves the target's own undo files and undo locations as
        they are, and the undo the rewound node writes next goes after them,
        not over them.  The undo of the target's divergent history is of no
        use to the rewound node: the files holding it must go away through
        the usual cleanup once the node has moved on.
        """
		undo_file_size = 0x4000000

		def undo_files(node):
			undo_dir = os.path.join(node.data_dir, 'orioledb_undo')
			files = {}
			for name in os.listdir(undo_dir):
				match = re.fullmatch(r'[0-9A-F]{2}([0-9A-F]{8})row', name)
				if match:
					files[name] = int(match.group(1), 16)
			return files

		with self.node as master:
			self.enable_archive(master)
			master.append_conf("orioledb.undo_buffers = 2MB")
			master.start()
			with self.getReplica() as replica:
				state = {}

				def diverge():
					# More than a few undo files of the target's own undo
					for _ in range(7):
						self.big_undo_trx(master, 8, 'master')
					self.big_undo_trx(replica, 8, 'replica')
					master.safe_psql("CHECKPOINT")
					state['files'] = undo_files(master)
					state['last_used'] = master.execute("""
                        SELECT lastusedlocation FROM orioledb_get_undo_meta()
                         WHERE undo_type = 'row'
                    """)[0][0]

				self.rewind_in_progress_trx(
				    master, replica,
				    "CREATE TABLE o_test (id integer NOT NULL PRIMARY KEY, "
				    "val text, pad text) USING orioledb;"
				    "INSERT INTO o_test SELECT id, 'orig', repeat('x', 500) "
				    "FROM generate_series(1, 3000) id;",
				    "UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;",
				    before_rewind=diverge)

				# The rewound node moves past the undo it kept
				for _ in range(9):
					self.big_undo_trx(replica, 8, 'late')
					self.catchup_orioledb(master)
					master.safe_psql("CHECKPOINT")
				last_used = state['last_used']
				master.poll_query_until(
				    f"""
                    SELECT cleanedlocation > {last_used}
                      FROM orioledb_get_undo_meta()
                     WHERE undo_type = 'row'
                """,
				    expected=True)
				cleaned = master.execute("""
                    SELECT cleanedlocation FROM orioledb_get_undo_meta()
                     WHERE undo_type = 'row'
                """)[0][0]

				# The files of the divergent history that are behind the
				# cleaned location are gone
				stale = [
				    name for name, number in state['files'].items()
				    if (number + 1) * undo_file_size <= cleaned
				]
				self.assertTrue(stale, f"{state['files']} cleaned {cleaned}")
				remaining = undo_files(master)
				self.assertEqual([name for name in stale if name in remaining],
				                 [], f"{remaining}")
				self.check_undo_order(master)
				self.assert_same_tables(master, replica)
				self.exercise_rewound(master, replica)

	def test_pg_rewind_parallel_recovery_workers(self):
		"""
        Both nodes run recovery with parallel workers: the promoted source
        captures the keys in several processes, and the rewound target
        replays the source's WAL through them.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				# pg_rewind copies the source's configuration to the target
				replica.append_conf("orioledb.recovery_pool_size = 4\n"
				                    "orioledb.recovery_idx_pool_size = 1")

				def diverge():
					self.horizon_workload(master, 150, 'master')
					self.horizon_workload(replica, 100, 'replica')

				self.rewind_in_progress_trx(
				    master, replica,
				    self.create_table_sql('o_test', 1000) +
				    "CREATE INDEX o_test_val_idx ON o_test (val);",
				    "UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;",
				    before_rewind=diverge)
				for node in (master, replica):
					self.assertEqual(
					    node.execute("SHOW orioledb.recovery_pool_size"),
					    [('4', )])
				self.exercise_rewound(master, replica)

	def test_pg_rewind_horizons_rewind_twice(self):
		"""
        The rewound node is promoted and diverges from its old source, which
        is then rewound onto it: the xid horizons seeded by the first rewind
        must hold for the second one.
        """
		with self.node as master:
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:

				def diverge():
					self.horizon_workload(master, 150, 'master')
					self.horizon_workload(replica, 100, 'replica')

				self.rewind_in_progress_trx(
				    master, replica,
				    self.create_table_sql('o_test', 1000) +
				    "CREATE INDEX o_test_val_idx ON o_test (val);",
				    "UPDATE o_test SET val = 'trx' WHERE mod(id, 10) = 1;",
				    before_rewind=diverge)
				self.exercise_rewound(master, replica)

				# The old source runs past the promoted master, then it is
				# the one that is rewound
				self.horizon_workload(replica, 80, 'old_source')
				self.horizon_workload(master, 60, 'new_primary')
				self.rewind_and_follow(replica, master)
				with open(replica.pg_log_file, 'r') as log_file:
					self.assertIn('orioledb rewind ended', log_file.read())
				self.assert_same_tables(replica, master)
				self.horizon_workload(master, 50, 'again')
				self.catchup_orioledb(replica)
				self.assert_same_tables(replica, master)
				self.check_tbl(master)

	def test_pg_rewind_keys_removed_with_wal(self):
		"""
        The keys of a promotion are WAL: without WAL archiving they go away
        together with the WAL around its switchpoint.
        """
		with self.node as master:
			master.start()
			with self.getReplica() as replica:
				master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    CREATE TABLE h_test (id int);
                """)
				replica.append_conf(wal_keep_size='0')
				replica.start()
				self.catchup_orioledb(replica)
				replica.promote()
				self.assertEqual(self.divergence_keys(replica), b'')

				for i in range(4):
					replica.safe_psql("INSERT INTO h_test VALUES (1);")
					replica.safe_psql("SELECT pg_switch_wal();")
					replica.safe_psql("CHECKPOINT;")
				with self.assertRaises(psycopg2.Error):
					self.divergence_keys(replica)

	def test_pg_rewind_large_divergence(self):
		"""
        The rewind deletes and inserts again about 3000 rows.  Small buffers
        and a short bgwriter_delay make the bgwriter advance the undo
        locations often meanwhile, which must not remove undo records the
        rewind still needs.
        """
		with self.node as master:
			master.append_conf(
			    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
			    "bgwriter_delay = 10ms\n")
			self.enable_archive(master)
			master.start()
			with self.getReplica() as replica:
				replica_sql, master_sql = self.divergence_sql('o_test')
				self.rewind_diverged(master, replica,
				                     self.create_table_sql('o_test', 100000),
				                     replica_sql, master_sql)
				self.assertEqual(self.table_digest(master, 'o_test'),
				                 self.table_digest(replica, 'o_test'))
				replica.stop()
				master.promote()
				# Check the table on a quiet server: the restart also makes a
				# checkpoint, after which the free extents are consistent.
				self.run_sql(master, [
				    "ALTER SYSTEM SET orioledb.main_buffers = '64MB';",
				    "ALTER SYSTEM SET bgwriter_delay = '200ms';"
				])
				master.restart()
				with master.connect() as con:
					self.assertTrue(
					    con.execute("""
                            SELECT orioledb_tbl_check('o_test'::regclass, true)
                        """)[0][0])
					notices = [
					    n for n in con.connection.notices
					    if n.startswith('NOTICE')
					]
					self.assertEqual(notices, [])

	def test_pg_rewind_in_progress_target_trx_undo(self):
		with self.node as master:
			self.enable_archive(master)
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

				replica.append_conf(primary_slot_name='replica')
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
				self.pg_rewind_master(master, replica.port, master_slot)
				self.start_master_as_standby(master, replica, primary_conninfo,
				                             master_slot)
				self.assertEqual(
				    master.execute("SELECT COUNT(*) FROM test_table")[0][0], 5)

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

				replica.append_conf(primary_slot_name='replica')
				replica.start()
				self.catchup_orioledb(replica)

				con1 = master.connect()
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
				con1.execute("SELECT orioledb_flush_local_wal()")

				self.catchup_orioledb(replica)
				self.assertEqual(
				    replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
				    5)

				con1.commit()
				self.catchup_orioledb(replica)
				self.assertEqual(
				    replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
				    13)

	def test_pg_rewind_in_progress_replica_trx(self):
		with self.node as master:
			self.enable_archive(master)
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

				replica.append_conf(primary_slot_name='replica')
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
				self.pg_rewind_master(master,
				                      replica.port,
				                      master_slot,
				                      verbose=True,
				                      rewind_log_file=master.pg_log_file)
				self.start_master_as_standby(master, replica, primary_conninfo,
				                             master_slot)
				con1.execute("SELECT orioledb_flush_local_wal()")
				con2 = replica.connect()
				self.catchup_orioledb(master)
				self.assertEqual(
				    replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
				    5)
				self.assertEqual(
				    master.execute("SELECT COUNT(*) FROM test_table")[0][0], 5)
				con1.execute("""ROLLBACK TO SAVEPOINT s2;""")
				con1.execute("""DELETE FROM test_table WHERE x = 4""")
				con1.commit()
				con1.close()
				self.catchup_orioledb(master)
				self.assertEqual(
				    master.execute("SELECT COUNT(*) FROM test_table")[0][0],
				    12)
				self.assertEqual(
				    replica.execute("SELECT COUNT(*) FROM test_table")[0][0],
				    12)

	def test_pg_rewind_toast(self):
		with self.node as master:
			self.enable_archive(master)
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

				replica.append_conf(primary_slot_name='replica')
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
				self.pg_rewind_master(master, replica.port, master_slot)
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

	class _PgRewindTest:

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

	class _SecondaryIndexRevivalTest(_PgRewindTest):

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
			return {
			    'tables': {
			        '+': ['test1']
			    },
			    'indices': {
			        '+': ['test1_pkey', 'test1_ix1', 'toast']
			    }
			}

		def after_promote(self):
			self.master.safe_psql("""
                DROP INDEX test1_ix1;
            """)

			self.replica.safe_psql("""
                INSERT INTO test1 (x, y)
                    SELECT id, (id + 2) % 5 + 1
                        FROM generate_series(6, 10) id;
            """)

			return {'master': {'indices': {'-': ['test1_ix1']}}}

		def before_rewind(self):
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1 ORDER BY x"""),
			    [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3)])
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1 ORDER BY y"""),
			    [(3, 1), (4, 2), (5, 3), (1, 4), (2, 5)])
			self.test_case.assertEqual(
			    self.replica.execute("""
                    SELECT * FROM test1 ORDER BY x"""),
			    [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3), (6, 4), (7, 5),
			     (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
			    self.replica.execute("""
                    SELECT * FROM test1 ORDER BY y, x"""), [(3, 1), (8, 1),
			                                                (4, 2), (9, 2),
			                                                (5, 3), (10, 3),
			                                                (1, 4), (6, 4),
			                                                (2, 5), (7, 5)])

		def after_rewind(self):
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1 ORDER BY x"""),
			    [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3), (6, 4), (7, 5),
			     (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1 ORDER BY y, x"""), [(3, 1), (8, 1),
			                                                (4, 2), (9, 2),
			                                                (5, 3), (10, 3),
			                                                (1, 4), (6, 4),
			                                                (2, 5), (7, 5)])

	class _PrimaryIndexRevivalTest(_PgRewindTest):

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
			return {
			    'tables': {
			        '+': ['test1b']
			    },
			    'indices': {
			        '+': ['test1b_pkey', 'test1b_ix1', 'toast']
			    }
			}

		def after_promote(self):
			self.master.safe_psql("""
                ALTER TABLE test1b DROP CONSTRAINT test1b_pkey;
            """)

			self.replica.safe_psql("""
                INSERT INTO test1b (x, y)
                    SELECT id, (id + 2) % 5 + 1
                        FROM generate_series(6, 10) id;
            """)

			return {
			    'master': {
			        'indices': {
			            '-': ['test1b_pkey'],
			            '+': ['ctid_primary']
			        }
			    }
			}

		def before_rewind(self):
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1b ORDER BY x"""),
			    [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3)])
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1b ORDER BY y"""),
			    [(3, 1), (4, 2), (5, 3), (1, 4), (2, 5)])
			self.test_case.assertEqual(
			    self.replica.execute("""
                    SELECT * FROM test1b ORDER BY x"""),
			    [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3), (6, 4), (7, 5),
			     (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
			    self.replica.execute("""
                    SELECT * FROM test1b ORDER BY y, x"""), [(3, 1), (8, 1),
			                                                 (4, 2), (9, 2),
			                                                 (5, 3), (10, 3),
			                                                 (1, 4), (6, 4),
			                                                 (2, 5), (7, 5)])

		def after_rewind(self):
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1b ORDER BY x"""),
			    [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3), (6, 4), (7, 5),
			     (8, 1), (9, 2), (10, 3)])
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test1b ORDER BY y, x"""), [(3, 1), (8, 1),
			                                                 (4, 2), (9, 2),
			                                                 (5, 3), (10, 3),
			                                                 (1, 4), (6, 4),
			                                                 (2, 5), (7, 5)])

	class _TableRevivalTest(_PgRewindTest):

		def before_promote(self):
			self.master.safe_psql("""
                CREATE TABLE test1a (
                    x integer NOT NULL PRIMARY KEY,
                    y integer
                ) USING orioledb;
                INSERT INTO test1a (x, y)
                    SELECT id, 10 FROM generate_series(1, 5) id;
            """)
			return {
			    'tables': {
			        '+': ['test1a']
			    },
			    'indices': {
			        '+': ['test1a_pkey', 'toast']
			    }
			}

		def after_promote(self):
			self.master.safe_psql("""
                DROP TABLE test1a;
            """)
			return {
			    'master': {
			        'tables': {
			            '-': ['test1a']
			        },
			        'indices': {
			            '-': ['test1a_pkey', 'toast']
			        }
			    }
			}

		def after_rewind(self):
			self.test_case.assertEqual(
			    self.master.execute("SELECT COUNT(*) FROM test1a")[0][0], 5)

	class _RewindSysTreesInProgressTest(_PgRewindTest):

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
			return {
			    'tables': {
			        '+': ['test2']
			    },
			    'indices': {
			        '+': ['test2_pkey', 'toast']
			    }
			}

		def after_promote(self):
			replica_changes = {
			    'tables': {
			        '+': ['test2', 'test2', 'test2', 'test2'],
			        '-': []
			    },
			    'indices': {
			        '+': [
			            'ctid_primary', 'ctid_primary', 'ctid_primary',
			            'test2_renamed', 'test2_renamed', 'toast', 'toast',
			            'toast', 'toast'
			        ],
			        '-': ['test2_pkey']
			    }
			}
			self.master.safe_psql("""
                DROP TABLE test2;
            """)
			master_changes = {
			    'tables': {
			        '-': ['test2']
			    },
			    'indices': {
			        '-': ['test2_pkey', 'toast']
			    }
			}
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
			con1.execute("SELECT orioledb_flush_local_wal()")
			return {'master': master_changes, 'replica': replica_changes}

		def after_rewind(self):
			self.con1.rollback()
			self.test_case.catchup_orioledb(self.master)
			self.test_case.assertEqual(
			    self.master.execute("SELECT COUNT(*) FROM test2")[0][0], 5)
			return {}

	class _ReplicaTableDropTest(_PgRewindTest):

		def before_promote(self):
			self.master.safe_psql("""
                CREATE TABLE test2a (
                    x integer NOT NULL PRIMARY KEY,
                    y integer
                ) USING orioledb;
                INSERT INTO test2a (x, y)
                    SELECT id, 20 FROM generate_series(1, 5) id;
            """)
			return {
			    'tables': {
			        '+': ['test2a']
			    },
			    'indices': {
			        '+': ['test2a_pkey', 'toast']
			    }
			}

		def after_promote(self):
			self.replica.safe_psql("""
                DROP TABLE test2a;
            """)
			return {
			    'replica': {
			        'tables': {
			            '-': ['test2a']
			        },
			        'indices': {
			            '-': ['test2a_pkey', 'toast']
			        }
			    }
			}

	class _NewTablesOnTargetAfterPromoteTest(_PgRewindTest):

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
			    self.master.execute("SELECT COUNT(*) FROM test3")[0][0], 5)
			self.test_case.assertEqual(
			    self.master.execute("SELECT COUNT(*) FROM test4")[0][0], 5)
			return {
			    'master': {
			        'tables': {
			            '+': ['test3', 'test4']
			        },
			        'indices': {
			            '+': ['ctid_primary', 'test4_pkey', 'toast', 'toast']
			        }
			    }
			}

	class _ToastAfterSysTreeRevivalTest(_PgRewindTest):

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

			return {
			    'tables': {
			        '+': ['test5']
			    },
			    'indices': {
			        '+': ['test5_pkey', 'toast']
			    }
			}

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
                    SELECT * FROM test5 ORDER BY x"""), self.master_rows)
			self.test_case.assertEqual(
			    self.replica.execute("""
                    SELECT * FROM test5 ORDER BY x"""), self.replica_rows)

		def after_rewind(self):
			self.test_case.assertEqual(
			    self.master.execute("""
                    SELECT * FROM test5 ORDER BY x"""), self.replica_rows)

	def _process_changes(self, changes, master_changes, replica_changes):
		if isinstance(changes, dict):
			if 'master' in changes:
				self._merge_dicts(master_changes, changes['master'])
			if 'replica' in changes:
				self._merge_dicts(replica_changes, changes['replica'])

		master_tables = [(x, ) for x in self._substract_lists(
		    master_changes["tables"]['+'], master_changes["tables"]['-'])]
		master_indices = [(x, ) for x in self._substract_lists(
		    master_changes["indices"]['+'], master_changes["indices"]['-'])]

		replica_tables = [(x, ) for x in self._substract_lists(
		    replica_changes["tables"]['+'], replica_changes["tables"]['-'])]
		replica_indices = [(x, ) for x in self._substract_lists(
		    replica_changes["indices"]['+'], replica_changes["indices"]['-'])]
		return [master_tables, master_indices, replica_tables, replica_indices]

	def test_pg_rewind_sys_trees(self):
		master_changes = {
		    'tables': {
		        '+': [],
		        '-': []
		    },
		    'indices': {
		        '+': [],
		        '-': []
		    }
		}
		replica_changes = {
		    'tables': {
		        '+': [],
		        '-': []
		    },
		    'indices': {
		        '+': [],
		        '-': []
		    }
		}

		with self.node as master:
			self.enable_archive(master)
			master.start()

			with self.getReplica() as replica:
				tests: List[self._RewindTest] = [
				    self._SecondaryIndexRevivalTest(self),
				    self._PrimaryIndexRevivalTest(self),
				    self._TableRevivalTest(self),
				    # self._RewindSysTreesInProgressTest(self),
				    # self._ReplicaTableDropTest(self),
				    # self._NewTablesOnTargetAfterPromoteTest(self),
				    # self._ToastAfterSysTreeRevivalTest(self)
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

				replica.append_conf(primary_slot_name='replica')
				replica.start()

				replica_changes = copy.deepcopy(master_changes)
				self.catchup_orioledb(replica)
				replica.promote()
				replica.safe_psql("CHECKPOINT;")

				for test in tests:
					with test.subtest():
						changes = test.after_promote()
						new_changes = self._process_changes(
						    changes, master_changes, replica_changes)
						(master_tables, master_indices, replica_tables,
						 replica_indices) = new_changes

				self.assertEqual(
				    master.execute("""
                        SELECT relname FROM db_o_tables
                            ORDER BY relname COLLATE "C"
                    """), master_tables)
				self.assertEqual(
				    master.execute("""
                        SELECT name FROM orioledb_index
                            ORDER BY name COLLATE "C"
                    """), master_indices)

				self.assertEqual(
				    replica.execute("""
                        SELECT relname FROM db_o_tables
                            ORDER BY relname COLLATE "C"
                    """), replica_tables)
				self.assertEqual(
				    replica.execute("""
                        SELECT name FROM orioledb_index
                            ORDER BY name COLLATE "C"
                    """), replica_indices)

				for test in tests:
					with test.subtest():
						changes = test.before_rewind()
						new_changes = self._process_changes(
						    changes, master_changes, replica_changes)
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
				self.pg_rewind_master(master,
				                      replica.port,
				                      master_slot,
				                      verbose=True,
				                      rewind_log_file=master.pg_log_file)
				self.start_master_as_standby(master, replica, primary_conninfo,
				                             master_slot)
				for test in tests:
					with test.subtest():
						changes = test.after_rewind()
						new_changes = self._process_changes(
						    changes, master_changes, replica_changes)
						(master_tables, master_indices, replica_tables,
						 replica_indices) = new_changes
				self.catchup_orioledb(master)

				self.assertEqual(
				    master.execute("""
                        SELECT relname FROM db_o_tables
                            ORDER BY relname COLLATE "C"
                    """), replica_tables)
				self.assertEqual(
				    master.execute("""
                        SELECT name FROM orioledb_index
                            ORDER BY name COLLATE "C"
                    """), replica_indices)
