import copy
from itertools import chain
from collections import Counter
import os
from shutil import rmtree
import socket
import struct
import subprocess
import time
from tempfile import mkdtemp
from typing import List
from unittest import result
import unittest
import glob
import testgres
from testgres.defaults import default_dbname
from testgres.utils import file_tail, get_bin_path, options_string
from testgres.consts import PG_AUTO_CONF_FILE
from testgres.enums import NodeStatus
from sys import platform

from .base_test import BaseTest, generate_string

REWIND_KEYS_WARNING = "Target and source servers don't have the OrioleDB rewind keys"

class RewindTest(BaseTest):

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
            s.bind(("", 0))
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
    def rewind_keys_files(node):
        # Return all rewind_keys file names
        return glob.glob(
            os.path.join(node.data_dir, 'orioledb_data', 'rewind_keys_*'))
 
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
        for query in ([sql] if isinstance(sql, str) else sql):
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
                replica_sql, master_sql = self.divergence_sql('o_test', 'h_test')
                self.rewind_diverged(
                    master, replica,
                    self.create_table_sql('o_test', 10000) +
                    self.create_table_sql('h_test', 10000, 'heap'), replica_sql,
                    master_sql)
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
                self.assertEqual(result,
                                 {key: (1000, orig_rows, 500500)
                                  for key in result})
                for node in (replica, master):
                    con = node.connect()
                    con.execute("SET enable_seqscan = off")
                    for table in tables:
                        plan = con.execute(
                            f"EXPLAIN SELECT * FROM {table} WHERE val = 'orig'")
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

    def check_invalid_rewind_keys(self, failure):
        with self.node as master:
            self.enable_archive(master)
            master.start()
            with self.getReplica() as replica:
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
                replica.promote()
                replica.safe_psql("CHECKPOINT")
                con.commit()
                con.close()

                files = self.rewind_keys_files(replica)
                self.assertEqual(len(files), 1)
                if failure == 'checksum':
                    with open(files[0], 'r+b') as rewind_file:
                        rewind_file.seek(-1, os.SEEK_END)
                        byte = rewind_file.read(1)
                        rewind_file.seek(-1, os.SEEK_END)
                        rewind_file.write(bytes([byte[0] ^ 0xff]))
                elif failure == 'missing':
                    os.remove(files[0])

                replica.safe_psql(
                    "SELECT pg_create_physical_replication_slot('origin')")
                _, _, err = self.pg_rewind_master(master,
                                                   replica.port,
                                                   'origin',
                                                   expect_error=True)
                message = err.decode()
                if failure == 'checksum':
                    self.assertIn('checksum mismatch', message)
                elif failure == 'missing':
                    self.assertIn('do not have the OrioleDB rewind keys',
                                  message)

    def test_pg_rewind_missing_rewind_keys_fails(self):
        self.check_invalid_rewind_keys('missing')

    def test_pg_rewind_corrupt_rewind_keys_fails(self):
        self.check_invalid_rewind_keys('checksum')

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

                files = self.rewind_keys_files(replica)
                self.assertEqual(len(files), 1)
                with open(files[0], 'rb') as rewind_file:
                    header = rewind_file.read(36)
                magic, version, header_len, _, count, length, _ = \
                    struct.unpack('!IHHQQQI', header)
                self.assertEqual(magic, 0x4f524b32)
                self.assertEqual(version, 1)
                self.assertEqual(header_len, 36)
                self.assertEqual(count, 0)
                self.assertEqual(length, 0)

                replica.safe_psql("INSERT INTO o_test VALUES (2)")
                master.safe_psql("INSERT INTO o_test VALUES (3)")
                self.rewind_and_follow(master, replica)
                self.assertEqual(master.execute("SELECT * FROM o_test ORDER BY id"),
                                 [(1, ), (2, )])

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
                ], check=True)
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

                self.assertEqual(len(self.rewind_keys_files(replica)), 1)
                self.assertEqual(replica.execute("""
                    SELECT count(*), count(*) FILTER (WHERE val = 'orig')
                        FROM o_test
                """), [(100, 100)])

    def test_rewind_keys_crash_before_ready(self):
        self.check_rewind_keys_promotion_crash('rewind_keys_before_ready')

    def test_rewind_keys_crash_after_ready(self):
        self.check_rewind_keys_promotion_crash('rewind_keys_after_ready')

    def test_rewind_keys_crash_after_wal_flush(self):
        self.check_rewind_keys_promotion_crash(
            'rewind_keys_after_wal_flush')

    def test_rewind_keys_missing_part_stops_promotion(self):
        with self.node as master:
            self.enable_archive(master)
            master.start()
            with self.getReplica() as replica:
                replica.append_conf("orioledb.enable_stopevents = true\n"
                                    "orioledb.restart_after_crash = off")
                master.safe_psql("""
                    CREATE EXTENSION orioledb;
                    CREATE TABLE o_test (id integer PRIMARY KEY) USING orioledb;
                    INSERT INTO o_test SELECT generate_series(1, 100);
                    SELECT pg_create_physical_replication_slot('replica');
                """)
                replica.append_conf(primary_slot_name='replica')
                replica.start()
                self.catchup_orioledb(replica)

                con = master.connect()
                con.begin()
                con.execute("DELETE FROM o_test")
                con.execute("SELECT orioledb_flush_local_wal()")
                master.safe_psql("CHECKPOINT")
                self.catchup_orioledb(replica)
                replica.safe_psql(
                    "SELECT pg_stopevent_set('rewind_keys_before_ready', "
                    "'true')")
                subprocess.run([
                    get_bin_path('pg_ctl'), '-D', replica.data_dir, '-W',
                    'promote'
                ], check=True)
                replica.poll_query_until(
                    "SELECT coalesce(array_length(waiter_pids, 1), 0) > 0 "
                    "FROM pg_stopevents() "
                    "WHERE stopevent = 'rewind_keys_before_ready'",
                    expected=True)
                parts = glob.glob(os.path.join(replica.data_dir,
                                               'orioledb_data',
                                               'rewind_keys.*.part'))
                self.assertGreater(len(parts), 0)
                os.remove(parts[0])
                replica.safe_psql(
                    "SELECT pg_stopevent_reset('rewind_keys_before_ready')")
                for _ in range(100):
                    if replica.status() != NodeStatus.Running:
                        break
                    time.sleep(0.1)
                self.assertNotEqual(replica.status(), NodeStatus.Running)
                with open(replica.pg_log_file, 'r') as log_file:
                    self.assertIn('missing or invalid rewind keys part',
                                  log_file.read())
                con.rollback()
                con.close()

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

                # No timeline of the replica begins at its old switchpoint
                # anymore: its next restartpoint removes the keys
                master.safe_psql("CHECKPOINT;")
                self.catchup_orioledb(replica)
                replica.safe_psql("CHECKPOINT;")
                wal = os.path.join(replica.data_dir, 'pg_wal')
                self.assertEqual(self.rewind_keys_files(replica), [])

    def test_rewind_keys_removed_with_wal(self):
        """
        Without WAL archiving the keys of a promotion go away together with
        the WAL around its switchpoint: nobody could catch up from it.
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
                self.assertEqual(len(self.rewind_keys_files(replica)), 1)
                replica.safe_psql("CHECKPOINT;")
                self.assertEqual(len(self.rewind_keys_files(replica)), 1)

                for i in range(4):
                    replica.safe_psql("INSERT INTO h_test VALUES (1);")
                    replica.safe_psql("SELECT pg_switch_wal();")
                    replica.safe_psql("CHECKPOINT;")
                self.assertEqual(self.rewind_keys_files(replica), [])

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
                self.pg_rewind_master(master, replica.port, master_slot,
                                      verbose=True, rewind_log_file=master.pg_log_file)
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
                    SELECT * FROM test1 ORDER BY x"""), [(1, 4), (2, 5), (3, 1), (4, 2),
                                          (5, 3)])
            self.test_case.assertEqual(
                self.master.execute("""
                    SELECT * FROM test1 ORDER BY y"""), [(3, 1), (4, 2), (5, 3), (1, 4),
                                          (2, 5)])
            self.test_case.assertEqual(
                self.replica.execute("""
                    SELECT * FROM test1 ORDER BY x"""),
                [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3), (6, 4), (7, 5),
                 (8, 1), (9, 2), (10, 3)])
            self.test_case.assertEqual(
                self.replica.execute("""
                    SELECT * FROM test1 ORDER BY y, x"""), [(3, 1), (8, 1), (4, 2), (9, 2),
                                             (5, 3), (10, 3), (1, 4), (6, 4),
                                             (2, 5), (7, 5)])

        def after_rewind(self):
            self.test_case.assertEqual(
                self.master.execute("""
                    SELECT * FROM test1 ORDER BY x"""),
                [(1, 4), (2, 5), (3, 1), (4, 2), (5, 3), (6, 4), (7, 5),
                 (8, 1), (9, 2), (10, 3)])
            self.test_case.assertEqual(
                self.master.execute("""
                    SELECT * FROM test1 ORDER BY y, x"""), [(3, 1), (8, 1), (4, 2), (9, 2),
                                             (5, 3), (10, 3), (1, 4), (6, 4),
                                             (2, 5), (7, 5)])

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
                    SELECT * FROM test1b ORDER BY x"""), [(1, 4), (2, 5), (3, 1), (4, 2),
                                           (5, 3)])
            self.test_case.assertEqual(
                self.master.execute("""
                    SELECT * FROM test1b ORDER BY y"""), [(3, 1), (4, 2), (5, 3), (1, 4),
                                           (2, 5)])
            self.test_case.assertEqual(
                self.replica.execute("""
                    SELECT * FROM test1b ORDER BY x"""), [(1, 4), (2, 5), (3, 1), (4, 2),
                                           (5, 3), (6, 4), (7, 5), (8, 1),
                                           (9, 2), (10, 3)])
            self.test_case.assertEqual(
                self.replica.execute("""
                    SELECT * FROM test1b ORDER BY y, x"""), [(3, 1), (8, 1), (4, 2), (9, 2),
                                              (5, 3), (10, 3), (1, 4), (6, 4),
                                              (2, 5), (7, 5)])

        def after_rewind(self):
            self.test_case.assertEqual(
                self.master.execute("""
                    SELECT * FROM test1b ORDER BY x"""), [(1, 4), (2, 5), (3, 1), (4, 2),
                                           (5, 3), (6, 4), (7, 5), (8, 1),
                                           (9, 2), (10, 3)])
            self.test_case.assertEqual(
                self.master.execute("""
                    SELECT * FROM test1b ORDER BY y, x"""), [(3, 1), (8, 1), (4, 2), (9, 2),
                                              (5, 3), (10, 3), (1, 4), (6, 4),
                                              (2, 5), (7, 5)])

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
                    '+': ['ctid_primary', 'ctid_primary', 'ctid_primary',
                          'test2_renamed', 'test2_renamed',
                          'toast', 'toast', 'toast', 'toast'],
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
                self.pg_rewind_master(master, replica.port, master_slot,
                                      verbose=True, rewind_log_file=master.pg_log_file)
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
