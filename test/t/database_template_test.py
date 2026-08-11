#!usr/bin/env python3
# coding: utf-8

import os

from .base_test import BaseTest
from testgres.exceptions import QueryException

class DatabaseTemplateTest(BaseTest):

    TEMPLATE_DDL = """
        CREATE EXTENSION orioledb;

        CREATE TABLE o_tb_no_pk (
            k int NOT NULL,
            val text NOT NULL
        ) USING orioledb;
        CREATE INDEX o_tb_no_pk_idx ON o_tb_no_pk (k);
        INSERT INTO o_tb_no_pk VALUES (10, 'A'), (20, 'B');

        CREATE TABLE o_tb (
            id int PRIMARY KEY,
            val text NOT NULL
        ) USING orioledb;
        INSERT INTO o_tb VALUES (1, 'one'), (2, 'two');

        CREATE TABLE o_tb_secondary_k (
            id int PRIMARY KEY,
            k int NOT NULL,
            val text
        ) USING orioledb;
        CREATE INDEX o_tb_secondary_k_idx ON o_tb_secondary_k (k);
        INSERT INTO o_tb_secondary_k VALUES (10, 100, 'Val1'), (50, 500, 'Val2');

        CREATE TABLE o_tb_bridge (
            id int PRIMARY KEY,
            tag int
        ) USING orioledb;
        CREATE INDEX o_tb_bridge_idx ON o_tb_bridge USING btree (tag)
            WITH (orioledb_index = off, deduplicate_items = off);
        INSERT INTO o_tb_bridge VALUES (1, 11), (2, 22);

        CREATE TABLE o_tb_toast (
            id int PRIMARY KEY,
            t_key text NOT NULL,
            t_big text NOT NULL
        ) USING orioledb;
        CREATE INDEX o_tb_toast_idx ON o_tb_toast (t_key);
        INSERT INTO o_tb_toast VALUES
            (1, 'k1', repeat('x', 3000)),
            (2, 'k2', repeat('y', 3500));

        CREATE TABLE heap_table (
            id int PRIMARY KEY,
            k int NOT NULL
        ) USING heap;
        CREATE INDEX heap_table_idx ON heap_table (k);
        INSERT INTO heap_table VALUES (1, 10), (2, 20);
    """

    TEMPLATE_TABLESPACE_DDL = """
        CREATE TABLE o_tb_tblspc (
            id int PRIMARY KEY,
            n int NOT NULL
        ) USING orioledb TABLESPACE db_template_tblspc;
        CREATE INDEX o_tb_tblspc_idx ON o_tb_tblspc (n) TABLESPACE db_template_tblspc;
        INSERT INTO o_tb_tblspc VALUES (1, 111), (2, 222);
    """

    def setup_template_database(self, master):
        master.safe_psql("CREATE DATABASE orioledb_template;")
        master.safe_psql("orioledb_template", self.TEMPLATE_DDL)
        master.safe_psql("orioledb_template", "CREATE TABLESPACE db_template_tblspc LOCATION '';")
        master.safe_psql("orioledb_template", self.TEMPLATE_TABLESPACE_DDL)

    def createdb_from_template(self, master):
        master.safe_psql(
            "CREATE DATABASE orioledb TEMPLATE orioledb_template;")

    def check_orioledb_data(self, node, dbname):
        self.assertEqual(
            [(10, 'A'), (20, 'B')],
            node.execute(dbname, "SELECT k, val FROM o_tb_no_pk;"))
        self.assertEqual(
            [(1, 'one'), (2, 'two')],
            node.execute(dbname, "SELECT id, val FROM o_tb ORDER BY id;"))
        self.assertEqual(
            [(10, 100, 'Val1'), (50, 500, 'Val2')],
            node.execute(dbname, "SELECT * FROM o_tb_secondary_k;"))
        self.assertEqual(
            [(1, 11), (2, 22)],
            node.execute(dbname, "SELECT * FROM o_tb_bridge ORDER BY id;"))
        self.assertEqual(
            [(1, 'k1', 3000), (2, 'k2', 3500)],
            node.execute(dbname, "SELECT id, t_key, length(t_big) FROM o_tb_toast ORDER BY id;"))
        self.assertEqual(
            [(1, 10), (2, 20)],
            node.execute(dbname, "SELECT id, k FROM heap_table ORDER BY id;"))
        self.assertEqual(
            [(1, 111), (2, 222)],
            node.execute(dbname, "SELECT id, n FROM o_tb_tblspc ORDER BY id;"))

        with node.connect(dbname) as con:
            con.execute("SET enable_seqscan = off;")
            self.assertEqual(
                [(20, 'B')],
                con.execute("SELECT k, val FROM o_tb_no_pk WHERE k = 20;"))
            self.assertEqual(
                [(50, 'Val2')],
                con.execute("SELECT id, val FROM o_tb_secondary_k WHERE k = 500;"))
            self.assertEqual(
                [(1, 11)],
                con.execute("SELECT id, tag FROM o_tb_bridge WHERE tag = 11;"))
            self.assertEqual(
                [(2, 'k2', 3500)],
                con.execute("SELECT id, t_key, length(t_big) FROM o_tb_toast WHERE t_key = 'k2';"))
            self.assertEqual(
                [(2, 20)],
                con.execute("SELECT id, k FROM heap_table WHERE k = 20;"))
            self.assertEqual(
                [(2, 222)],
                con.execute("SELECT id, n FROM o_tb_tblspc WHERE n = 222;"))

    def orioledb_data_datoids(self, node):
        data_root = os.path.join(node.data_dir, "orioledb_data")
        if not os.path.isdir(data_root):
            return []
        return sorted(
            name for name in os.listdir(data_root)
            if os.path.isdir(os.path.join(data_root, name)))

    def test_create_database_template_failure_cleans_orioledb_data(self):
        with self.node as master:
            master.append_conf("orioledb.enable_stopevents = true\n")
            master.start()
            master.safe_psql("CREATE EXTENSION orioledb;")
            master.safe_psql("CREATE DATABASE orioledb_template;")
            master.safe_psql("orioledb_template", """
                CREATE EXTENSION orioledb;
                CREATE TABLE oriole_table (id int PRIMARY KEY) USING orioledb;
                INSERT INTO oriole_table VALUES (1);
            """)

            before_datoids = self.orioledb_data_datoids(master)
            master.safe_psql(
                "SELECT pg_stopevent_set('createdb_copy_fail', 'true');")
            with self.assertRaises(QueryException) as cm:
                master.safe_psql(
                    "CREATE DATABASE orioledb TEMPLATE orioledb_template;")
            self.assertIn("createdb copy failed", str(cm.exception))

            self.assertEqual([], master.execute(
                "SELECT 1 from pg_database WHERE datname = 'orioledb';"))
            self.assertEqual(before_datoids, self.orioledb_data_datoids(master))
        

    def test_create_database_template_on_master(self):
        with self.node as master:
            master.append_conf("allow_in_place_tablespaces = true\n")
            master.start()
            self.setup_template_database(master)
            self.createdb_from_template(master)
            self.check_orioledb_data(master, "orioledb_template")
            self.check_orioledb_data(master, "orioledb")

    def test_create_database_template_replication(self):
        with self.node as master:
            master.append_conf("allow_in_place_tablespaces = true\n")
            master.start()

            replica = self.getReplica()
            replica.append_conf("allow_in_place_tablespaces = true\n")
            with replica.start() as replica:
                master.safe_psql(
                    "CREATE EXTENSION IF NOT EXISTS orioledb;")

                self.setup_template_database(master)

                self.catchup_orioledb(replica)

                self.createdb_from_template(master)

                self.catchup_orioledb(replica)

                replica.poll_query_until(
                    "SELECT orioledb_has_retained_undo();", expected=False)

                self.check_orioledb_data(master, "orioledb_template")
                self.check_orioledb_data(replica, "orioledb_template")
                self.check_orioledb_data(master, "orioledb")
                self.check_orioledb_data(replica, "orioledb")