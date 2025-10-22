#!/usr/bin/env python3
# coding: utf-8

import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException


def init(node, tablename):
    node.safe_psql('postgres', f"""

        CREATE EXTENSION orioledb;

        CREATE TABLE {tablename} (a int PRIMARY KEY, b text) USING orioledb;

        CREATE OR REPLACE FUNCTION check_xact()
        RETURNS TEXT AS $$
        DECLARE
            output TEXT;
        BEGIN
            SELECT FORMAT('%I/%I', h, o) INTO output FROM
                orioledb_get_current_heap_xid() as h,
                orioledb_get_current_logical_xid() as o;
            RETURN output;
        END;
        $$ LANGUAGE plpgsql;
    """)


class LogicalXidSubxactsTest(BaseTest):

    tablename = "o_test0"
    newtable = "o_test1"
    check_xact = "SELECT check_xact();"

    # top xact: readonly
    # sub xact: heap write
    def test_top_ro_sub_hwr(self):
        node = self.node
        node.start()
        init(node, self.tablename)

        con = node.connect()
        con.begin()

        output = con.execute(f"""
            {self.check_xact}
            SELECT COUNT(*) FROM {self.tablename};
            {self.check_xact}
            SAVEPOINT sp1;
            {self.check_xact}
            CREATE TABLE {self.newtable} (a int PRIMARY KEY, b text);
            {self.check_xact}
        """)

        self.assertEqual(output[0][0], '"0"/"0"')
        self.assertEqual(output[2][0], '"0"/"0"')
        self.assertEqual(output[3][0], '"0"/"0"')
        self.assertEqual(output[4][0], '"740"/"0"')

        con.commit()


    # top xact: readonly
    # sub xact: oriole write
    def test_top_ro_sub_owr(self):
        node = self.node
        node.start()
        init(node, self.tablename)

        con = node.connect()
        con.begin()

        output = con.execute(f"""
            {self.check_xact}
            SELECT COUNT(*) FROM {self.tablename};
            {self.check_xact}
            SAVEPOINT sp1;
            {self.check_xact}
            INSERT INTO {self.tablename} (a, b) VALUES (1, 'one'), (2, 'two');
            {self.check_xact}
        """)

        self.assertEqual(output[0][0], '"0"/"0"')
        self.assertEqual(output[2][0], '"0"/"0"')
        self.assertEqual(output[3][0], '"0"/"0"')
        self.assertEqual(output[4][0], '"0"/"32"')

        con.commit()


    # top xact: readonly
    # sub xact: heap->oriole write
    def test_top_ro_sub_howr(self):
        node = self.node
        node.start()
        init(node, self.tablename)

        con = node.connect()
        con.begin()

        output = con.execute(f"""
            {self.check_xact}
            SELECT COUNT(*) FROM {self.tablename};
            {self.check_xact}
            SAVEPOINT sp1;
            {self.check_xact}
            CREATE TABLE {self.newtable} (a int PRIMARY KEY, b text) USING orioledb;
            {self.check_xact}
        """)

        self.assertEqual(output[0][0], '"0"/"0"')
        self.assertEqual(output[2][0], '"0"/"0"')
        self.assertEqual(output[3][0], '"0"/"0"')
        self.assertEqual(output[4][0], '"740"/"740"')

        con.commit()


    # top xact: readonly
    # sub xact: heap->oriole write
    # sub xact: readonly
    def test_top_ro_sub_howr_ro(self):
        node = self.node
        node.start()
        init(node, self.tablename)

        con = node.connect()
        con.begin()

        output = con.execute(f"""
            {self.check_xact}
            SELECT COUNT(*) FROM {self.tablename};
            {self.check_xact}
            SAVEPOINT sp1;
            {self.check_xact}
            CREATE TABLE {self.newtable} (a int PRIMARY KEY, b text) USING orioledb;
            {self.check_xact}
            SAVEPOINT sp2;
            {self.check_xact}
            SELECT COUNT(*) FROM {self.tablename};
            {self.check_xact}
        """)

        self.assertEqual(output[0][0], '"0"/"0"')
        self.assertEqual(output[2][0], '"0"/"0"')
        self.assertEqual(output[3][0], '"0"/"0"')
        self.assertEqual(output[4][0], '"740"/"740"')
        self.assertEqual(output[5][0], '"0"/"32"')
        self.assertEqual(output[7][0], '"0"/"32"')

        con.commit()


    # top xact: readonly
    # sub xact: oriole->heap write
    def test_top_ro_sub_ohwr(self):
        node = self.node
        node.start()
        init(node, self.tablename)

        con = node.connect()
        con.begin()

        output = con.execute(f"""
            {self.check_xact}
            SELECT COUNT(*) FROM {self.tablename};
            {self.check_xact}
            SAVEPOINT sp1;
            {self.check_xact}
            INSERT INTO {self.tablename} (a, b) VALUES (1, 'one'), (2, 'two');
            {self.check_xact}
            CREATE TABLE {self.newtable} (a int PRIMARY KEY, b text);
            {self.check_xact}
        """)

        self.assertEqual(output[0][0], '"0"/"0"')
        self.assertEqual(output[2][0], '"0"/"0"')
        self.assertEqual(output[3][0], '"0"/"0"')
        self.assertEqual(output[4][0], '"0"/"32"')
        self.assertEqual(output[5][0], '"740"/"32"') # @TODO !!! switch xid !!!

        con.commit()