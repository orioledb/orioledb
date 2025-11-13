#!/usr/bin/env python3
# coding: utf-8

import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException


def equalZero(val):
	return val == 0


def aboveZero(val):
	return val > 0


class LogicalXidSubxactsTest(BaseTest):

	o_table = "o_test"
	h_table = "h_test"

	orioledb_get_current_heap_xid = "SELECT orioledb_get_current_heap_xid();"
	orioledb_get_current_logical_xid = "SELECT orioledb_get_current_logical_xid();"

	check_stmt = f"""
		{orioledb_get_current_heap_xid}
		{orioledb_get_current_logical_xid}
	"""
	heap_xid = 0
	oriole_xid = 1

	def run_check(self, con, stmt, cb):
		output = con.execute(f"""{stmt}""")
		#print(output)
		self.assertTrue(cb(output[0][0]))

	def check(self, con, on_heap, on_oriole):
		self.run_check(con, self.orioledb_get_current_heap_xid, on_heap)
		self.run_check(con, self.orioledb_get_current_logical_xid, on_oriole)

	def init(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', f"""
        		CREATE EXTENSION orioledb;
        		CREATE TABLE {self.o_table} (a int PRIMARY KEY, b text) USING orioledb;
    		""")

	# top xact: readonly
	# sub xact: heap write
	def test_top_ro_sub_hwr(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(
		    f"""CREATE TABLE {self.h_table} (a int PRIMARY KEY, b text);""")
		self.check(con, aboveZero, equalZero)

		con.commit()
		con.close()
		node.stop()

	# top xact: readonly
	# sub xact: oriole write
	def test_top_ro_sub_owr(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(
		    f"""INSERT INTO {self.o_table} (a, b) VALUES (1, 'one'), (2, 'two');"""
		)
		self.check(con, equalZero, aboveZero)

		con.commit()
		con.close()
		node.stop()

	# top xact: readonly
	# sub xact: heap->oriole write
	def test_top_ro_sub_howr(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(
		    f"""CREATE TABLE {self.h_table} (a int PRIMARY KEY, b text) USING orioledb;"""
		)
		self.check(con, aboveZero, aboveZero)

		con.commit()
		con.close()
		node.stop()

	# top xact: readonly
	# sub xact: heap->oriole write
	# sub xact: readonly
	def test_top_ro_sub_howr_ro(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(
		    f"""CREATE TABLE {self.h_table} (a int PRIMARY KEY, b text) USING orioledb;"""
		)
		self.check(con, aboveZero, aboveZero)

		con.execute("SAVEPOINT sp2;")
		self.check(con, equalZero, aboveZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, aboveZero)

		con.commit()
		con.close()
		node.stop()

	# top xact: readonly
	# sub xact: oriole->heap write
	def test_top_ro_sub_ohwr_1(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(
		    f"""INSERT INTO {self.o_table} (a, b) VALUES (1, 'one'), (2, 'two');"""
		)
		self.check(con, equalZero, aboveZero)

		con.execute(
		    f"""CREATE TABLE {self.h_table} (a int PRIMARY KEY, b text);""")
		self.check(con, aboveZero, aboveZero)

		con.commit()
		con.close()
		node.stop()

	# top xact: readonly
	# sub xact: oriole->heap write
	def test_top_ro_sub_ohwr_2(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(
		    f"""INSERT INTO {self.o_table} (a, b) VALUES (1, 'one'), (2, 'two');"""
		)
		self.check(con, equalZero, aboveZero)

		con.execute(
		    f"""CREATE TABLE {self.h_table} (a int PRIMARY KEY, b text);""")
		self.check(con, aboveZero, aboveZero)

		con.execute(f"""
			INSERT INTO {self.o_table} (a, b) VALUES (3, 'three'), (4, 'four');
            INSERT INTO {self.o_table} (a, b) VALUES (5, 'five'), (6, 'six');
		""")
		self.check(con, aboveZero, aboveZero)

		con.execute("SAVEPOINT sp2;")
		self.check(con, equalZero, aboveZero)

		con.commit()
		con.close()
		node.stop()

	def test_top_ro_sub_ALTER_01(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(f"""ALTER TABLE {self.o_table} RENAME COLUMN b TO data;""")
		self.check(con, aboveZero, aboveZero)

		con.execute(
		    f"""INSERT INTO {self.o_table}(a, data) VALUES(1, '100');""")
		self.check(con, aboveZero, aboveZero)

		con.execute("ROLLBACK TO sp1;")
		self.check(con, equalZero, aboveZero)

		con.execute(f"""INSERT INTO {self.o_table}(a, b) VALUES(2, '200');""")
		self.check(con, equalZero, aboveZero)

		con.commit()
		con.close()
		node.stop()

	def test_top_ro_sub_ALTER_02(self):
		node = self.node
		self.init()
		con = node.connect()
		con.begin()

		self.check(con, equalZero, equalZero)

		con.execute(f"""SELECT COUNT(*) FROM {self.o_table};""")
		self.check(con, equalZero, equalZero)

		con.execute("SAVEPOINT sp1;")
		self.check(con, equalZero, equalZero)

		con.execute(f"""ALTER TABLE {self.o_table} RENAME COLUMN b TO data;""")
		self.check(con, aboveZero, aboveZero)

		con.execute(
		    f"""INSERT INTO {self.o_table}(a, data) VALUES(1, '100');""")
		self.check(con, aboveZero, aboveZero)

		con.execute("ROLLBACK TO sp1;")
		self.check(con, equalZero, aboveZero)

		con.execute(f"""ALTER TABLE {self.o_table} RENAME COLUMN b TO data;""")
		self.check(con, aboveZero, aboveZero)

		con.execute(
		    f"""INSERT INTO {self.o_table}(a, data) VALUES(2, '200');""")
		self.check(con, aboveZero, aboveZero)

		con.execute("SAVEPOINT sp2;")
		self.check(con, equalZero, aboveZero)

		con.execute(
		    f"""INSERT INTO {self.o_table}(a, data) VALUES(3, '300');""")
		self.check(con, equalZero, aboveZero)

		con.execute("SAVEPOINT sp3;")
		self.check(con, equalZero, aboveZero)

		con.execute(
		    f"""ALTER TABLE {self.o_table} RENAME COLUMN data TO newdata;""")
		self.check(con, aboveZero, aboveZero)

		con.execute("SAVEPOINT sp4;")
		self.check(con, equalZero, aboveZero)

		con.execute("SAVEPOINT sp5;")
		self.check(con, equalZero, aboveZero)

		con.commit()
		con.close()
		node.stop()
