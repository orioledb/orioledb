#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

class EvictionFullMemoryTest(BaseTest):
	def test_eviction_table_full_memory(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.debug_disable_bgwriter = true\n"
						 "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	key int8 NOT NULL,\n"
			"	val int8 NOT NULL,\n"
			"	val2 text NOT NULL,\n"
			"	PRIMARY KEY (key)\n"
			") USING orioledb;\n")

		n = 200000
		con = node.connect()
		con.begin()
		con.execute("INSERT INTO o_test (SELECT id, %s - id, repeat('x', 1000) FROM generate_series(%s, %s, 1) id);" %
					(str(n), str(1), str(n)))
		con.commit()

		con.close()
		node.stop()

	def test_eviction_table_full_memory_root_incomplete_split(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.debug_disable_bgwriter = true\n"
						 "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	key int8 NOT NULL,\n"
			"	val int8 NOT NULL,\n"
			"	val2 text NOT NULL,\n"
			"	PRIMARY KEY (key, val)\n"
			") USING orioledb;\n")

		before_root_split = 255000
		n = 256000
		step = 1000
		con1 = node.connect()
		con2 = node.connect()
		con1_pid = con1.pid
		con2_pid = con2.pid

		con1.execute("SET orioledb.enable_stopevents = true;")
		con2.execute("SELECT pg_stopevent_set('split_fail', '$.treeName == \"o_test_pkey\" && $.level == 2');")
		# prevent to fix root split by an apply_undo()
		con2.execute("SELECT pg_stopevent_set('before_apply_undo', '$.commit == false');")

		for i in range(1, before_root_split, step):
			con1.begin()
			con1.execute("INSERT INTO o_test (SELECT id, %s - id, repeat('x', 2000) FROM generate_series(%s, %s, 1) id);" %
						(str(n), str(i), str(i + step - 1)))
			con1.commit()

		t1 = ThreadQueryExecutor(con1,
								 "INSERT INTO o_test (SELECT id, %s - id, repeat('x', 2000) FROM generate_series(%s, %s, 1) id);" %
								 (str(200000), str(before_root_split + 1), str(before_root_split + step)))

		t1.start()

		while node.execute("SELECT pg_isolation_test_session_is_blocked(%d, '{%d}');"
						   % (con1_pid, con2_pid))[0][0] == False:
			continue

		# we have 1 reserved free page, table creation needs 7, try to get
		con2.execute("CREATE TABLE o_get_free_pages (\n"
					 "   key integer NOT NULL,\n"
					 "   PRIMARY KEY (key)\n"
					 ") USING orioledb;\n")
		con2.execute("SELECT * FROM o_get_free_pages;")
		con2.execute("SELECT pg_stopevent_reset('split_fail');")
		con2.execute("SELECT pg_stopevent_reset('before_apply_undo');")

		try:
			t1.join()
		except AssertionError:
			raise
		except Exception:
			self.assertTrue(True)

		con1.close()
		con2.close()
		node.stop()

if __name__ == "__main__":
	unittest.main()
