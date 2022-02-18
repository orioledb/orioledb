#!/usr/bin/env python3
# coding: utf-8

import random

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

class CheckpointUpdateBaseTest(BaseTest):
	def concurrent_update_eviction_base(self, compressed, is_chkp_before_insert, is_chkp_after_insert, loops_count, is_loop_update = True):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 8MB\n"
						 "checkpoint_timeout = 1900\n"
						 "max_wal_size = 1GB\n"
						 "orioledb.debug_disable_bgwriter = true\n"
						 "orioledb.enable_stopevents = true\n"
						 "log_min_messages = notice\n")
		node.start() # start PostgreSQL
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE TABLE IF NOT EXISTS o_evicted (
				id int NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb %s;""" % ("WITH (primary_compress)" if compressed else ""))
		con1 = node.connect()
		con2 = node.connect()

		if is_chkp_before_insert:
			node.execute("CHECKPOINT;")

		con1.execute("INSERT INTO o_evicted (SELECT id, id + 1 FROM generate_series(1, 15000, 1) id);\n")
		con1.commit()

		if is_chkp_after_insert:
			node.execute("CHECKPOINT;")

		# uses random to detect more problems
		con2.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkUpwards\" && "
											  "$.treeName == \"o_evicted_pkey\" && "
											  "$.level == 0 && "
											  "$.nextKey.value.id > %d');" % (random.randint(2000, 13000)))

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		node.safe_psql('postgres', "UPDATE o_evicted SET val = val + 1 WHERE id % 10 = 0;")

		n = 200000
		con2.execute("INSERT INTO o_test (val)\n"
						"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
			(str(1), str(n)))
		con2.commit()

		con2.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		con1.close()
		con2.close()

		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass);")[0][0])
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass, True);")[0][0])

		for i in range(loops_count):
			if is_loop_update:
				node.safe_psql('postgres', "UPDATE o_evicted SET val = val + 1 WHERE id % 10 = 0;")
			node.safe_psql('postgres', "CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass);")[0][0])
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass, True);")[0][0])
		node.stop(['-m', 'immediate'])

		node.append_conf('postgresql.conf',
						 "orioledb.main_buffers = 20MB\n")
		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_evicted;")[0][0], 15000);
		if is_chkp_before_insert or is_chkp_after_insert or loops_count != 0:
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass, TRUE);")[0][0])
		# else recovery happens

		for i in range(2):
			node.safe_psql('postgres', "UPDATE o_evicted SET val = val + 1 WHERE id % 10 = 0;")
			node.safe_psql('postgres', "CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass);")[0][0])
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_evicted'::regclass, True);")[0][0])
		node.stop()
