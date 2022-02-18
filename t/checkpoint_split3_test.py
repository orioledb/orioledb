#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent
from .checkpoint_split_base_test import CheckpointSplitBaseTest

class CheckpointSplit3Test(CheckpointSplitBaseTest):
	def test_checkpoint_split_concurrent_begin(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb\n")
		node.safe_psql('postgres',
			"INSERT INTO o_checkpoint\n"
			"	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(20, 400, 40) id);\n"
		)
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()
		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.lokey.id > \"0020\" &&"
											  "$.level == 1');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		t2 = ThreadQueryExecutor(con2, "INSERT INTO o_checkpoint\n"
								 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(111, 120, 1) id);")
		t2.start()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()
		t2.join()
		con2.commit()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 20)
		node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)") # no errors, can be true or false
		node.safe_psql('postgres', "CHECKPOINT;")
		# no incomplete split
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 20)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_split_concurrent_midle(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
			"INSERT INTO o_checkpoint\n"
			"	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10, 300, 40) id);\n"
		)
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()
		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											 "$.treeName == \"o_checkpoint_pkey\" && "
											 "$.lokey.id > \"0020\" &&"
											 "$.level == 1');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		t2 = ThreadQueryExecutor(con2, "INSERT INTO o_checkpoint\n"
								 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(110, 120, 1) id);")
		t2.start()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()
		t2.join()
		con2.commit()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 19)
		node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)") # no errors, can be true or false
		node.safe_psql('postgres', "CHECKPOINT;")
		# no incomplete split
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 19)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_split_concurrent_end(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
			"INSERT INTO o_checkpoint\n"
			"	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(1, 30, 4) id);\n"
		)
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()
		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.level == 1');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("INSERT INTO o_checkpoint\n"
					 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(11, 12, 1) id);")
		con2.commit()

		con2.begin()
		t2 = ThreadQueryExecutor(con2, "INSERT INTO o_checkpoint\n"
								 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(410, 440, 1) id);")
		t2.start()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()
		t2.join()
		con2.commit()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 41)
		node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")
		node.safe_psql('postgres', "CHECKPOINT;")
		# no incomplete split
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 41)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_split_no_deadlock(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(1, 40, 4) id);\n"
		)
		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		con3.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.lokey.id > \"0030\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.execute("INSERT INTO o_checkpoint (SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(90, 95, 1) id);")
		con2.commit()

		con2.execute("INSERT INTO o_checkpoint (SELECT '0330' || repeat('x', 2500));")

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")

		t1.join()
		con2.commit()

		con1.close()
		con2.close()
		con3.close()
		node.stop()

	def test_checkpoint_split_root(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "log_min_messages = notice\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10, 300, 40) id);\n")
		con1 = node.connect()
		con2 = node.connect()
		con2.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.lokey.id > \"0200\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("INSERT INTO o_checkpoint\n"
					 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(911, 930, 1) id);")
		con2.commit()
		con2.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 28)
		# autonomous checkpoint write happens
		self.assertFalse(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 28)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_split_root_v2(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "log_min_messages = notice\n"
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10, 300, 40) id);\n")
		con1 = node.connect()
		con2 = node.connect()
		con2.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkDownwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.lokey.id > \"0100\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("INSERT INTO o_checkpoint\n"
					 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(211, 231, 1) id);")
		con2.commit()
		con2.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 29)
		# autonomous checkpoint page write happens
		self.assertFalse(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.execute("CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], 29)
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()

	def test_checkpoint_split_ok(self):
		node = self.node
		node.append_conf('postgresql.conf',
						 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_checkpoint (\n"
					   "	id text NOT NULL,\n"
					   "	PRIMARY KEY (id)\n"
					   ") USING orioledb;\n")
		node.safe_psql('postgres',
					   "INSERT INTO o_checkpoint\n"
					   "	(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(1, 30, 4) id);\n")
		con1 = node.connect()
		con2 = node.connect()
		con2.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
											 "'$.action == \"walkUpwards\" && "
											  "$.treeName == \"o_checkpoint_pkey\" && "
											  "$.nextKey.type == \"value\" && "
											  "$.nextKey.value.id > \"0010\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("INSERT INTO o_checkpoint\n"
					 "(SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(11, 12, 1) id);")
		con2.commit()
		con2.execute("SELECT pg_stopevent_reset('checkpoint_step')")

		t1.join()

		con1.close()
		con2.close()
		node.stop(['-m', 'immediate'])

		node.start()
		self.assertEqual(node.execute("SELECT count(*) FROM o_checkpoint;")[0][0], 10)
		node.stop()
