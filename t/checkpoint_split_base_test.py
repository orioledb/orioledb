#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent

class CheckpointSplitBaseTest(BaseTest):
	def checkpoint_concurrent_split_base(self, init_values,
										 breakpoint_event,
										 breakpoint_value,
										 breakpoint_level,
										 values_after_breakpoint, rows_amount,
										 with_eviction=False, select_after_eviction=False,
										 with_second_eviction=False, with_second_checkpoint=True):
		node = self.node
		node.append_conf('postgresql.conf',
						 "log_min_messages = notice\n"
						 "orioledb.enable_stopevents = true\n")
		if with_eviction:
			node.append_conf('postgresql.conf',
							 "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql('postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_checkpoint (
				id text NOT NULL,
				PRIMARY KEY (id)
			) USING orioledb;
		""")
		if with_eviction:
			node.safe_psql('postgres', """
			CREATE TABLE IF NOT EXISTS o_test (
				key SERIAL NOT NULL,
				val int NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb;
			CREATE UNIQUE INDEX o_test_idx2 ON o_test (key);
			CREATE UNIQUE INDEX o_test_idx3 ON o_test (key);
			CREATE UNIQUE INDEX o_test_idx4 ON o_test (key);""")
		node.safe_psql('postgres', "INSERT INTO o_checkpoint %s;" % init_values)

		con1 = node.connect()
		con2 = node.connect()
		con3 = node.connect()

		if breakpoint_event == 'downwards':
			breakpoint_condition = "$.action == \"walkDownwards\" && $.treeName == \"o_checkpoint_pkey\""
			if breakpoint_value:
				breakpoint_condition += " && $.lokey.id > \"%s\"" % breakpoint_value
		elif breakpoint_event == 'upwards':
			breakpoint_condition = "$.action == \"walkUpwards\" && $.treeName == \"o_checkpoint_pkey\""
			if breakpoint_value:
				breakpoint_condition += " && $.nextKey.type == \"value\" && $.nextKey.value.id > \"%s\"" % breakpoint_value
		elif breakpoint_event == 'continue':
			breakpoint_condition = "$.action == \"walkContinue\" && $.treeName == \"o_checkpoint_pkey\""
			if breakpoint_value:
				breakpoint_condition += " && $.nextKey.type == \"value\" && $.nextKey.value.id > \"%s\"" % breakpoint_value

		if breakpoint_level:
			breakpoint_condition += " && $.level == %s" % breakpoint_level

		con3.execute("SELECT pg_stopevent_set('checkpoint_step', '%s');" % breakpoint_condition)

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		con2.begin()
		con2.execute("INSERT INTO o_checkpoint %s" % values_after_breakpoint)
		con2.commit()

		if with_eviction:
			n = 20000
			step = 1000
			for i in range(1, n, step):
				con2.execute("INSERT INTO o_test (val)\n"
								"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
					(str(i), str(i + step - 1)))
				con2.commit()

		if select_after_eviction:
			con3.execute("SELECT * FROM o_checkpoint")
			con3.commit()

		if with_second_eviction:
			n = 20000
			step = 1000
			for i in range(1, n, step):
				con2.execute("INSERT INTO o_test (val)\n"
								"	(SELECT val FROM generate_series(%s, %s, 1) val);\n" %
					(str(i), str(i + step - 1)))
				con2.commit()

		con3.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], rows_amount)
		node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)") # no errors, can be true or false

		if with_second_checkpoint:
			node.safe_psql('postgres', "CHECKPOINT;")
			self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])

		con1.close()
		con2.close()
		con3.close()
		node.stop(['-m', 'immediate'])

		node.start()
		node.safe_psql("SELECT * FROM o_checkpoint;")
		self.assertEqual(node.execute("SELECT COUNT(*) FROM o_checkpoint;")[0][0], rows_amount)

		if not with_second_checkpoint:
			node.safe_psql('postgres', "CHECKPOINT;")
		self.assertTrue(node.execute("SELECT orioledb_tbl_check('o_checkpoint'::regclass)")[0][0])
		node.stop()
