#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import generate_string as gen_str
from .base_test import wait_checkpointer_stopevent

class ToastIndexTest(BaseTest):
	def init_table(self, stopEvents):
		node = self.node
		if stopEvents:
			node.append_conf('postgresql.conf',
							 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql('postgres',
					   "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
					   "CREATE TABLE IF NOT EXISTS o_toast_index (\n"
					   "	id integer PRIMARY KEY,\n"
					   "	v1 text,\n"
					   "	v2 text,\n"
					   "	v3 text\n"
					   ") USING orioledb;\n"
					   "CREATE INDEX o_toast_index_v1_idx ON o_toast_index(v1);\n"
					   "CREATE INDEX o_toast_index_v2_idx ON o_toast_index(v2);\n"
					   "CREATE INDEX o_toast_index_v3_idx ON o_toast_index(v3);\n")

		for i in range(0, 10):
			baseIndex = i * 3 + 1
			node.safe_psql("INSERT INTO o_toast_index VALUES (%d, '%s', '%s', '%s')" %
						   (i + 1,
							gen_str(2500, baseIndex),
							gen_str(2500, baseIndex + 1),
							gen_str(2500, baseIndex + 2)))

	def do_update(self, con, j):
		for i in range(0, 10):
			baseIndex = j * 30 + i * 3 + 1
			con.execute("UPDATE o_toast_index SET v1 = '%s', v2 = '%s', v3 = '%s' WHERE id = %d" %
						(gen_str(2500, baseIndex),
						 gen_str(2500, baseIndex + 1),
						 gen_str(2500, baseIndex + 2),
						 i + 1))

	def check_result(self, j):
		node = self.node
		con = node.connect()
		baseRs = con.execute("SELECT * FROM o_toast_index;")
		self.assertEqual(len(baseRs), 10)
		for row in baseRs:
			baseIndex = j * 30 + (row[0] - 1) * 3 + 1
			self.assertEqual(row[1], gen_str(2500, baseIndex))
			self.assertEqual(row[2], gen_str(2500, baseIndex + 1))
			self.assertEqual(row[3], gen_str(2500, baseIndex + 2))

		v1Rs = con.execute("SELECT id, v1 FROM o_toast_index ORDER BY v1;")
		self.assertEqual(len(v1Rs), 10)
		for row in v1Rs:
			self.assertEqual(row[1], gen_str(2500, j * 30 + (row[0] - 1) * 3 + 1))

		v2Rs = con.execute("SELECT id, v2 FROM o_toast_index ORDER BY v2;")
		self.assertEqual(len(v2Rs), 10)
		for row in v2Rs:
			self.assertEqual(row[1], gen_str(2500, j * 30 + (row[0] - 1) * 3 + 2))

		v3Rs = con.execute("SELECT id, v3 FROM o_toast_index ORDER BY v3;")
		self.assertEqual(len(v3Rs), 10)
		for row in v3Rs:
			self.assertEqual(row[1], gen_str(2500, j * 30 + (row[0] - 1) * 3 + 3))

		con.close()

	def test_no_checkpoint(self):
		self.init_table(False)
		node = self.node
		con = node.connect()
		con.begin()
		for j in range(1, 5):
			self.do_update(con, j)
		con.commit()

		con.begin()
		for j in range(5, 9):
			self.do_update(con, j)
		con.rollback()
		con.close()

		node.stop(['-m', 'immediate'])
		node.start()
		self.check_result(4)
		node.stop()

	def test_checkpoint_at_start(self):
		self.init_table(False)
		node = self.node
		con = node.connect()
		con.execute("CHECKPOINT;")
		con.begin()
		for j in range(1, 5):
			self.do_update(con, j)
		con.commit()

		con.begin()
		for j in range(5, 7):
			self.do_update(con, j)
		con.execute("CHECKPOINT;")
		for j in range(7, 9):
			self.do_update(con, j)
		con.rollback()
		con.close()

		node.stop(['-m', 'immediate'])
		node.start()
		self.check_result(4)
		node.stop()

	def test_checkpoint_in_middle(self):
		self.init_table(False)
		node = self.node
		con = node.connect()
		con.begin()
		for j in range(1, 5):
			self.do_update(con, j)
		con.commit()

		con.begin()
		for j in range(5, 7):
			self.do_update(con, j)
		con.execute("CHECKPOINT;")
		for j in range(7, 9):
			self.do_update(con, j)
		con.rollback()
		con.close()

		node.stop(['-m', 'immediate'])
		node.start()
		self.check_result(4)
		node.stop()

	def test_checkpoint(self):
		self.init_table(True)
		node = self.node
		con = node.connect()
		chkpCon = node.connect()
		con.begin()
		for j in range(1, 4):
			self.do_update(con, j)

		con.execute("""
			SELECT pg_stopevent_set('checkpoint_step',
									'$.treeName == \"toast\" &&
									 $.action == \"walkUpwards\" &&
									 $.nextKey.value.attnum > 2');""")
		t1 = ThreadQueryExecutor(chkpCon, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		self.do_update(con, 4)

		con.execute("SELECT pg_stopevent_reset('checkpoint_step')")
		t1.join()

		con.commit()
		con.close()
		chkpCon.close()

		node.stop(['-m', 'immediate'])
		node.start()
		self.check_result(4)
		node.stop()
