#!/usr/bin/env python3
# coding: utf-8

from contextlib import ExitStack

from testgres.connection import DatabaseError

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_stopevent


class UniqueRootSplitTest(BaseTest):

	def test_unique_check_root_split(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE t(i bigint PRIMARY KEY, j bigint NOT NULL) USING orioledb;
			CREATE UNIQUE INDEX t_j_idx ON t(j);
			INSERT INTO t VALUES (0, 0);
		""")
		with ExitStack() as stack:
			holder = stack.enter_context(node.connect(autocommit=True))
			waiter = stack.enter_context(node.connect(autocommit=True))
			ctl = stack.enter_context(node.connect(autocommit=True))
			# Equal field hashes guarantee the same unique-lock bucket.
			self.assertEqual(
			    ctl.execute('SELECT hashint8(1) = hashint8(4294967296)'),
			    [(True, )])
			for con in (holder, waiter):
				con.execute('SET orioledb.enable_stopevents = on')
				con.execute("SET statement_timeout = '30s'")
			holder_pid = holder.pid
			waiter_pid = waiter.pid
			tasks = []
			try:
				ctl.execute("SELECT pg_stopevent_set('unique_insert_done', "
				            "'$.treeName == \"t_j_idx\"')")
				inserting = ThreadQueryExecutor(holder,
				                                'INSERT INTO t VALUES (1, 1)')
				tasks.append(inserting)
				inserting.start()
				wait_stopevent(node, holder_pid)
				ctl.execute("SELECT pg_stopevent_set('unique_page_unlocked', "
				            "'$.treeName == \"t_j_idx\"')")
				checking = ThreadQueryExecutor(
				    waiter, 'INSERT INTO t VALUES (-1, 4294967296)')
				tasks.append(checking)
				checking.start()
				wait_stopevent(node, waiter_pid)
				ctl.execute("SELECT pg_stopevent_reset('unique_insert_done')")
				inserting.join()
				# Grow the single-leaf unique tree and insert the conflicting key
				# while the waiter retains its old root block and change count.
				ctl.execute(
				    'INSERT INTO t SELECT g,g FROM generate_series(2,1001) g')
				ctl.execute('INSERT INTO t VALUES (2000,4294967296)')
				structure = ctl.execute(
				    "SELECT orioledb_tbl_structure('t'::regclass, 'nue')"
				)[0][0]
				index = structure.split('Index t_j_idx contents', 1)[1]
				self.assertRegex(index, r'Page \d+: level = [1-9]')
				ctl.execute(
				    "SELECT pg_stopevent_reset('unique_page_unlocked')")
				with self.assertRaisesRegex(DatabaseError, 'duplicate key'):
					checking.join()
			finally:
				ctl.execute("SELECT pg_stopevent_reset('unique_insert_done')")
				ctl.execute(
				    "SELECT pg_stopevent_reset('unique_page_unlocked')")
				for task in tasks:
					if task.is_alive():
						task.join(timeout=30)
			self.assertEqual(ctl.execute('SELECT count(*) FROM t'), [(1003, )])
			self.assertEqual(
			    ctl.execute('SELECT i FROM t WHERE j = 4294967296'),
			    [(2000, )])
			self.assertEqual(
			    ctl.execute("SELECT orioledb_tbl_check('t'::regclass)"),
			    [(True, )])
