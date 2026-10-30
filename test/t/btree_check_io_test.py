#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class BTreeCheckIOTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf',
		    "orioledb.enable_stopevents = true\n"
		    "orioledb.debug_disable_bgwriter = true\n"
		    "orioledb.main_buffers = 8MB\n")

	def test_check_waits_for_page_io_without_parent_lock(self):
		"""The checker must not hold a page needed to finish its child I/O."""
		node = self.node
		node.start()
		node.safe_psql(
		    "CREATE EXTENSION orioledb;\n"
		    "CREATE TABLE o_check_io (id int PRIMARY KEY, v text NOT NULL)"
		    " USING orioledb;\n"
		    "INSERT INTO o_check_io SELECT g, repeat('x', 400)"
		    " FROM generate_series(1, 2000) g;\n"
		    "CHECKPOINT;\n"
		    "UPDATE o_check_io SET v = v || 'y';\n")

		ctrl = node.connect()
		writer = node.connect()
		checker = node.connect()
		writer_pid = writer.pid
		checker_pid = checker.pid
		write = None
		check = None

		ctrl.execute(
		    "SELECT pg_stopevent_set('after_evict_ionum_set',"
		    " '$.treeName == \"o_check_io_pkey\" && $.level == 0 &&"
		    "  $pid == %d');" % writer_pid)
		try:
			write = ThreadQueryExecutor(
			    writer,
			    "SELECT orioledb_evict_pages('o_check_io'::regclass, 0);")
			write.start()
			deadline = time.time() + 30
			while time.time() < deadline:
				parked = ctrl.execute(
				    "SELECT EXISTS(SELECT 1 FROM pg_stopevents()"
				    " WHERE waiter_pids @> ARRAY[%d]);" % writer_pid)[0][0]
				if parked:
					break
				time.sleep(0.05)
			else:
				self.fail("page eviction did not reach its I/O stop event")

			check = ThreadQueryExecutor(
			    checker, "SELECT * FROM verify_orioledb('o_check_io'::regclass);")
			check.start()

			deadline = time.time() + 30
			last_wait = None
			while time.time() < deadline:
				waiting = ctrl.execute(
				    "SELECT wait_event_type, wait_event FROM pg_stat_activity"
				    " WHERE pid = %d;" % checker_pid)
				if waiting:
					last_wait = waiting[0]
				if last_wait and last_wait[0] == 'LWLock':
					break
				time.sleep(0.05)
			else:
				self.fail("B-tree check did not wait for page I/O;"
				          " last wait was %s" % (last_wait, ))
		finally:
			ctrl.execute("SELECT pg_stopevent_reset('after_evict_ionum_set');")

		write.join(timeout=10)
		check.join(timeout=10)
		write_alive = write.is_alive()
		check_alive = check.is_alive()
		if write_alive or check_alive:
			writer.close()
			checker.close()
		self.assertFalse(write_alive, "page eviction did not finish")
		self.assertFalse(check_alive,
		                 "B-tree check deadlocked with page eviction")

		writer.commit()
		self.assertTrue(checker.execute(
		    "SELECT orioledb_tbl_check('o_check_io'::regclass, false);")[0][0])
		checker.commit()

		ctrl.close()
		writer.close()
		checker.close()
