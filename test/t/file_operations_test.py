#!/usr/bin/env python3

import glob
import os
import re
import time

from .base_test import BaseTest

from testgres.enums import NodeStatus


class FileOperationsTest(BaseTest):
	###
	#  Check create checkpoint file error and release of pages which belong to
	#  the tree.
	###
	def test_can_not_create_checkpoint_file(self):
		node = self.node
		node.start()  # start PostgreSQL
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_test (\n"
		    "	id integer NOT NULL,\n"
		    "	val text\n"
		    ") USING orioledb;\n")

		was_busy = node.execute('postgres',
		                        "SELECT orioledb_page_stats();")[0][0]

		# have not access to create checkpoint file, exception
		try:
			self.unsetRWX([self.getOrioleDBDir()])
			node.safe_psql("SELECT * FROM o_test;")
			self.assertEqual("We should not be here", "")
		except Exception as e:
			message = re.sub('[0-9]+', 'x', e.message)
			self.assertTrue(
			    "FATAL:  could not open data file orioledb_data/x/x: Permission denied"
			    in message)

		# checks that all pages has been released
		self.assertEqual(
		    node.execute('postgres', "SELECT orioledb_page_stats();")[0][0],
		    was_busy)

		# have access to create checkpoint file, no exception
		try:
			self.setRWX([self.getOrioleDBDir()])
			node.safe_psql("SELECT * FROM o_test;")
		except Exception:
			self.assertEqual("We should not be here", "")

	def unsetRWX(self, files):
		for fname in files:
			os.chmod(fname, 0o000)

	def setRWX(self, files):
		for fname in files:
			os.chmod(fname, 0o700)

	def getOrioleDBDir(self):
		return self.node.data_dir + "/orioledb_data"

	def test_checkpoint_fatal_on_corrupted_tree(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', """
			orioledb.main_buffers = 8MB
			orioledb.debug_disable_bgwriter = true
		""")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_corrupt (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			INSERT INTO o_corrupt
				SELECT i, repeat('x', 200) FROM generate_series(1, 5000) i;
			CHECKPOINT;
		""")

		datoid = node.execute(
		    'postgres',
		    "SELECT oid FROM pg_database WHERE datname='postgres';")[0][0]

		node.safe_psql(
		    'postgres',
		    "SELECT orioledb_evict_pages('o_corrupt'::regclass, 99);")

		datoid_dir = os.path.join(node.data_dir, 'orioledb_data', str(datoid))
		for f in glob.glob(os.path.join(datoid_dir, '*')):
			if not f.endswith('.map'):
				with open(f, 'r+b') as fh:
					fh.truncate(0)

		# Without the fix the checkpointer gets ERROR and either hits an
		# Assert (debug builds) or silently skips the corrupted tree on
		# subsequent checkpoints (release builds). With the fix it FATALs
		# immediately and shuts down the cluster cleanly.
		try:
			node.safe_psql('postgres', "CHECKPOINT;")
		except Exception:
			pass

		for _ in range(10):
			if node.status() != NodeStatus.Running:
				break
			time.sleep(1)

		self.assertEqual(node.status(), NodeStatus.Stopped)

		with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
			log = f.read()
			self.assertIn("could not read rootPageBlkno page from", log)
			# With the fix the shutdown is FATAL, not an Assert crash.
			self.assertNotIn("TRAP", log)

	def test_load_page_read_error_releases_page(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, val text) USING orioledb;
			INSERT INTO o_test
				SELECT i, repeat('x', 200) FROM generate_series(1, 5000) i;
			CHECKPOINT;
			SELECT orioledb_evict_pages('o_test'::regclass, 0);
		""")

		# The leaves are on disk only now: empty the data file, so that
		# reading one back in load_page() fails.
		datoid = node.execute(
		    "SELECT oid FROM pg_database WHERE datname = 'postgres';")[0][0]
		db_dir = os.path.join(self.getOrioleDBDir(), str(datoid))
		for f in os.listdir(db_dir):
			if f.isdigit():
				os.truncate(os.path.join(db_dir, f), 0)

		free_pages = ("SELECT free_pages FROM orioledb_page_stats() "
		              "WHERE pool_name = 'main';")
		before = node.execute(free_pages)
		with self.assertRaisesRegex(Exception, "could not read page"):
			node.safe_psql("SELECT * FROM o_test WHERE id = 1;")
		# The page taken for the failed read must be back in the pool
		self.assertEqual(node.execute(free_pages), before)
