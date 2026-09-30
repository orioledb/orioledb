#!/usr/bin/env python3
# coding: utf-8
"""
Dropping a tree deletes its files one by one: the first data file durably,
then the map files of each checkpoint.  A crash in between leaves maps that
name pages of a data file that is not there.  Recovery starts from a
checkpoint that still has the tree, so it loads the tree while replaying the
changes made to it before the drop, and must take it for having no data --
the drop that follows in the WAL deletes it again -- rather than fail
reading its root page, which kept the server from starting at all.

The tests make that state directly: they keep the map files of the last
checkpoint aside, and after the drop and an immediate stop put back the maps
whose data file is gone.
"""

import os
import shutil

from .base_test import BaseTest


class RecoveryPartialDropTest(BaseTest):

	def orioledb_dir(self, node):
		datoid = node.execute("SELECT oid FROM pg_database "
		                      "WHERE datname = current_database();")[0][0]
		return os.path.join(node.data_dir, "orioledb_data", str(datoid))

	def save_maps(self, node):
		"""Keep aside the map files the last checkpoint wrote."""
		path = self.orioledb_dir(node)
		saved = os.path.join(node.base_dir, "saved_maps")
		shutil.rmtree(saved, ignore_errors=True)
		os.makedirs(saved)
		for name in os.listdir(path):
			if name.endswith(".map"):
				shutil.copy(os.path.join(path, name), saved)
		return path, saved

	def restore_maps_of_dropped(self, path, saved):
		"""Put back the maps of trees whose first data file is gone, as a
		crash between the two unlinks would have left them."""
		restored = []
		for name in os.listdir(saved):
			relnode = name.split('-')[0]
			if not os.path.exists(os.path.join(path, relnode)):
				shutil.copy(os.path.join(saved, name), path)
				restored.append(name)
		self.assertTrue(restored, "no dropped tree to restore maps for")
		return restored

	def assertFilesGone(self, path, restored):
		"""Replaying the drop again removed what the crash left behind."""
		for name in restored:
			relnode = name.split('-')[0]
			left = [
			    f for f in os.listdir(path)
			    if f == relnode or f.startswith(relnode +
			                                    '-') or f.startswith(relnode +
			                                                         '.')
			]
			self.assertEqual(left, [], "files of relnode %s left" % relnode)

	def test_crash_while_dropping_table_files(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, val int NOT NULL)
				USING orioledb;
			CREATE INDEX o_test_val ON o_test (val);
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 500) g;
		""")
		node.safe_psql("CHECKPOINT;")
		path, saved = self.save_maps(node)

		# Changes after the checkpoint, so that recovery loads the trees
		node.safe_psql("""
			INSERT INTO o_test SELECT g, g FROM generate_series(501, 1000) g;
			DROP TABLE o_test;
		""")
		node.stop(['-m', 'immediate'])
		restored = self.restore_maps_of_dropped(path, saved)

		node.start()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM pg_class "
		                 "WHERE relname = 'o_test';")[0][0], 0)
		self.assertFilesGone(path, restored)
		node.safe_psql("CHECKPOINT;")
		node.stop()

	def test_crash_while_dropping_files_of_a_rewrite(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, val int, p point)
				USING orioledb;
			INSERT INTO o_test SELECT g, g, point(g, g)
				FROM generate_series(1, 50) g;
		""")
		node.safe_psql("CHECKPOINT;")
		path, saved = self.save_maps(node)

		# The bridge index rewrites the table into a new relnode, and its
		# commit drops the old one
		node.safe_psql("""
			INSERT INTO o_test SELECT g, g, point(g, g)
				FROM generate_series(51, 100) g;
			CREATE INDEX o_test_gist ON o_test USING gist (p);
		""")
		node.stop(['-m', 'immediate'])
		restored = self.restore_maps_of_dropped(path, saved)

		node.start()
		self.assertEqual(
		    node.execute("SELECT count(*), sum(val) FROM o_test;")[0],
		    (100, 5050))
		with node.connect() as con:
			con.execute("SET enable_seqscan = off;")
			con.execute("SET enable_indexonlyscan = off;")
			self.assertEqual(
			    con.execute("SELECT count(*) FROM o_test "
			                "WHERE p <@ '((0,0),(100,100))'::box;")[0][0], 100)
		self.assertFilesGone(path, restored)
		node.stop()

	def test_crash_while_dropping_files_of_a_truncate(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, val int NOT NULL)
				USING orioledb;
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 500) g;
		""")
		node.safe_psql("CHECKPOINT;")
		path, saved = self.save_maps(node)

		node.safe_psql("""
			INSERT INTO o_test SELECT g, g FROM generate_series(501, 1000) g;
			TRUNCATE o_test;
			INSERT INTO o_test SELECT g, g FROM generate_series(1, 10) g;
		""")
		node.stop(['-m', 'immediate'])
		restored = self.restore_maps_of_dropped(path, saved)

		node.start()
		self.assertEqual(
		    node.execute("SELECT count(*), sum(val) FROM o_test;")[0],
		    (10, 55))
		self.assertFilesGone(path, restored)
		node.stop()

	def test_replica_crash_while_dropping_files_of_a_rewrite(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY, val int, p point)
				USING orioledb;
			INSERT INTO o_test SELECT g, g, point(g, g)
				FROM generate_series(1, 50) g;
		""")
		with self.getReplica().start() as replica:
			self.catchup_orioledb(replica)
			node.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)
			# The restartpoint the replica recovers from after its crash
			replica.safe_psql("CHECKPOINT;")
			path, saved = self.save_maps(replica)

			node.safe_psql("""
				INSERT INTO o_test SELECT g, g, point(g, g)
					FROM generate_series(51, 100) g;
				CREATE INDEX o_test_gist ON o_test USING gist (p);
			""")
			self.catchup_orioledb(replica)
			replica.poll_query_until(
			    "SELECT orioledb_recovery_synchronized();", expected=True)
			replica.stop(['-m', 'immediate'])
			restored = self.restore_maps_of_dropped(path, saved)

			replica.start()
			self.catchup_orioledb(replica)
			replica.poll_query_until(
			    "SELECT orioledb_recovery_synchronized();", expected=True)
			self.assertEqual(
			    replica.execute("SELECT count(*), sum(val) FROM o_test;")[0],
			    (100, 5050))
			self.assertFilesGone(path, restored)
		node.stop()
