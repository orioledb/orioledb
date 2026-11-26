#!/usr/bin/env python3
# coding: utf-8
"""
Sorted index build must link every page of the tree, including the right
half of a root split that happens while the build flushes its final pages.

btree_write_index_data() finishes by writing out the rightmost page of each
level and putting its downlink into the parent.  Putting that downlink can
split the parent, and when the parent is the root this adds a level.  The
flush loop used a bound captured before the loop (saved_root_level).  So
the right half of such a root split was never written nor linked.  The
new root ended with the left half as its last child, while that page
still had a high key.  Any descent into the right part of the key space
then PANICs in load_page() with "error reading downlink ... Hikeys don't
match".  The keys to the right of that page are unreachable through the
index.

Whether the final flush splits the root depends on the exact page layout.
So the tests below use several key sizes and row counts that are known to
hit it, including the exact reproducer from the bug report.  Each test
runs on its own node, so a PANIC in one does not hide the result of
another.
"""

import time
from testgres.enums import NodeStatus
from .base_test import BaseTest


class IndexBuildRootSplitTest(BaseTest):

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "orioledb.checksums_enabled = on\n")
		self.node.start()
		self.node.safe_psql('postgres', "CREATE EXTENSION orioledb;")

	def server_log_tail(self, nbytes=4000):
		try:
			with open(self.node.pg_log_file, errors='replace') as f:
				return f.read()[-nbytes:]
		except OSError:
			return '<server log not available>'

	def run_query(self, sql):
		"""
		Run a query, turning a backend crash into a test failure that shows
		the server log (the PANIC message is only there, not in the client
		error).
		"""
		try:
			return self.node.execute('postgres', sql)
		except Exception as e:
			# After a PANIC the postmaster (restart_after_crash = off) shuts
			# down on its own.  Wait for it, so tearDown does not race with it.
			deadline = time.time() + 30
			while (time.time() < deadline
			       and self.node.status() == NodeStatus.Running):
				time.sleep(0.1)
			log = self.server_log_tail()
			self.fail("query failed: %s\n  error: %s\n"
			          "--- server log tail ---\n%s" %
			          (sql.strip(), str(e).strip(), log))

	def build_table_and_index(self, table, rep, nrows, checkpoint):
		self.node.safe_psql(
		    'postgres', f"""
			CREATE TABLE {table}(id int PRIMARY KEY, payload text)
				USING orioledb
				WITH (compress = -1, primary_compress = -1,
				      toast_compress = -1);
			INSERT INTO {table}
				SELECT i, repeat(md5(i::text), {rep})
				FROM generate_series(1, {nrows}) i;
			CREATE INDEX {table}_payload ON {table}(payload)
				WITH (compress = -1);
		""")
		if checkpoint:
			self.node.safe_psql('postgres', "CHECKPOINT;")

	def assert_index_only_scan(self, table, sql):
		plan = '\n'.join(r[0] for r in self.run_query(
		    "SET enable_seqscan = off; EXPLAIN (COSTS OFF) " + sql))
		self.assertIn(f"index only scan of: {table}_payload", plan,
		              "test query does not use the secondary index:\n" + plan)

	def check_index(self, table, rep, nrows, point_ids):
		# The primary tree is fine, the bug is only in the secondary index.
		self.assertEqual(
		    self.run_query(f"""
				SELECT count(*), sum(id),
				       bool_and(payload = repeat(md5(id::text), {rep}))
				FROM {table};
			""")[0], (nrows, nrows * (nrows + 1) // 2, True))

		# Point lookups through the secondary index.
		for pid in point_ids:
			sql = (f"SELECT id FROM {table} "
			       f"WHERE payload = repeat(md5('{pid}'), {rep})")
			self.assert_index_only_scan(table, sql)
			self.assertEqual(
			    self.run_query("SET enable_seqscan = off; " + sql), [(pid, )],
			    f"wrong result for id {pid}")

		# Walk the whole secondary index: every key must be reachable,
		# including the ones right of the page that lost its right sibling.
		sql = f"SELECT payload FROM {table} ORDER BY payload"
		self.assert_index_only_scan(table, sql)
		self.assertEqual(
		    self.run_query("SET enable_seqscan = off; "
		                   f"SELECT count(*) FROM ({sql}) s;")[0][0], nrows)

		# Backward walk enters the tree from the rightmost side of the root.
		self.assertEqual(
		    self.run_query(
		        "SET enable_seqscan = off; "
		        f"SELECT count(*) FROM (SELECT payload FROM {table} "
		        f"ORDER BY payload DESC) s;")[0][0], nrows)

		self.assertTrue(
		    self.run_query(
		        f"SELECT orioledb_tbl_check('{table}'::regclass, true);")[0]
		    [0])

	def check_after_restart(self, table, rep, nrows, point_ids):
		self.node.stop()
		self.node.start()
		self.check_index(table, rep, nrows, point_ids)

	def test_issue_reproducer(self):
		"""Exact steps from the bug report (384-byte keys, 5000 rows)."""
		self.build_table_and_index('index_restart', 12, 5000, True)
		self.check_index('index_restart', 12, 5000, [123, 1, 2500, 5000])
		self.check_after_restart('index_restart', 12, 5000, [123, 4999])

	def test_issue_reproducer_no_checkpoint(self):
		"""The checkpoint is not needed: the index is broken by the build."""
		self.build_table_and_index('index_restart', 12, 5000, False)
		self.check_index('index_restart', 12, 5000, [123, 1, 5000])

	def test_two_level_root_split(self):
		"""Final flush splits a level-1 root (192-byte keys, 1000 rows)."""
		self.build_table_and_index('t_two_level', 6, 1000, False)
		self.check_index('t_two_level', 6, 1000, [1, 123, 500, 1000])

	def test_root_split_other_row_count(self):
		"""Same key size as the report, different row count."""
		self.build_table_and_index('t_5150', 12, 5150, False)
		self.check_index('t_5150', 12, 5150, [1, 123, 5150])

	def test_root_split_larger_tree(self):
		"""A larger tree (256-byte keys, 15500 rows)."""
		self.build_table_and_index('t_large', 8, 15500, False)
		self.check_index('t_large', 8, 15500, [1, 123, 7750, 15500])
		self.check_after_restart('t_large', 8, 15500, [123, 15500])
