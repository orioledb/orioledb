#!/usr/bin/env python3
# coding: utf-8

import os
import re
import signal
import tempfile
import time
import unittest

from .base_test import BaseTest


class EvictionBGWriterTest(BaseTest):

	def test_eviction_multiple_bgwriters(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "orioledb.bgwriter_num_workers = 2\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_eviction (\n"
		    "	key integer NOT NULL,\n"
		    "	val integer NOT NULL,\n"
		    "	PRIMARY KEY(key)\n"
		    ") USING orioledb;\n\n")
		n = 200000
		node.safe_psql(
		    'postgres', "INSERT INTO o_eviction\n"
		    "	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
		    (str(n), str(1), str(n)))

		node.stop()

		node.start()
		self.assertEqual(
		    node.execute("SELECT COUNT(*) FROM o_eviction;")[0][0], 200000)
		node.stop()

	def test_eviction_bgwriter_invalidation(self):
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_eviction (\n"
		    "	key integer NOT NULL,\n"
		    "	val integer NOT NULL,\n"
		    "	PRIMARY KEY(key)\n"
		    ") USING orioledb;\n\n")
		n = 200000

		fp = tempfile.NamedTemporaryFile(mode='wt', delete=False)
		fp.write("CREATE TABLE o_tmp ("
		         "key integer NOT NULL, PRIMARY KEY(key)) USING orioledb;\n"
		         "DROP TABLE o_tmp;\n")
		fp.close()
		node.pgbench_with_wait(
		    options=['-f', fp.name, '-n', '-c', '1', '-t', '99'])

		node.safe_psql(
		    'postgres', "INSERT INTO o_eviction\n"
		    "	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
		    (str(n), str(1), str(n)))

		node.stop()

	def test_eviction_catalog_dependent_key_types(self):
		"""
		The background writer must be able to compare index keys whose
		comparison function consults the PostgreSQL type cache.
		"""
		# (name, column type, value expression, extra DDL run first)
		key_types = [
		    ('int4range', 'int4range', 'int4range(id, id + 10)', None),
		    ('int4multirange', 'int4multirange',
		     'int4multirange(int4range(id, id + 10))', None),
		    ('intarray', 'int[]', 'ARRAY[id, id + 1, id + 2]', None),
		    ('textarray', 'text[]', "ARRAY['a' || id, 'b' || id]", None),
		    ('composite', 'bgw_ctype', "ROW(id, 'v' || id)::bgw_ctype",
		     'CREATE TYPE bgw_ctype AS (a int, b text);'),
		]
		# Enough rows that the index spills past main_buffers and the bgwriter
		# has to walk the tree, comparing keys of the type under test.
		n = 600000

		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "orioledb.bgwriter_num_workers = 1\n")
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		appname_re = re.compile(r'orioledb background writer (\d+)\Z')

		def bgwriter_pids():
			rows = node.execute(
			    "SELECT application_name, pid FROM pg_stat_activity "
			    "WHERE backend_type = 'orioledb background writer'")
			result = {}
			for app_name, pid in rows or []:
				m = appname_re.match(app_name or '')
				if m:
					result[int(m.group(1))] = pid
			return result

		def wait_for_bgwriter(timeout=300):
			"""node.start() waits for the server to accept connections, not
			for the background writer to report its application_name."""
			deadline = time.time() + timeout
			last = {}
			while time.time() < deadline:
				last = bgwriter_pids()
				if set(last.keys()) == {0}:
					return last
				time.sleep(0.5)
			return last

		def assert_no_crash(stage):
			with open(node.pg_log_file) as f:
				log = f.read()
			for pattern in ('TRAP: failed Assert',
			                'was terminated by signal 6',
			                'terminating any other active server processes'):
				self.assertNotIn(
				    pattern, log,
				    "%s: background writer crashed (%r in the server log) "
				    "while handling catalog-dependent index keys" %
				    (stage, pattern))

		before = wait_for_bgwriter()
		self.assertEqual(set(before.keys()), {0},
		                 "background writer did not start; saw %s" % before)

		for name, decl, expr, pre_ddl in key_types:
			with self.subTest(key_type=name):
				table = 'o_eviction_%s' % name
				sql = pre_ddl + "\n" if pre_ddl else ""
				sql += ("CREATE TABLE %s (\n"
				        "	id integer NOT NULL,\n"
				        "	k %s,\n"
				        "	PRIMARY KEY(id)\n"
				        ") USING orioledb;\n"
				        "CREATE INDEX %s_k_idx ON %s (k);\n"
				        "INSERT INTO %s\n"
				        "	(SELECT id, %s FROM generate_series(1, %s) id);\n" %
				        (table, decl, table, table, table, expr, str(n)))
				node.safe_psql('postgres', sql)

				assert_no_crash("after loading %s" % name)
				self.assertEqual(
				    node.execute("SELECT COUNT(*) FROM %s;" % table)[0][0], n,
				    "wrong row count for key type %s" % name)

		# A crash would have respawned the writer under a new pid.
		after = bgwriter_pids()
		self.assertEqual(
		    after, before, "background writer pid changed (%s -> %s), so it "
		    "died and was restarted while evicting pages of indexes keyed on "
		    "catalog-dependent types" % (before, after))

		# The same comparison happens during WAL replay, where the startup
		# process has no catalog cache either.  An immediate shutdown skips
		# the shutdown checkpoint, so the restart actually replays the
		# workload instead of starting from a clean checkpoint.
		node.stop(['-m', 'immediate'])
		node.start()
		assert_no_crash("after crash recovery")
		for name, _, _, _ in key_types:
			self.assertEqual(
			    node.execute("SELECT COUNT(*) FROM o_eviction_%s;" %
			                 name)[0][0], n,
			    "rows lost for key type %s across restart" % name)
		node.stop()

	def test_bgwriter_keeps_number_across_crash(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.bgwriter_num_workers = 3\n"
		    "restart_after_crash = on\n")
		node.start()

		appname_re = re.compile(r'orioledb background writer (\d+)\Z')

		def get_bgwriters():
			try:
				rows = node.execute(
				    "SELECT application_name, pid FROM pg_stat_activity "
				    "WHERE backend_type = 'orioledb background writer'")
			except Exception:
				return None
			result = {}
			for app_name, pid in rows or []:
				m = appname_re.match(app_name or '')
				if m:
					result[int(m.group(1))] = pid
			return result

		def wait_for_bgwriters(expected_nums, exclude_pids=(), timeout=300):
			deadline = time.time() + timeout
			last = {}
			while time.time() < deadline:
				snapshot = get_bgwriters()
				if snapshot is None:
					time.sleep(0.5)
					continue
				last = snapshot
				if (set(last.keys()) == expected_nums
				    and not any(last[n] in exclude_pids
				                for n in expected_nums)):
					return last
				time.sleep(1.0)
			return last

		def diagnostic(prefix):
			lines = [f"{prefix}:"]
			snapshot = get_bgwriters()
			lines.append(f"pg_stat_activity bgwriters: {snapshot}")
			log_path = os.path.join(node.data_dir, 'log', 'postgresql.log')
			try:
				with open(log_path) as f:
					tail = f.readlines()[-40:]
				lines.append(f"tail of {log_path}:")
				lines.extend("  " + l.rstrip() for l in tail)
			except OSError:
				pass
			return "\n".join(lines)

		initial = wait_for_bgwriters({0, 1, 2})
		self.assertEqual(
		    set(initial.keys()), {0, 1, 2},
		    f"bgwriters did not come up at startup; saw {initial}\n" +
		    diagnostic("startup"))

		target_num = 1
		target_pid = initial[target_num]
		os.kill(target_pid, signal.SIGKILL)

		# Wait for postmaster crash recovery to bring all bgwriters back.
		final = wait_for_bgwriters({0, 1, 2}, exclude_pids=(target_pid, ))

		self.assertEqual(
		    set(final.keys()), {0, 1, 2},
		    f"bgwriters did not respawn after crash; saw {final}\n" +
		    diagnostic("post-crash"))
		self.assertNotEqual(final[target_num], target_pid,
		                    "killed bgwriter still has the old PID")
		# Whole-cluster crash recovery refreshes every PID; only the
		# number-to-slot mapping is the invariant we care about.

		node.stop()


if __name__ == "__main__":
	unittest.main()
