#!/usr/bin/env python3
# coding: utf-8

import os
import re
import signal
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

		for i in range(1, 100):
			node.safe_psql(
			    'postgres', "CREATE TABLE o_tmp (\n"
			    "	key integer NOT NULL,\n"
			    "	PRIMARY KEY(key)\n"
			    ") USING orioledb;\n")
			node.safe_psql("DROP TABLE o_tmp;")

		node.safe_psql(
		    'postgres', "INSERT INTO o_eviction\n"
		    "	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
		    (str(n), str(1), str(n)))

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
