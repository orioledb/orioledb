#!/usr/bin/env python3
# coding: utf-8

import os
import re
import signal
import subprocess
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

		title_re = re.compile(r'orioledb background writer (\d+)\s*$')

		def get_bgwriters():
			rows = node.execute("""
				SELECT pid FROM pg_stat_activity
				WHERE backend_type = 'orioledb background writer';
			""")
			result = {}
			for (pid,) in rows:
				try:
					title = subprocess.check_output(
					    ['ps', '-p', str(pid), '-o', 'command='],
					    stderr=subprocess.DEVNULL).decode()
				except subprocess.CalledProcessError:
					continue
				m = title_re.search(title)
				if m:
					result[int(m.group(1))] = pid
			return result

		initial = get_bgwriters()
		self.assertEqual(set(initial.keys()), {0, 1, 2})

		target_num = 1
		target_pid = initial[target_num]
		os.kill(target_pid, signal.SIGKILL)

		# Wait for postmaster crash recovery to bring all bgwriters back.
		deadline = time.time() + 60
		final = {}
		while time.time() < deadline:
			try:
				final = get_bgwriters()
			except Exception:
				final = {}
			if (set(final.keys()) == {0, 1, 2}
			        and final.get(target_num) not in (None, target_pid)):
				break
			time.sleep(0.2)

		self.assertEqual(set(final.keys()), {0, 1, 2},
		                 "bgwriters did not respawn after crash")
		self.assertNotEqual(final[target_num], target_pid,
		                    "killed bgwriter still has the old PID")
		# Whole-cluster crash recovery refreshes every PID; only the
		# number-to-slot mapping is the invariant we care about.

		node.stop()


if __name__ == "__main__":
	unittest.main()
