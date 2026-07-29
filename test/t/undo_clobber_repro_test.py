#!/usr/bin/env python3
# coding: utf-8
"""TEMPORARY repro for finding #2: stale o_buffers cache page clobbering a
direct-written undo page during checkpoint fsync.

Requires o_buffers.c built with OB_DIRECT_CLOBBER_CHECK.  The detector emits
an 'DIRECT-WRITE CLOBBER' WARNING to the server log when a cached dirty copy
of a just-direct-written block is flushed on top of it with divergent bytes.
"""

import re
import threading
import time
import unittest

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor

SMALL_UNDO_CONF = """
orioledb.main_buffers = 8MB
orioledb.undo_buffers = 128
orioledb.xid_buffers = 128
log_min_messages = warning
"""


class UndoClobberReproTest(BaseTest):

	def _scan_log(self):
		with open(self.node.pg_log_file, 'r', errors='replace') as f:
			text = f.read()
		clobbers = re.findall(r'DIRECT-WRITE CLOBBER.*', text)
		overlaps = re.findall(r'direct-write overlap \(identical\).*', text)
		return clobbers, overlaps

	def test_repro_clobber(self):
		node = self.node
		node.append_conf('postgresql.conf', SMALL_UNDO_CONF)
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_clob (id integer PRIMARY KEY, v integer NOT NULL)
				USING orioledb;
			INSERT INTO o_clob SELECT i, 0 FROM generate_series(1, 5000) i;
		""")
		node.safe_psql('postgres', 'CHECKPOINT;')

		# Pin undo retention low so the writer hits the slot-pressure
		# eviction path (which advances writtenLocation, often mid-page,
		# through the o_buffers cache).
		ret_con = node.connect()
		ret_con.begin()
		ret_con.execute("SELECT count(*) FROM o_clob;")

		writer_con = node.connect()
		chkp_con = node.connect()
		stop = [False]

		def writer():
			i = 0
			while not stop[0]:
				lo = (i * 137) % 5000 + 1
				writer_con.execute(
				    "UPDATE o_clob SET v = v + 1 WHERE id BETWEEN %d AND %d;"
				    % (lo, min(lo + 40, 5000)))
				i += 1

		def checkpointer():
			while not stop[0]:
				chkp_con.execute("CHECKPOINT;")

		tw = threading.Thread(target=writer)
		tc = threading.Thread(target=checkpointer)
		tw.start()
		tc.start()
		time.sleep(20)
		stop[0] = True
		tw.join()
		tc.join()

		ret_con.commit()
		ret_con.close()
		writer_con.close()
		chkp_con.close()
		node.stop()

		clobbers, overlaps = self._scan_log()
		print(f"\n=== overlaps (identical, harmless): {len(overlaps)} ===")
		print(f"=== CLOBBERS (divergent, the bug): {len(clobbers)} ===")
		for c in clobbers[:20]:
			print("  " + c)
		self.assertEqual(
		    clobbers, [],
		    f"detected {len(clobbers)} direct-write clobber(s); "
		    f"see server log {self.node.pg_log_file}")


if __name__ == '__main__':
	unittest.main()
