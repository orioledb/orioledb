#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest


class CompressPageBoundsTest(BaseTest):
	"""
	Regression tests for ORI-263: read_page_from_disk() used the B-tree
	downlink extent length and the on-disk compress_page_size header field
	without bounds checking.  Crafted metadata could make it compute a
	read_size far larger than the 8 KB stack buffer OrioleDBChecksummablePage
	(stack smash) or hand o_decompress_page() a size beyond the bytes
	actually fetched.  The fix rejects such metadata before any read or
	decompression, raising ERRCODE_DATA_CORRUPTED instead of crashing.
	"""

	CONF = ("shared_preload_libraries = orioledb\n"
	        "orioledb.main_buffers = 8MB\n"
	        "orioledb.debug_disable_pools_limit = true\n"
	        "orioledb.debug_disable_bgwriter = true\n")

	def _make_compressed_table(self):
		node = self.node
		node.append_conf('postgresql.conf', self.CONF)
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("""
			CREATE TABLE o_bounds (
				id int PRIMARY KEY,
				payload text
			) USING orioledb WITH (compress = 11);
			INSERT INTO o_bounds
				SELECT g, repeat('x', 200) FROM generate_series(1, 5000) g;
		""")
		# Evict leaves so the level-1 parent holds on-disk downlinks.
		node.safe_psql("SELECT orioledb_evict_pages('o_bounds'::regclass, 0);")

	def test_corrupt_downlink_len_overflow(self):
		"""
		A downlink length above ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ
		(16) makes read_page_from_disk() compute read_size = len * 512 and
		read that many bytes into the 8 KB stack buffer, smashing the stack.
		The fix rejects the extent before the read; the backend raises
		ERRCODE_DATA_CORRUPTED instead of crashing.  Without the fix this
		overflows the stack (signal 6/11).
		"""
		self._make_compressed_table()
		node = self.node

		# 0x7FFF is the 15-bit maximum storable in a downlink.
		self.assertIsNotNone(
		    node.execute("SELECT orioledb_test_corrupt_downlink_len("
		                 "'o_bounds'::regclass, 32767);")[0][0])

		reader = node.connect()
		reader.execute("SET LOCAL enable_indexscan = off;")
		reader.execute("SET LOCAL enable_bitmapscan = off;")
		with self.assertRaises(Exception) as cm:
			reader.execute("SELECT * FROM o_bounds;")
		self.assertIn("invalid leaf page", str(cm.exception))
		reader.close()
		node.stop()

	def test_corrupt_downlink_len_zero(self):
		"""
		A zero downlink length is invalid metadata.  It is rejected before
		read_size is computed, raising ERRCODE_DATA_CORRUPTED rather than
		reading zero bytes and feeding garbage to decompression.
		"""
		self._make_compressed_table()
		node = self.node

		self.assertIsNotNone(
		    node.execute("SELECT orioledb_test_corrupt_downlink_len("
		                 "'o_bounds'::regclass, 0);")[0][0])

		reader = node.connect()
		reader.execute("SET LOCAL enable_indexscan = off;")
		reader.execute("SET LOCAL enable_bitmapscan = off;")
		with self.assertRaises(Exception) as cm:
			reader.execute("SELECT * FROM o_bounds;")
		self.assertIn("invalid leaf page", str(cm.exception))
		reader.close()
		node.stop()

	def test_corrupt_compressed_page_size(self):
		"""
		An on-disk compress_page_size larger than the fetched extent makes
		o_decompress_page() read past the bytes actually read.  The on-disk
		page is rewritten with a matching checksum so the checksum check
		(always on) passes and execution reaches the compress_page_size
		bound, which rejects the page with ERRCODE_DATA_CORRUPTED.  Without
		the fix zstd decompresses uninitialized stack and PANICs.
		"""
		self._make_compressed_table()
		node = self.node

		self.assertIsNotNone(
		    node.execute("SELECT orioledb_test_corrupt_compressed_page_size("
		                 "'o_bounds'::regclass);")[0][0])

		reader = node.connect()
		reader.execute("SET LOCAL enable_indexscan = off;")
		reader.execute("SET LOCAL enable_bitmapscan = off;")
		with self.assertRaises(Exception) as cm:
			reader.execute("SELECT * FROM o_bounds;")
		self.assertIn("invalid leaf page", str(cm.exception))
		reader.close()
		node.stop()


if __name__ == "__main__":
	unittest.main()
