#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import struct

from .base_test import BaseTest

# PostgreSQL's block checksum (storage/checksum_impl.h), which OrioleDB uses
# for its page checksums: 32 partial FNV-1a sums over the page read as
# uint32[len][32], folded together at the end.  Reproduced here because a
# forged page has to carry a checksum that matches it -- that is precisely
# what an attacker who controls the bytes does, and without it the read is
# rejected for the checksum and never reaches the structure check this test
# is about.
N_SUMS = 32
FNV_PRIME = 16777619
MASK32 = 0xFFFFFFFF
CHECKSUM_BASE_OFFSETS = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3,
    0x217E7CD2, 0x83E13D2C, 0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA,
    0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB, 0xE58F764B, 0x187636BC,
    0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E, 0x9FBF8C76, 0x15CA20BE,
    0xF2CA9FD3, 0x959BD756
]


def page_checksum(page):
	"""The checksum OrioleDB stores in the on-disk page header."""
	sums = list(CHECKSUM_BASE_OFFSETS)
	words = struct.unpack_from('<%dI' % (len(page) // 4), page)
	rows = len(words) // N_SUMS

	def comp(checksum, value):
		tmp = checksum ^ value
		return ((tmp * FNV_PRIME) & MASK32) ^ (tmp >> 17)

	for i in range(rows):
		base = i * N_SUMS
		for j in range(N_SUMS):
			sums[j] = comp(sums[j], words[base + j])
	for _ in range(2):
		for j in range(N_SUMS):
			sums[j] = comp(sums[j], 0)
	result = 0
	for s in sums:
		result ^= s
	return (result % 65535) + 1


class PageStructValidationTest(BaseTest):
	"""A page whose layout cannot fit the page must be refused on read.

	Everything that touches a page image computes offsets and lengths from
	the layout fields in its header, and then trusts the result.
	page_get_hikey_size() is the sharpest case: it returns
	`hikeysEnd - <the last chunk's hikey location>`, and merge_pages() copies
	that many bytes into

	    char newItem[Max(BTreeLeafTuphdrSize, BTreeNonLeafTuphdrSize) +
	                 O_BTREE_MAX_TUPLE_SIZE];

	on the stack.  hikeysEnd is a plain uint16, so an image claiming 65535
	there asks for a ~64 kB copy into a ~2.7 kB buffer.

	The bytes are not ours to trust: they come from a file that may have been
	damaged, or -- in S3 mode -- from an endpoint that answers with whatever
	it likes.  The checksum does not help, since it proves only that these are
	the bytes that were stored; anyone who can choose the bytes can compute
	the checksum too, which is what this test does.
	"""

	# Byte offsets into the on-disk page.  The page opens with
	# OrioleDBOndiskPageHeader (16 bytes: checkpointNum, compress_page_size,
	# compress_version, page_version, checkSum, reserved), and the rest of
	# BTreePageHeader follows: undoLocation, csn, rightLink, the flags word,
	# then the uint16s maxKeyLen, prevInsertOffset, chunksCount, itemsCount,
	# hikeysEnd, dataSize.  Cross-checked below against a real page rather
	# than trusted, so a header change cannot make this test quietly corrupt
	# some other field.
	CHECKSUM_OFFSET = 8
	CHUNKS_COUNT_OFFSET = 48
	ITEMS_COUNT_OFFSET = 50
	HIKEYS_END_OFFSET = 52
	DATA_SIZE_OFFSET = 54

	BLCKSZ = 8192
	# BTREE_PAGE_MAX_CHUNKS
	MAX_CHUNKS = (512 - 56) // (8 + 4)

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "orioledb.main_buffers = 8MB\n"
		    "checkpoint_timeout = 1h\n")

	def build_tree(self, node):
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_pages (\n"
		    "  id int NOT NULL,\n"
		    "  v text NOT NULL,\n"
		    "  PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_pages SELECT g, repeat('x', 400) "
		    "FROM generate_series(1, 20000) g;\n"
		    "CHECKPOINT;\n")
		datoid, relnode = node.execute(
		    'postgres', """
			SELECT (SELECT oid FROM pg_database WHERE datname='postgres'),
			       (SELECT relfilenode FROM pg_class
			        WHERE relname='o_pages_pkey');
		""")[0]
		path = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                    str(relnode))
		self.assertTrue(os.path.exists(path), f"no data file at {path}")
		return path

	def read_page(self, path, page_no):
		with open(path, 'rb') as f:
			f.seek(page_no * self.BLCKSZ)
			page = f.read(self.BLCKSZ)
		self.assertEqual(len(page), self.BLCKSZ)
		return bytearray(page)

	def check_offsets(self, page):
		"""Prove the offsets above still point where they are believed to.

		The checksum is the strong half: recomputing the stored value from an
		untouched page means both the field offset and the algorithm match the
		server's, which is what makes the forgery below valid rather than just
		corrupt.
		"""
		chunks = struct.unpack_from('<H', page, self.CHUNKS_COUNT_OFFSET)[0]
		items = struct.unpack_from('<H', page, self.ITEMS_COUNT_OFFSET)[0]
		hikeys_end = struct.unpack_from('<H', page, self.HIKEYS_END_OFFSET)[0]
		data_size = struct.unpack_from('<H', page, self.DATA_SIZE_OFFSET)[0]
		stored = struct.unpack_from('<H', page, self.CHECKSUM_OFFSET)[0]

		self.assertTrue(
		    1 <= chunks <= self.MAX_CHUNKS, f"chunksCount {chunks} at offset "
		    f"{self.CHUNKS_COUNT_OFFSET}: header layout moved")
		self.assertTrue(0 < items <= 1000, f"itemsCount {items}")
		self.assertTrue(0 < data_size <= self.BLCKSZ, f"dataSize {data_size}")
		self.assertTrue(0 < hikeys_end <= data_size,
		                f"hikeysEnd {hikeys_end} vs dataSize {data_size}")

		zeroed = bytearray(page)
		struct.pack_into('<H', zeroed, self.CHECKSUM_OFFSET, 0)
		self.assertEqual(
		    page_checksum(bytes(zeroed)), stored,
		    "recomputed checksum does not match the stored one: the "
		    "algorithm or the field offset has changed")

	def poison_page(self, path, page_no, hikeys_end):
		"""Set hikeysEnd on one page and re-checksum it, so the image is
		exactly what a store that chose its own bytes would hand back."""
		page = self.read_page(path, page_no)
		self.check_offsets(page)

		struct.pack_into('<H', page, self.HIKEYS_END_OFFSET, hikeys_end)
		struct.pack_into('<H', page, self.CHECKSUM_OFFSET, 0)
		struct.pack_into('<H', page, self.CHECKSUM_OFFSET,
		                 page_checksum(bytes(page)))
		with open(path, 'r+b') as f:
			f.seek(page_no * self.BLCKSZ)
			f.write(page)

	def test_oversized_hikey_is_refused(self):
		node = self.node
		node.start()
		path = self.build_tree(node)
		node.stop()

		# A page in the middle of the file, so it is a live tree page that a
		# scan has to read, and not the root the cluster opens with.
		self.poison_page(path, 10, 0xFFFF)

		node.start()
		with self.assertRaises(Exception) as caught:
			node.execute("SELECT count(*) FROM o_pages;")
		self.assertIn("invalid", str(caught.exception).lower())

		# Say which check refused it, so this cannot pass on some unrelated
		# error: the WARNING carries the invariant and the values that broke
		# it, the way a checksum mismatch does.
		with open(node.pg_log_file) as f:
			log = f.read()
		self.assertIn("invalid B-tree page structure", log)
		self.assertIn("hikeysEnd is 65535", log)

		# Refused, not copied: the cluster is still there to say so.
		self.assertEqual(node.execute("SELECT 1;")[0][0], 1)
		node.stop()
