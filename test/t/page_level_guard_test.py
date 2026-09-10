#!/usr/bin/env python3
# coding: utf-8

import os
import struct

from test.t.base_test import BaseTest

ORIOLEDB_BLCKSZ = 8192
ORIOLEDB_PAGE_VERSION = 1
ORIOLEDB_MAX_DEPTH = 32
O_BTREE_FLAG_LEAF = 0x0004
FORGED_LEVEL = 40

FNV_PRIME = 16777619
N_SUMS = 32
MASK32 = 0xFFFFFFFF
_checksumBaseOffsets = (
    0x5B1F36E9,
    0xB8525960,
    0x02AB50AA,
    0x1DE66D2A,
    0x79FF467A,
    0x9BB9F8A3,
    0x217E7CD2,
    0x83E13D2C,
    0xF8D4474F,
    0xE39EB970,
    0x42C6AE16,
    0x993216FA,
    0x7B093B5D,
    0x98DAFF3C,
    0xF718902A,
    0x0B1C9CDB,
    0xE58F764B,
    0x187636BC,
    0x5D7B3BB1,
    0xE73DE7DE,
    0x92BEC979,
    0xCCA6C0B2,
    0x304A0979,
    0x85AA43D4,
    0x783125BB,
    0x6CA8EAA2,
    0xE407EAC6,
    0x4B5CFC3E,
    0x9FBF8C76,
    0x15CA20BE,
    0xF2CA9FD3,
    0x959BD756,
)


def _checksum_comp(checksum, value):
	tmp = (checksum ^ value) & MASK32
	return ((tmp * FNV_PRIME) ^ (tmp >> 17)) & MASK32


def _oriole_checksum_block(block):
	words = struct.unpack('<2048I', block)
	sums = list(_checksumBaseOffsets)
	for idx in range(2048):
		sums[idx % N_SUMS] = _checksum_comp(sums[idx % N_SUMS], words[idx])
	for _ in range(2):
		for j in range(N_SUMS):
			sums[j] = _checksum_comp(sums[j], 0)
	result = 0
	for j in range(N_SUMS):
		result ^= sums[j]
	return result & MASK32


def _recompute_checksum(block):
	b = bytearray(block)
	struct.pack_into('<H', b, 8, 0)
	checksum = (_oriole_checksum_block(bytes(b)) % 65535) + 1
	struct.pack_into('<H', b, 8, checksum & 0xFFFF)
	return bytes(b)


def _bitfield_word(block):
	return struct.unpack_from('<I', block, 40)[0]


def _page_level(block):
	if _bitfield_word(block) & O_BTREE_FLAG_LEAF:
		return 0
	return (_bitfield_word(block) >> 6) & 0x7FF


def _set_page_level(block, level):
	b = bytearray(block)
	word = struct.unpack_from('<I', b, 40)[0]
	word = (word & ~(0x7FF << 6)) | ((level & 0x7FF) << 6)
	struct.pack_into('<I', b, 40, word)
	return bytes(b)


def _is_data_file(name):
	if name in ('control', ):
		return False
	for suffix in ('.xid', '.xidmap', '.map'):
		if name.endswith(suffix):
			return False
	return True


def _looks_like_orioledb_page(block):
	if len(block) != ORIOLEDB_BLCKSZ:
		return False
	if block == b'\x00' * ORIOLEDB_BLCKSZ:
		return False
	return block[7] == ORIOLEDB_PAGE_VERSION


class PageLevelGuardTest(BaseTest):
	"""
	Regression coverage for forged B-tree page levels.

	A B-tree page level is read from on-disk storage and used as an index into
	the fixed ORIOLEDB_MAX_DEPTH checkpoint stack.  A forged out-of-range
	level must be rejected with a controlled FATAL instead of an out-of-bounds
	access (which, in cassert builds, surfaces as an Assert trap / SIGABRT and,
	without assertions, as silent memory corruption).
	"""

	def test_forged_root_level_rejected(self):
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		node.safe_psql("""
			CREATE TABLE o_level_guard (
				id integer PRIMARY KEY,
				payload text
			) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO o_level_guard
				SELECT g, repeat('x', 4000)
				FROM generate_series(1, 2000) g;
		""")
		node.safe_psql("CHECKPOINT;")
		node.stop(['-m', 'immediate'])

		forged = self._forge_root_levels(node.data_dir, FORGED_LEVEL)
		self.assertGreater(
		    forged, 0, "no non-leaf B-tree root pages were found to forge")

		started = True
		try:
			node.start()
		except Exception:
			started = False

		if started:
			try:
				node.safe_psql("CHECKPOINT;")
			except Exception:
				pass
			try:
				if node.status() == 'running':
					node.stop(['-m', 'immediate'])
			except Exception:
				pass

		with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
			log = f.read()

		self.assertIn("invalid B-tree root page level", log)
		self.assertNotIn("TRAP: failed Assert", log)
		self.assertNotIn("terminated by signal 6", log)
		self.assertNotIn("Segmentation fault", log)

	def _forge_root_levels(self, data_dir, level):
		odir = os.path.join(data_dir, 'orioledb_data')
		self.assertTrue(os.path.isdir(odir),
		                "orioledb_data directory not found")

		max_level = -1
		locations = []
		for root, dirs, files in os.walk(odir):
			for name in files:
				if not _is_data_file(name):
					continue
				path = os.path.join(root, name)
				try:
					with open(path, 'rb') as fh:
						data = fh.read()
				except OSError:
					continue
				for off in range(0, len(data), ORIOLEDB_BLCKSZ):
					block = data[off:off + ORIOLEDB_BLCKSZ]
					if not _looks_like_orioledb_page(block):
						continue
					page_level = _page_level(block)
					if page_level == 0:
						continue
					if page_level > max_level:
						max_level = page_level
						locations = [(path, off)]
					elif page_level == max_level:
						locations.append((path, off))

		if max_level < 0:
			return 0

		forged = 0
		for path, off in locations:
			with open(path, 'r+b') as fh:
				fh.seek(off)
				block = fh.read(ORIOLEDB_BLCKSZ)
				if not _looks_like_orioledb_page(block):
					continue
				block = _set_page_level(block, level)
				block = _recompute_checksum(block)
				fh.seek(off)
				fh.write(block)
				forged += 1
		return forged
