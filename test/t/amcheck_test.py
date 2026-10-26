#!/usr/bin/env python3
# coding: utf-8
"""
Tests for amcheck / pg_amcheck integration with orioledb tables.
"""

import glob
import os
import subprocess
import time
import unittest

from .base_test import BaseTest, ThreadQueryExecutor, wait_checkpointer_stopevent

# A build with -DCHECK_PAGE_STRUCT (the check_page CI job) checks the layout of
# every page it unlocks, including a page it has just read from disk.  It fails
# an assertion on a page whose layout we broke before bt_index_check() gets to
# the page.  Set CHECK_TYPE=check_page to run the tests against such a build.
CHECK_PAGE_STRUCT = os.environ.get('CHECK_TYPE') == 'check_page'

#TODO future orioledb's version have checksum which we must recalculate
O_PAGE_SIZE = 8192
O_PAGE_FLAGS_OFFSET = 40
O_CHUNKS_COUNT_OFFSET = 48
O_ITEMS_COUNT_OFFSET = 50
O_PAGE_DATASIZE_OFFSET = 54
O_CHUNK_DESC_OFFSET = 56
# on-disk size of BTreeLeafTuphdr (xactInfo/undoLocation bitfields, MAXALIGNed)
O_LEAF_TUPHDR_SIZE = 16
O_BTREE_FLAG_LEFTMOST = 0x1
O_BTREE_FLAG_LEAF = 0x4


def read_uint(page, offset, size):
	return int.from_bytes(page[offset:offset + size], 'little')


def write_uint(page, offset, size, val):
	page[offset:offset + size] = val.to_bytes(size, 'little')


def swap_items(page, first, second, size):
	first_item = page[first:first + size]
	page[first:first + size] = page[second:second + size]
	page[second:second + size] = first_item


def page_flags(page):
	return read_uint(page, O_PAGE_FLAGS_OFFSET, 4) & 0x3F


def first_chunk(page):
	chunks_count = read_uint(page, O_CHUNKS_COUNT_OFFSET, 2)
	desc = read_uint(page, O_CHUNK_DESC_OFFSET, 4)
	start = (desc & 0xFFF) * 4  # shortLocation * 4
	if chunks_count > 1:
		# The itemno of this chunk is the next chunk offset
		next_desc = read_uint(page, O_CHUNK_DESC_OFFSET + 4, 4)
		items = (next_desc >> 12) & 0x3FF
		end = (next_desc & 0xFFF) * 4
	else:
		items = read_uint(page, O_ITEMS_COUNT_OFFSET, 2)
		end = read_uint(page, O_PAGE_DATASIZE_OFFSET, 2)

	return start, end, items


def swap_first_chunk_locations(page):
	if not page_flags(page) & O_BTREE_FLAG_LEAF:
		return False

	if read_uint(page, O_CHUNKS_COUNT_OFFSET, 2) < 2:
		return False

	first = read_uint(page, O_CHUNK_DESC_OFFSET, 4)
	second = read_uint(page, O_CHUNK_DESC_OFFSET + 4, 4)
	write_uint(page, O_CHUNK_DESC_OFFSET, 4,
	           (first & ~0xFFF) | (second & 0xFFF))
	write_uint(page, O_CHUNK_DESC_OFFSET + 4, 4,
	           (second & ~0xFFF) | (first & 0xFFF))

	return True


def swap_item_offsets(page):
	if not page_flags(page) & O_BTREE_FLAG_LEAF:
		return False

	chunk_start, _, chunk_items = first_chunk(page)

	if chunk_items < 2:
		return False

	first = read_uint(page, chunk_start, 2)
	second = read_uint(page, chunk_start + 2, 2)
	write_uint(page, chunk_start, 2, second)
	write_uint(page, chunk_start + 2, 2, first)

	return True


def swap_first_leaf_items(page):
	if not page_flags(page) & O_BTREE_FLAG_LEAF:
		return False

	chunk_start, chunk_end, chunk_items = first_chunk(page)
	if chunk_items < 2:
		return False

	items = [read_uint(page, chunk_start + 2 * j, 2) for j in range(2)]
	offsets = [item & 0x3fff for item in items]
	flags = [item >> 14 for item in items]
	if chunk_items > 2:
		second_end = read_uint(page, chunk_start + 4, 2) & 0x3FFF
	else:
		second_end = chunk_end - chunk_start

	size = offsets[1] - offsets[0]
	if second_end - offsets[1] != size or flags[0] != flags[1]:
		return False

	first = chunk_start + offsets[0]
	second = chunk_start + offsets[1]
	swap_items(page, first, second, size)

	return True


def mark_leaf_leftmost(page):
	flags = page_flags(page)
	if not flags & O_BTREE_FLAG_LEAF or flags & O_BTREE_FLAG_LEFTMOST:
		return False
	word = read_uint(page, O_PAGE_FLAGS_OFFSET, 4)
	write_uint(page, O_PAGE_FLAGS_OFFSET, 4, word | O_BTREE_FLAG_LEFTMOST)
	return True


def mark_first_leaf_deleted(page):
	"""Soft-delete the first leaf item so it no longer counts as an index
	entry (its tuple data stays on the page, structure remains valid)."""
	if not page_flags(page) & O_BTREE_FLAG_LEAF:
		return False

	chunk_start, chunk_end, chunk_items = first_chunk(page)
	if chunk_items < 1:
		return False

	# Item 0 offset is stored right at the start of the first chunk.
	first_item_off = read_uint(page, chunk_start, 2) & 0x3FFF
	item_data = chunk_start + first_item_off
	if chunk_items > 1:
		item_end = read_uint(page, chunk_start + 2, 2) & 0x3FFF
	else:
		item_end = chunk_end - chunk_start

	# BTreeLeafTuphdr: xactInfo:61, deleted:2, chainHasLocks:1 in the first
	# 8 bytes.  "deleted" is bits 61-62.
	word = read_uint(page, item_data, 8)
	if (word >> 61) & 0x3:
		return False  # already deleted

	# Like a real delete, count the item in the vacated bytes of the page
	# (field2, the bits 17-31 of the flags word), which builds with
	# -DCHECK_PAGE_STATS check.  A deleted item counts with its whole size, and
	# an item that was never updated in place has no free space to count yet.
	flags_word = read_uint(page, O_PAGE_FLAGS_OFFSET, 4)
	vacated = (flags_word >> 17) + item_end - first_item_off
	if vacated >= 1 << 15:
		return False
	write_uint(page, O_PAGE_FLAGS_OFFSET, 4,
	           (flags_word & 0x1FFFF) | (vacated << 17))

	word |= (1 << 61)  # BTreeLeafTupleDeleted
	write_uint(page, item_data, 8, word)
	return True


def corrupt_secondary_pk(page, key_len):
	"""XOR one byte of the primary-key value (tail) of the first leaf item.
	Changing the PK breaks the row-to-index match without disturbing key
	ordering, so only the heapallindexed check reports it."""
	if not page_flags(page) & O_BTREE_FLAG_LEAF:
		return False

	chunk_start, _, chunk_items = first_chunk(page)
	if chunk_items < 1:
		return False

	first_item_off = read_uint(page, chunk_start, 2) & 0x3FFF
	# tuple data starts after BTreeLeafTuphdr; the PK tail follows the key
	pk_start = chunk_start + first_item_off + O_LEAF_TUPHDR_SIZE + key_len

	page[pk_start] ^= 0xFF
	return True


#
# OrioleDBOndiskPageHeader.checkSum recomputation.
#
# The on-disk page begins with OrioleDBOndiskPageHeader (16 bytes); the
# 16-bit checkSum lives at offset 8.  OrioleDB always enables page
# checksums, so a page read back from disk is verified against that field.
# After corrupting a page in place we must recompute the checksum over the
# (now modified) full 8192-byte page with the field zeroed, exactly as
# check_orioledb_page_checksum() does on load, or the load fails with a
# "page checksum mismatch" instead of reaching the structure check.
#
# The algorithm mirrors oriole_checksum_block() (a 32-way FNV variant, the
# same scheme as PostgreSQL's pg_checksum_block).
#
FNV_PRIME = 16777619
N_SUMS = 32
MASK32 = 0xFFFFFFFF
O_ONDISK_CHECKSUM_OFFSET = 8
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
	words = [read_uint(block, idx * 4, 4) for idx in range(2048)]
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


def recalc_page_checksum(page):
	"""
	Recompute OrioleDBOndiskPageHeader.checkSum for an on-disk page and
	store it back (mutates the bytearray in place).  Call this after applying
	a corruption so the page still passes checksum verification on load.
	"""
	write_uint(page, O_ONDISK_CHECKSUM_OFFSET, 2, 0)
	checksum = (_oriole_checksum_block(page) % 65535) + 1
	write_uint(page, O_ONDISK_CHECKSUM_OFFSET, 2, checksum & 0xFFFF)


class AmcheckTest(BaseTest):

	def test_verify_orioledb(self):
		"""verify_orioledb returns no rows on a healthy relation"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			CREATE INDEX o_t_v_idx ON o_t (v);
			INSERT INTO o_t SELECT i, 'v' || i FROM generate_series(1, 100) i;
			CHECKPOINT;
		""")

		rows = node.execute('postgres',
		                    "SELECT * FROM verify_orioledb('o_t'::regclass);")
		self.assertEqual(rows, [])

		# Thorough variant (force_file_check) also clean.
		rows = node.execute(
		    'postgres',
		    "SELECT * FROM verify_orioledb('o_t'::regclass, true);")
		self.assertEqual(rows, [])

	def test_verify_orioledb_no_false_positive_with_broken_split(self):
		"""Pre-fix, a rightlink pointing to a BROKEN_SPLIT page made
		verify_orioledb emit 'BTree has a broken split.' and return a
		spurious ('o_t_pkey', 'check failed') row, even though a broken
		split is a benign in-progress state finished by the next inserter
		or checkpoint. This test forces a split failure via the split_fail
		stopevent and asserts the default mode is clean both before and
		after the fixup checkpoint."""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_t (id text PRIMARY KEY) USING orioledb;
			INSERT INTO o_t SELECT repeat('x', 400) || id
				FROM generate_series(1, 20) id;
			CHECKPOINT;
		""")

		con_set = node.connect()
		con_set.execute("SELECT pg_stopevent_set('split_fail', 'true');")

		# An INSERT that triggers a split now errors out; the error
		# cleanup hook marks the in-progress split as broken.
		con_ins = node.connect()
		con_ins.execute("SET orioledb.enable_stopevents = true;")
		with self.assertRaises(Exception):
			con_ins.execute("INSERT INTO o_t SELECT repeat('x', 400) || id "
			                "FROM generate_series(131, 139) id;")
		con_ins.close()
		con_set.execute("SELECT pg_stopevent_reset('split_fail');")
		con_set.close()

		# Pre-fix, the broken-split walk emits a NOTICE and the function
		# returns a 'check failed' row.
		rows = node.execute("SELECT * FROM verify_orioledb('o_t'::regclass);")
		self.assertEqual(rows, [], f"verify returned false positive: {rows!r}")

		# A CHECKPOINT completes the pending split. Both modes clean now.
		node.safe_psql('postgres', "CHECKPOINT;")
		self.assertEqual(
		    node.execute("SELECT * FROM verify_orioledb('o_t'::regclass);"),
		    [])
		self.assertEqual(
		    node.execute(
		        "SELECT * FROM verify_orioledb('o_t'::regclass, true);"), [])
		node.stop()

	def test_verify_heapam_rejects_orioledb(self):
		"""verify_heapam() must error on non-heap relations"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_t (k int PRIMARY KEY) USING orioledb;
		""")
		with self.assertRaises(Exception) as cm:
			node.execute('postgres',
			             "SELECT * FROM verify_heapam('o_t'::regclass);")
		self.assertIn("only heap AM is supported", str(cm.exception))

	def test_pg_amcheck(self):
		"""pg_amcheck on a healthy orioledb table exits 0 with no output"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_t (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			INSERT INTO o_t SELECT i, 'v' || i FROM generate_series(1, 50) i;
			CHECKPOINT;
		""")

		res = subprocess.run([
		    'pg_amcheck', '-d', 'postgres', '-t', 'public.o_t', '-p',
		    str(node.port), '-h', node.host
		],
		                     capture_output=True,
		                     text=True)
		self.assertEqual(
		    res.returncode, 0, f"pg_amcheck failed: stdout={res.stdout!r} "
		    f"stderr={res.stderr!r}")
		self.assertEqual(res.stdout, '')

	def test_pg_amcheck_whole_database(self):
		"""pg_amcheck on a whole clean database mixing heap/orioledb tables,
		secondary indexes and sequences"""
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;

			CREATE SEQUENCE h_seq;
			CREATE TABLE h_t (
				k int PRIMARY KEY DEFAULT nextval('h_seq'),
				v text NOT NULL
			);
			CREATE INDEX h_t_v_idx ON h_t (v);
			INSERT INTO h_t (v)
				SELECT 'h' || i FROM generate_series(1, 100) i;

			CREATE SEQUENCE o_seq;
			CREATE TABLE o_t (
				k int PRIMARY KEY DEFAULT nextval('o_seq'),
				v text NOT NULL
			) USING orioledb;
			CREATE INDEX o_t_v_idx ON o_t (v);
			INSERT INTO o_t (v)
				SELECT 'o' || i FROM generate_series(1, 100) i;

			CHECKPOINT;
		""")

		res = subprocess.run([
		    'pg_amcheck', '-d', 'postgres', '-p',
		    str(node.port), '-h', node.host
		],
		                     capture_output=True,
		                     text=True)
		self.assertEqual(
		    res.returncode, 0, f"pg_amcheck failed: stdout={res.stdout!r} "
		    f"stderr={res.stderr!r}")
		self.assertEqual(res.stdout, '')

	def test_verify_orioledb_corruption(self):
		"""
		Corrupting the on-disk CheckpointFileHeader.datafileLength of an
		index's .map file makes check_btree see a mismatch between the
		in-memory busy/free extent counts and the loaded data_file_len.
		The function then emits one row per failed index.
		"""
		import struct

		node = self.node
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_corrupt (
				k int PRIMARY KEY,
				v text NOT NULL
			) USING orioledb;
			INSERT INTO o_corrupt
				SELECT i, repeat('x', 200) FROM generate_series(1, 1000) i;
			CHECKPOINT;
		""")

		datoid, pkey_relnode = node.execute(
		    'postgres', """
			SELECT (SELECT oid FROM pg_database WHERE datname='postgres'),
			       (SELECT relfilenode FROM pg_class
			        WHERE relname='o_corrupt_pkey');
		""")[0]

		node.stop()

		# Pick the latest .map file and overwrite its datafileLength field
		# (offset 24 in struct CheckpointFileHeader) with a bogus value.
		# On restart, recovery loads this wrong length into the meta page;
		# the next check_btree finds busy+free != data_file_len.
		pattern = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                       f'{pkey_relnode}-*.map')
		map_files = sorted(glob.glob(pattern))
		self.assertTrue(map_files, f"no .map files matched {pattern}")
		with open(map_files[-1], 'r+b') as f:
			f.seek(24)
			f.write(struct.pack('<Q', 0xDEADBEEF))

		node.start()
		rows = node.execute(
		    'postgres',
		    "SELECT * FROM verify_orioledb('o_corrupt'::regclass);")
		self.assertTrue(
		    rows, "verify_orioledb should have reported at least one "
		    "corrupt index")
		self.assertTrue(any('check failed' in r[1] for r in rows),
		                f"unexpected verify_orioledb output: {rows!r}")

	def verify_orioledb_page_checksum_base(self, corrupt, compress=False):
		"""
		Shared helper for the page-checksum tests. OrioleDB page checksums are
		always enabled, so we create + checkpoint a table, optionally corrupt a
		page byte on disk, restart, and assert on the log: a corrupted page
		must yield a 'page checksum mismatch' WARNING, a clean one must not.

		With compress=True the table is created WITH (compress = 1) and filled
		with both compressible and incompressible rows, so the on-disk image
		exercises both the compressed-extent path and the full-block
		(len == 1) path in read_page_from_disk / write_page_to_disk.
		"""
		import random
		import struct

		node = self.initNode(self.getBasePort(),
		                     initdb_args=["--no-locale", "--encoding=UTF8"])
		self.node = node

		node.start()
		if compress:
			row_count = 1400
			node.safe_psql(
			    'postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;
				CREATE TABLE o_corrupt (
					k int PRIMARY KEY,
					v bytea NOT NULL
				) USING orioledb WITH (compress = 1);
				INSERT INTO o_corrupt
					SELECT i, decode(repeat('78', 200), 'hex')
					FROM generate_series(1, 1000) i;
			""")
			# Incompressible rows so some leaf pages stay above the block size
			# and get stored full-block (the len == 1 path). A fixed-seed PRNG
			# keeps the run reproducible; the assertions don't depend on the
			# actual bytes, only on the pages being incompressible.
			rnd = random.Random(1234)
			con = node.connect()
			for i in range(1001, row_count + 1):
				con.execute("INSERT INTO o_corrupt VALUES (%s, %s)", i,
				            rnd.randbytes(2560))
			con.commit()
			con.close()
			node.safe_psql('postgres', "CHECKPOINT;")
		else:
			row_count = 1000
			node.safe_psql(
			    'postgres', """
				CREATE EXTENSION IF NOT EXISTS orioledb;
				CREATE TABLE o_corrupt (
					k int PRIMARY KEY,
					v text NOT NULL
				) USING orioledb;
				INSERT INTO o_corrupt
					SELECT i, repeat('x', 200) FROM generate_series(1, 1000) i;
				CHECKPOINT;
			""")

		datoid, pkey_relnode = node.execute(
		    'postgres', """
			SELECT (SELECT oid FROM pg_database WHERE datname='postgres'),
			       (SELECT relfilenode FROM pg_class
			        WHERE relname='o_corrupt_pkey');
		""")[0]

		node.stop()

		if corrupt:
			# Pick the page file and write garbage somewhere. On restart we
			# should get a checksum error (checksums are always on).
			pattern = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
			                       f'{pkey_relnode}')
			page_files = sorted(glob.glob(pattern))
			self.assertTrue(page_files, f"no page files matched {pattern}")
			with open(page_files[-1], 'r+b') as f:
				if compress:
					# Compressed extents are variably sized and the file is
					# sparse, so a fixed offset can land in a hole. Flip the
					# last non-zero byte instead: it is guaranteed to sit inside
					# a written extent's checksummed region.
					data = f.read()
					pos = len(data)
					while pos > 0 and data[pos - 1] == 0:
						pos -= 1
					self.assertGreater(pos, 0, "page file is all zeroes")
					f.seek(pos - 1)
					f.write(bytes([data[pos - 1] ^ 0xFF]))
				else:
					f.seek(16000)
					f.write(struct.pack('<Q', 0xDEADBEEF))

		node.start()
		if corrupt:
			# The checksum error is emitted as a WARNING inside
			# read_page_from_disk because WARNING doesn't longjmp.
			# After the cleanup, load_page raises the generic
			# ereport(errcode_for_file_access, "could not read page ...: %m"),
			# whose SQLSTATE comes from errcode_for_file_access() reading
			# errno. The checksum path doesn't set errno, so whatever value
			# happens to be there gets mapped (in practice ENOENT,
			# surfaced as UndefinedFile).
			#
			# Assert on any Exception and grep the server log for the
			# WARNING text to confirm the checksum path fired.
			with self.assertRaises(Exception):
				node.execute(
				    'postgres',
				    "SELECT * FROM verify_orioledb('o_corrupt'::regclass);")
			with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
				self.assertIn("page checksum mismatch", f.read())
		else:
			# Clean round-trip: reading every page back from cold cache must
			# not report a checksum mismatch.
			self.assertEqual(
			    node.execute('postgres',
			                 "SELECT count(*) FROM o_corrupt;")[0][0],
			    row_count)
			with open(os.path.join(node.logs_dir, 'postgresql.log')) as f:
				self.assertNotIn("page checksum mismatch", f.read())

	def test_verify_orioledb_page_corruption(self):
		"""
		Corrupting an on-disk page must produce a 'page checksum mismatch'
		WARNING in the log.
		"""
		self.verify_orioledb_page_checksum_base(corrupt=True)

	def test_verify_orioledb_page_corruption_compressed(self):
		"""
		Corrupting an on-disk page of a compressed table must produce a
		'page checksum mismatch' WARNING, exercising the compressed read path
		in read_page_from_disk.
		"""
		self.verify_orioledb_page_checksum_base(corrupt=True, compress=True)

	def test_verify_orioledb_compressed_checksums_clean(self):
		"""
		A compressed table mixing compressed and full-block (len == 1) pages
		must round-trip through disk with no checksum mismatch.
		"""
		self.verify_orioledb_page_checksum_base(corrupt=False, compress=True)

	def test_verify_orioledb_during_checkpoint(self):
		"""
		With the checkpointer parked inside our pkey tree via a stopevent,
		verify_orioledb must block on the gate (rather than spuriously
		emit "Tree is under checkpoint now") and only complete after the
		checkpoint releases the boundary.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.enable_stopevents = true\n")
		node.start()
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_chkp (
				k int PRIMARY KEY,
				v int NOT NULL
			) USING orioledb;
			INSERT INTO o_chkp
				SELECT i, i FROM generate_series(1, 2000) i;
			CHECKPOINT;
			-- dirty pages so the next CHECKPOINT walks o_chkp_pkey
			UPDATE o_chkp SET v = v + 1;
		""")

		con_chkp = node.connect(autocommit=True)
		con_set = node.connect()

		# Park the next checkpointer inside o_chkp_pkey at first descent.
		con_set.execute("SELECT pg_stopevent_set('checkpoint_step',\n"
		                "'$.action == \"walkDownwards\" && "
		                "$.treeName == \"o_chkp_pkey\"');")

		t_chkp = ThreadQueryExecutor(con_chkp, "CHECKPOINT;")
		t_chkp.start()
		wait_checkpointer_stopevent(node)

		# Capture stderr from psql so any "Tree is under checkpoint now"
		# NOTICE would be visible if the wait failed to kick in.
		verify_proc = subprocess.Popen([
		    'psql', '-p',
		    str(node.port), '-h', node.host, '-d', 'postgres', '-At', '-c',
		    "SELECT * FROM verify_orioledb('o_chkp'::regclass);"
		],
		                               stdout=subprocess.PIPE,
		                               stderr=subprocess.PIPE,
		                               text=True)

		# Must block on the gate while the stopevent holds.
		time.sleep(0.5)
		self.assertIsNone(
		    verify_proc.poll(),
		    "verify_orioledb returned before checkpoint released")

		con_set.execute("SELECT pg_stopevent_reset('checkpoint_step');")
		t_chkp.join()
		stdout, stderr = verify_proc.communicate(timeout=10)

		self.assertNotIn(
		    "Tree is under checkpoint now", stderr,
		    f"verify_orioledb emitted spurious gate notice: {stderr!r}")

		con_chkp.close()
		con_set.close()

	def create_bt_index_check_table(self, node, size):
		node.safe_psql(
		    'postgres', f"""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE EXTENSION IF NOT EXISTS amcheck;
			CREATE TABLE o_bt(i int, v text) USING orioledb;
			CREATE INDEX o_bt_i ON o_bt USING btree(i);
			CREATE INDEX o_bt_v ON o_bt USING btree(v);
			INSERT INTO o_bt SELECT g, g || 'v' FROM generate_series(1, {size}) g;
		""")

	def corrupt_index(self, node, index, corrupt_page):
		"""
		Stop the server, apply changes to the index and start it again
		"""
		datoid, relnode = node.execute(
		    'postgres', f"""
			SELECT (SELECT oid FROM pg_database WHERE datname = 'postgres'),
				   (SELECT relfilenode FROM pg_class WHERE relname = '{index}');
		""")[0]
		node.stop()

		# Apply corrupt_page to every page
		path = os.path.join(node.data_dir, 'orioledb_data', str(datoid),
		                    str(relnode))
		with open(path, 'r+b') as f:
			pos = 0
			while True:
				f.seek(pos)
				page = bytearray(f.read(O_PAGE_SIZE))
				if len(page) < O_PAGE_SIZE:
					break
				if corrupt_page(page):
					recalc_page_checksum(page)
					f.seek(pos)
					f.write(page)
				pos += O_PAGE_SIZE

		node.start()

	def check_corruption(self,
	                     index,
	                     corrupt_page,
	                     message,
	                     size=100000,
	                     heapallindexed=False,
	                     other_index=None,
	                     check_errors=True,
	                     parent_check_errors=True):
		"""
		Common driver for the bt_index_check* corruption tests:

		1. create the standard o_bt table with o_bt_i / o_bt_v indexes,
		2. assert the checks pass on the clean index,
		3. apply corrupt_page to the target index,
		4. assert bt_index_check / bt_index_parent_check (optionally with
		   heapallindexed) report `message`, that other_index is still clean,
		   and that pg_amcheck reports it too.
		"""
		node = self.node
		node.start()
		self.create_bt_index_check_table(node, size)

		arg = ', true' if heapallindexed else ''

		# Healthy: both checks pass.
		node.safe_psql('postgres', f"SELECT bt_index_check('{index}'{arg});")
		node.safe_psql('postgres',
		               f"SELECT bt_index_parent_check('{index}'{arg});")

		self.corrupt_index(node, index, corrupt_page)

		if check_errors:
			with self.assertRaises(Exception) as e:
				node.safe_psql('postgres',
				               f"SELECT bt_index_check('{index}'{arg});")
			self.assertIn(message, str(e.exception))
		else:
			node.safe_psql('postgres',
			               f"SELECT bt_index_check('{index}'{arg});")

		if parent_check_errors:
			with self.assertRaises(Exception) as e:
				node.safe_psql(
				    'postgres',
				    f"SELECT bt_index_parent_check('{index}'{arg});")
			self.assertIn(message, str(e.exception))
		else:
			node.safe_psql('postgres',
			               f"SELECT bt_index_parent_check('{index}'{arg});")

		if other_index is not None:
			node.safe_psql('postgres',
			               f"SELECT bt_index_check('{other_index}'{arg});")

		# pg_amcheck: pass the same option as the check that detects the
		# corruption (default, --parent-check or --heapallindexed).
		amcheck_args = ['-i', index]
		if other_index is not None:
			amcheck_args.append('-i')
			amcheck_args.append(other_index)
		if heapallindexed:
			amcheck_args.append('--heapallindexed')
		elif not check_errors:
			amcheck_args.append('--parent-check')

		res = subprocess.run([
		    'pg_amcheck',
		    '-d',
		    'postgres',
		    '-p',
		    str(node.port),
		    '-h',
		    node.host,
		] + amcheck_args,
		                     capture_output=True,
		                     text=True)

		self.assertIn(message, res.stdout)
		self.assertIn(index, res.stdout)
		if other_index is not None:
			self.assertNotIn(other_index, res.stdout)

	def test_bt_index_check_swap_leaf(self):
		"""
		bt_index_check() reports leaf items out of key order
		"""
		self.check_corruption('o_bt_i',
		                      swap_first_leaf_items,
		                      'is not above the previous item',
		                      other_index='o_bt_v')

	@unittest.skipIf(CHECK_PAGE_STRUCT,
	                 'CHECK_PAGE_STRUCT asserts on loading the broken page')
	def test_bt_index_check_chunk_order_corruption(self):
		"""
		bt_index_check() reports chunks that are out of order
		"""
		self.check_corruption('o_bt_i', swap_first_chunk_locations,
		                      'chunks 0 and 1 are out of order')

	@unittest.skipIf(CHECK_PAGE_STRUCT,
	                 'CHECK_PAGE_STRUCT asserts on loading the broken page')
	def test_bt_index_check_layout_corruption(self):
		"""
		bt_index_check() reports items whose offsets are out of order
		"""
		self.check_corruption('o_bt_i', swap_item_offsets,
		                      'items 0 and 1 of chunk 0 are out of order')

	def test_bt_index_parent_check_corruption(self):
		"""
		Only bt_index_parent_check() reports a page that doesn't match what
		its parent expects of it
		"""
		self.check_corruption('o_bt_i',
		                      mark_leaf_leftmost,
		                      "page is marked leftmost, but it it isn't",
		                      check_errors=False)

	def test_bt_index_check_heapallindexed_missing_entry(self):
		"""
		bt_index_check(heapallindexed) reports a heap row that has no
		matching entry in the secondary index (entry is soft-deleted).
		"""
		self.check_corruption('o_bt_v',
		                      mark_first_leaf_deleted,
		                      'heap tuple lacks matching index tuple',
		                      size=1000,
		                      heapallindexed=True,
		                      other_index='o_bt_i')

	def test_bt_index_check_heapallindexed_value_corruption(self):
		"""
		bt_index_check(heapallindexed) reports a row whose primary-key value
		in the secondary index has been changed.  Corrupting the PK (the
		tail of the secondary leaf tuple) keeps the index structurally valid,
		so only the heapallindexed check fires.
		"""
		# o_bt_i indexes the int column "i" (4 bytes on disk); the PK tail
		# of a secondary leaf tuple starts right after it.
		self.check_corruption('o_bt_i',
		                      lambda page: corrupt_secondary_pk(page, 4),
		                      'heap tuple lacks matching index tuple',
		                      size=1000,
		                      heapallindexed=True,
		                      other_index='o_bt_v')
