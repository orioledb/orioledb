#!/usr/bin/env python3
# coding: utf-8

import os
import struct

from .base_test import BaseTest

from testgres.exceptions import StartNodeException

# Offset of binaryVersion (uint32) in CheckpointControl, and of crc
# (pg_crc32c, uint32) that follows the last field before it.  Derived
# from the struct layout in include/checkpoint/control.h.
BINARY_VERSION_OFFSET = 152
CRC_OFFSET = 164


def _crc32c_table():
	tbl = []
	for i in range(256):
		crc = i
		for _ in range(8):
			crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
		tbl.append(crc)
	return tbl


_CRC32C_TABLE = _crc32c_table()


def crc32c(data):
	"""CRC-32C (Castagnoli), matching PostgreSQL's pg_crc32c."""
	crc = 0xFFFFFFFF
	for b in data:
		crc = _CRC32C_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
	return crc ^ 0xFFFFFFFF


def patch_checkpoint_version(control_path, checkpoint_version):
	"""
	Patch the checkpoint version packed in binaryVersion and recompute CRC.

	binaryVersion packs (binaryVersion & 0xFFFF) | (checkpointVersion << 16).
	"""
	with open(control_path, "rb") as f:
		data = bytearray(f.read())

	old_val = struct.unpack_from("<I", data, BINARY_VERSION_OFFSET)[0]
	bin_ver = old_val & 0xFFFF
	new_val = bin_ver | (checkpoint_version << 16)
	struct.pack_into("<I", data, BINARY_VERSION_OFFSET, new_val)

	new_crc = crc32c(data[:CRC_OFFSET])
	struct.pack_into("<I", data, CRC_OFFSET, new_crc)

	with open(control_path, "wb") as f:
		f.write(data)


class CheckpointVersionTest(BaseTest):

	def test_checkpoint_version_recovery_check(self):
		"""
		After a crash, recovery must refuse to start if the control file's
		checkpoint version exceeds the compiled ORIOLEDB_CHECKPOINT_VERSION.
		"""
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test (id int PRIMARY KEY) USING orioledb;
			INSERT INTO o_test VALUES (1);
		""")
		node.safe_psql("CHECKPOINT;")
		node.stop(['-m', 'immediate'])

		control_path = os.path.join(node.data_dir, "orioledb_data", "control")

		# Back up the original control file.
		with open(control_path, "rb") as f:
			original = f.read()

		# Patch checkpoint version to a value higher than any compiled version.
		patch_checkpoint_version(control_path, 0xFFFE)

		with self.assertRaises(StartNodeException):
			node.start()

		with open(node.pg_log_file) as f:
			log = f.read()
			self.assertIn("checkpoint version", log)

		# Restore the original control file; recovery should succeed.
		with open(control_path, "wb") as f:
			f.write(original)

		node.start()
		val = node.safe_psql("SELECT id FROM o_test;")
		self.assertEqual(val, b"1\n")
		node.stop()
