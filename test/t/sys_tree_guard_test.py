#!/usr/bin/env python3
# coding: utf-8

from test.t.base_test import BaseTest

SYS_TREES_DATOID = 1
SYS_TREES_NUM = 23


class SysTreeGuardTest(BaseTest):
	"""
	Regression coverage for unchecked system-tree index values.

	Recovery and page-walking code accepts a serialized relation-node
	value as a system-tree number after checking only the identifier
	type (datoid == SYS_TREES_DATOID).  An out-of-range relnode indexes
	fixed arrays (sysTreesMeta, sysTreesDescrs) without runtime bounds
	validation, potentially crashing recovery or maintenance and
	preventing cluster startup.

	The IS_DEV helper orioledb_test_wal_parse_relation() feeds a
	hand-built WAL container holding a single WAL_REC_RELATION record
	to wal_parse_container() and returns the parse result.  When
	datoid == SYS_TREES_DATOID and relnode is outside 1..SYS_TREES_NUM,
	the parser must reject it with WALPARSE_BAD_TYPE instead of
	accepting an out-of-range system-tree number.
	"""

	def test_out_of_range_sys_tree_relnode_rejected(self):
		"""A WAL_REC_RELATION with datoid=SYS_TREES_DATOID and an
		out-of-range relnode must be rejected as WALPARSE_BAD_TYPE."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")

		con = node.connect()
		try:
			for relnode in (0, SYS_TREES_NUM + 1, 999999):
				result = con.execute(
				    "SELECT orioledb_test_wal_parse_relation(%s::oid, "
				    "%s::oid);", SYS_TREES_DATOID, relnode)[0][0]
				self.assertEqual(
				    result, "bad_type",
				    "out-of-range system-tree relnode %d was not "
				    "rejected by the WAL parser (got %r, expected "
				    "'bad_type')" % (relnode, result))
		finally:
			con.close()
		node.stop()

	def test_valid_sys_tree_relnode_accepted(self):
		"""A WAL_REC_RELATION with datoid=SYS_TREES_DATOID and a
		valid in-range relnode must be accepted by the parser."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")

		con = node.connect()
		try:
			for tree_num in (1, SYS_TREES_NUM):
				result = con.execute(
				    "SELECT orioledb_test_wal_parse_relation(%s::oid, "
				    "%s::oid);", SYS_TREES_DATOID, tree_num)[0][0]
				self.assertEqual(
				    result, "ok",
				    "valid system-tree relnode %d was rejected by "
				    "the WAL parser (got %r, expected 'ok')" %
				    (tree_num, result))
		finally:
			con.close()
		node.stop()

	def test_non_sys_tree_datoid_not_rejected(self):
		"""A WAL_REC_RELATION whose datoid is not SYS_TREES_DATOID
		must not be rejected at the system-tree bounds check, even
		with an unusual relnode, because the relnode is not
		interpreted as a system-tree number in that case."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")

		con = node.connect()
		try:
			result = con.execute(
			    "SELECT orioledb_test_wal_parse_relation(%s::oid, "
			    "%s::oid);", SYS_TREES_DATOID + 1, SYS_TREES_NUM + 1)[0][0]
			self.assertEqual(
			    result, "ok",
			    "non-system-tree datoid was incorrectly rejected by "
			    "the WAL parser (got %r, expected 'ok')" % (result, ))
		finally:
			con.close()
		node.stop()
