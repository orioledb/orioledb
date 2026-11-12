#!/usr/bin/env python3
# coding: utf-8

from test.t.base_test import BaseTest

SYS_TREES_DATOID = 1
SYS_TREES_NUM = 23
FIRST_NORMAL_OBJECT_ID = 16384


class SysTreeGuardTest(BaseTest):
	"""System-tree numbers must not be confused with template1 relations."""

	def test_zero_relnode_rejected(self):
		"""Zero is invalid for both system trees and user relations."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")

		con = node.connect()
		try:
			for datoid in (SYS_TREES_DATOID, SYS_TREES_DATOID + 1):
				result = con.execute(
				    "SELECT orioledb_test_wal_parse_relation(%s::oid, "
				    "%s::oid);", datoid, 0)[0][0]
				self.assertEqual(result, "bad_type")
		finally:
			con.close()
		node.stop()

	def test_template1_user_relnode_accepted(self):
		"""template1 WAL records with user relfilenumbers are not system trees."""
		node = self.node
		node.start()
		node.safe_psql("CREATE EXTENSION orioledb;")
		with node.connect() as con:
			for relnode in (SYS_TREES_NUM + 1, FIRST_NORMAL_OBJECT_ID - 1,
			                FIRST_NORMAL_OBJECT_ID, 999999, 4294967295):
				self.assertEqual(
				    con.execute(
				        "SELECT orioledb_test_wal_parse_relation(%s::oid, %s::oid)",
				        SYS_TREES_DATOID, relnode), [('ok', )])
		node.stop()

	def test_template1_low_relnode_insert(self):
		"""A relnode just above the system-tree range supports insertion and WAL."""
		node = self.node
		node.start()
		node.safe_psql('template1', "CREATE EXTENSION orioledb;")
		node.stop()
		node.start(params=['-o', '-b'])
		# Binary upgrade can assign relfilenumbers below FirstNormalObjectId.
		node.safe_psql(
		    'template1', """
			SELECT binary_upgrade_set_next_pg_type_oid(930001);
			SELECT binary_upgrade_set_next_array_pg_type_oid(930002);
			SELECT binary_upgrade_set_next_heap_pg_class_oid(930003);
			SELECT binary_upgrade_set_next_heap_relfilenode(%d);
			CREATE TABLE low_relnode(i int) USING orioledb;
		""" % (SYS_TREES_NUM + 1))
		self.assertEqual(
		    node.execute('template1',
		                 "SELECT pg_relation_filenode('low_relnode')"),
		    [(SYS_TREES_NUM + 1, )])
		node.safe_psql('template1', "INSERT INTO low_relnode VALUES (1);")
		node.safe_psql('template1', "CHECKPOINT;")
		node.safe_psql('template1', "INSERT INTO low_relnode VALUES (2);")
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(
		    node.execute('template1', "SELECT i FROM low_relnode ORDER BY i"),
		    [(1, ), (2, )])
		self.assertEqual(
		    node.execute('template1',
		                 "SELECT orioledb_tbl_check('low_relnode'::regclass)"),
		    [(True, )])
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
