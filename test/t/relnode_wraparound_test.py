#!/usr/bin/env python3
# coding: utf-8

from threading import Thread

from .base_test import BaseTest

MAX_RELNODE = 4294967295


class RelnodeWraparoundTest(BaseTest):
	"""Enumerating trees must terminate at the largest relnode.

	o_tables_foreach_oids() and o_indices_foreach_oids() step from one tree
	to the next by adding one to the relnode of the tree they just handled,
	and the chunks of a single tree are bounded the same way -- relnode + 1
	with chunk number zero.  At UINT32_MAX that sum wraps to zero, which is
	below where the scan started rather than above where it finished: the
	enumeration returns to the first tree of the tablespace and walks the
	same set for ever, and a value's chunks fall outside their own bounds.
	An assert build trips the repeat assertion in the loop instead of
	spinning, and a checkpoint drives the same enumeration, so neither
	backends nor checkpoints get past it.

	A relnode is a relfilenumber, and pg_upgrade pins those through
	binary_upgrade_set_next_heap_relfilenode(), which is what this test uses
	to put a table exactly at the boundary.  The extension has to be created
	before the server enters binary upgrade mode, since creating its views
	needs OIDs that mode insists on being told.
	"""

	def _start_binary_upgrade(self):
		self.node.stop()
		self.node.start(params=['-o', '-b'])

	def _bounded(self, work, what, timeout=60):
		"""Run something that hangs when the wraparound is unfixed."""
		thread = Thread(target=work, daemon=True)
		thread.start()
		thread.join(timeout=timeout)
		if thread.is_alive():
			self.node.stop(['-m', 'immediate'])
			self.fail("%s did not finish within %d s" % (what, timeout))

	def test_max_relnode_enumeration_terminates(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION orioledb;")
		self._start_binary_upgrade()

		# The wraparound bites the CREATE below as well, through the bound
		# oTablesGetNextKey() puts on the new table's own chunks.  That loop
		# never reaches a CHECK_FOR_INTERRUPTS, so statement_timeout does not
		# end it -- run it off-thread and report the hang instead of joining
		# CI to it.
		self._bounded(
		    lambda: node.safe_psql(
		        'postgres', """
			SELECT binary_upgrade_set_next_pg_type_oid(930001);
			SELECT binary_upgrade_set_next_array_pg_type_oid(930002);
			SELECT binary_upgrade_set_next_heap_pg_class_oid(930003);
			SELECT binary_upgrade_set_next_heap_relfilenode(%d);
			CREATE TABLE o_maxrelnode (id int NOT NULL, v int) USING orioledb;
			INSERT INTO o_maxrelnode VALUES (1, 1), (2, 2);
		""" % MAX_RELNODE), "creating a table at the largest relnode")

		con = node.connect()
		counted = {}
		try:
			# Without the fix this never comes back; the timeout turns that
			# into a failure rather than a hung run on a build whose asserts
			# are off.
			def enumerate_trees():
				counted['tables'] = con.execute(
				    "SELECT count(*) FROM orioledb_table_oids()"
				    " WHERE relnode = %d;" % MAX_RELNODE)[0][0]
				counted['indices'] = con.execute(
				    "SELECT count(*) FROM orioledb_index_oids()"
				    " WHERE index_relnode = %d;" % MAX_RELNODE)[0][0]
				# The checkpointer walks the same enumeration.
				con.execute("CHECKPOINT;")
				con.commit()
				counted['rows'] = con.execute(
				    "SELECT count(*) FROM o_maxrelnode;")[0][0]

			self._bounded(enumerate_trees, "enumerating the trees")
			self.assertEqual(
			    counted['tables'], 1,
			    "the table at the largest relnode was not"
			    " enumerated")
			self.assertEqual(counted['indices'], 1,
			                 "its primary index was not enumerated")
			self.assertEqual(counted['rows'], 2)
		finally:
			con.close()
