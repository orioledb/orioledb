#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent


class TruncateUnderCheckpointTest(BaseTest):
	"""
	A TRUNCATE that lands between two of the checkpointer's visits.

	o_btree_load_shmem_internal() keys shared root info by
	(datoid, relnode, tablespace) and, when the entry is missing, creates the
	tree through checkpointable_tree_init(init_shmem = true).  That path sets
	the tree up for chkp_num + 1, i.e. registers it as a participant of the
	checkpoint that is running right now.  Nothing guarantees the checkpointer
	still has that tree ahead of it in o_indices_foreach_oids(): if its walk is
	already past that position, the freshly created tree carries seq bufs for
	the current checkpoint that nobody will ever finalize.

	Result so far: this does NOT break.  Kept as a regression test for the
	interleaving, and as a record of the staging -- the walk order was confirmed
	with orioledb_index_oids(), which lists o_victim (created first) before
	o_park, so parking on o_park really does mean the walk is past o_victim.

	The park point has to be checkpoint_writeback: o_perform_checkpoint() holds
	oTablesMetaLock across the whole o_indices_foreach_oids() walk, and DDL
	takes it too, so a TRUNCATE simply blocks anywhere else in the walk.
	perform_writeback_and_relock() is the one place that releases it -- which is
	precisely why that window exists and why it is the interesting one.
	"""

	def get_reloid(self, node, relname):
		return node.execute(
		    'postgres',
		    "SELECT oid FROM pg_class WHERE relname = '%s';" % relname)[0][0]

	def test_truncate_between_checkpointer_visits(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.enable_stopevents = true\n"
		    "checkpoint_flush_after = 0\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_victim (id int NOT NULL, val text, PRIMARY KEY (id))\n"
		    "	USING orioledb;\n"
		    "CREATE TABLE o_park (id int NOT NULL, val text, PRIMARY KEY (id))\n"
		    "	USING orioledb;\n"
		    "INSERT INTO o_victim SELECT i, repeat('v', 200) FROM generate_series(1, 5000) i;\n"
		    "INSERT INTO o_park SELECT i, repeat('p', 200) FROM generate_series(1, 5000) i;\n"
		)
		node.safe_psql('postgres', "CHECKPOINT;")
		node.safe_psql(
		    'postgres', "UPDATE o_park SET val = repeat('q', 200) WHERE id % 5 = 0;\n"
		    "UPDATE o_victim SET val = repeat('w', 200) WHERE id % 5 = 0;\n")

		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('checkpoint_writeback',\n"
		             "'$.treeName == \"o_park_pkey\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		# o_victim was created before o_park, so the walk (which goes in
		# O_INDICES key order) is already past it while the checkpointer sits in
		# o_park's writeback window.  Replacing o_victim's tree now is the case
		# in question: TRUNCATE gives it a new relfilenode and the INSERT pulls
		# the new tree into shared memory, where checkpointable_tree_init() sets
		# it up for the checkpoint currently running -- one that will never come
		# back to it.
		node.safe_psql(
		    'postgres', "TRUNCATE o_victim;\n"
		    "INSERT INTO o_victim SELECT i, repeat('x', 200) FROM generate_series(1, 3000) i;\n"
		)

		con2.execute("SELECT pg_stopevent_reset('checkpoint_writeback')")
		con2.close()
		t1.join()
		con1.close()

		# A second checkpoint has to cope with whatever the first one left
		# behind for the new tree.
		node.safe_psql('postgres', "CHECKPOINT;")

		self.assertEqual(
		    3000,
		    node.execute('postgres', "SELECT count(*) FROM o_victim;")[0][0])
		self.assertEqual(
		    5000,
		    node.execute('postgres', "SELECT count(*) FROM o_park;")[0][0])

		# ... and the data must survive a crash restart, which is where an
		# unfinalized *.map / *.tmp for the new tree would show up.
		node.stop(['-m', 'immediate'])
		node.start()
		self.assertEqual(
		    3000,
		    node.execute('postgres', "SELECT count(*) FROM o_victim;")[0][0])
		self.assertEqual(
		    5000,
		    node.execute('postgres', "SELECT count(*) FROM o_park;")[0][0])
		node.stop()
