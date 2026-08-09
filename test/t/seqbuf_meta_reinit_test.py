#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .base_test import wait_checkpointer_stopevent


class SeqBufMetaReinitTest(BaseTest):
	"""
	Reproduce the seq_buf "finalize-badpage" crash:

	  TRAP: failed Assert("OInMemoryBlknoIsValid(shared->pages[shared->curPageNum])")
	        src/utils/seq_buf.c  <- seq_buf_finalize <- checkpoint_ix

	init_meta_page() wipes the seq_buf page references of BOTH checkpoint
	slots, while evictable_tree_init() re-allocates only the next one
	((chkp_num + 1) % 2).  checkpoint_ix() finalizes the *current* slot
	(chkpNum % 2), so once a tree is (re)loaded into shared memory underneath
	a running checkpoint, that slot is left pointing at freed pages.

	The checkpoint_writeback stop event parks the checkpointer exactly in the
	window where it has released the tree lock, which is where a concurrent
	load can happen.
	"""

	def test_seq_buf_meta_reinit_under_checkpoint(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "checkpoint_flush_after = 0\n"
		    "orioledb.enable_stopevents = true\n"
		    "orioledb.main_buffers = 8MB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE IF NOT EXISTS o_seqbuf (\n"
		    "	id int NOT NULL,\n"
		    "	val text NOT NULL,\n"
		    "	PRIMARY KEY (id)\n"
		    ") USING orioledb;\n"
		    "INSERT INTO o_seqbuf\n"
		    "	(SELECT id, repeat('x', 500) FROM generate_series(1, 20000, 1) id);\n"
		)
		# A first checkpoint so the tree has a .map/.tmp history and the next
		# checkpoint works on an established slot.
		node.safe_psql('postgres', "CHECKPOINT;")
		node.safe_psql(
		    'postgres', "UPDATE o_seqbuf SET val = repeat('y', 500)\n"
		    "	WHERE id % 3 = 0;")

		con1 = node.connect()
		con2 = node.connect()

		con2.execute("SELECT pg_stopevent_set('checkpoint_writeback',\n"
		             "'$.treeName == \"o_seqbuf_pkey\"');")

		t1 = ThreadQueryExecutor(con1, "CHECKPOINT;")
		t1.start()
		wait_checkpointer_stopevent(node)

		# The checkpointer is parked with the tree unlocked.  Push the whole
		# tree (root included) out of shared memory and read it back: the read
		# goes through evictable_tree_init(init_shmem = true), which wipes both
		# slots and restores only the next one.
		node.safe_psql('postgres',
		               "SELECT orioledb_evict_pages('o_seqbuf'::regclass, -1);")
		node.safe_psql('postgres', "SELECT count(*) FROM o_seqbuf;")

		con2.execute("SELECT pg_stopevent_reset('checkpoint_writeback')")
		con2.close()

		# Finalizing the current slot is where it dies.
		t1.join()

		self.assertEqual(
		    20000,
		    node.execute('postgres', "SELECT count(*) FROM o_seqbuf;")[0][0])

		con1.close()
		node.stop()
