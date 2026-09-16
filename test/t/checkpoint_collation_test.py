#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class CheckpointCollationTest(BaseTest):
	"""The checkpointer must be able to compare a key with its own collation.

	Writing a tree walks it level by level, and above the leaves that means
	comparing keys (checkpoint_internal_pass() -> o_btree_cmp()).  For a
	collatable key the comparison function asks pg_locale.c for the collation,
	which reads pg_collation on its first use in a process.  The checkpointer
	has no transaction to read it in, so with an assertion-enabled build every
	checkpoint of such a tree used to abort it:

	    TRAP: failed Assert("IsTransactionState()"), File: "relcache.c"
	    #2  pg_newlocale_from_collation
	    #3  varstr_cmp / bttextcmp / FunctionCall2Coll
	    #7  o_call_comparator
	    #11 checkpoint_internal_pass
	    #21 CheckpointerMain

	The lookup itself is fine -- OrioleDB answers it from its own system trees
	(o_set_syscache_hooks()) -- so the fix is in two halves: PostgreSQL skips
	the transaction assertion for a hook-served lookup, and the collation is
	resolved once while the comparator is built rather than in the middle of a
	tree walk.

	The tree has to be more than one page deep, which is what the row count
	below is for: a one-page tree is written without comparing anything.
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "checkpoint_timeout = 1h\n"
		    "max_wal_size = 4GB\n")

	def test_checkpoint_a_tree_with_an_icu_collated_key(self):
		node = self.node
		node.start()
		node.safe_psql(
		    'postgres',
		    "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TABLE o_coll (\n"
		    "  k text COLLATE \"unicode\" NOT NULL,\n"
		    "  v int NOT NULL,\n"
		    "  PRIMARY KEY (k)\n"
		    ") USING orioledb;\n"
		    # Enough rows for internal pages, so the checkpointer compares.
		    "INSERT INTO o_coll SELECT md5(g::text), g"
		    " FROM generate_series(1, 2000) g;\n")
		node.safe_psql('postgres', "CHECKPOINT;")

		with open(node.pg_log_file, errors='replace') as f:
			log = f.read()
		died = [
		    line for line in log.splitlines() if 'TRAP' in line
		    or 'checkpointer process' in line and 'terminated' in line
		]
		self.assertEqual(
		    died, [],
		    "the checkpointer did not survive the tree: %s" % (died[:1], ))

		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_coll;")[0][0], 2000)
		self.assertTrue(
		    node.execute("SELECT orioledb_tbl_check('o_coll'::regclass,"
		                 " true);")[0][0])

		# And again after a restart, where the tree is read back from disk.
		node.restart()
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_coll"
		                 " WHERE k > md5('1');")[0][0],
		    node.execute("SELECT count(*) FROM o_coll"
		                 " WHERE k COLLATE \"unicode\" > md5('1');")[0][0])
