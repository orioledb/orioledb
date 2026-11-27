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

	def test_checkpoint_preloads_composite_comparator_metadata(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "max_connections = 10\n"
		    "autovacuum_max_workers = 1\n"
		    "max_worker_processes = 2\n"
		    "max_wal_senders = 0\n"
		    "orioledb.debug_disable_pools_limit = true\n"
		    "orioledb.catalog_buffers = 2MB\n")
		node.start()
		node.safe_psql(
		    'postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;\n"
		    "CREATE TYPE o_cmp_key AS"
		    " (r int4range, a int[], m int4multirange);\n"
		    "CREATE TABLE o_cmp (k o_cmp_key PRIMARY KEY, v int NOT NULL)"
		    " USING orioledb;\n"
		    "INSERT INTO o_cmp"
		    " SELECT ROW(int4range(g, g + 1), ARRAY[g, g + 1],"
		    " int4multirange(int4range(g + 2, g + 3)))::o_cmp_key, g"
		    " FROM generate_series(1, 2000) g;\n")

		# Grow the type and class sys trees past the deliberately small catalog
		# pool.  Reading them in key order below then leaves the target type's
		# low-OID leaves on disk.
		fill_count = 1500
		batch_size = 100
		for first in range(0, fill_count, batch_size):
			last = min(first + batch_size, fill_count)
			types = ''.join("CREATE TYPE o_fill_%d AS (x int);\n" % i
			                for i in range(first, last))
			columns = ', '.join("c%d o_fill_%d" % (i, i)
			                    for i in range(first, last))
			node.safe_psql(
			    'postgres',
			    types + "CREATE TABLE o_fill_tbl_%d (%s) USING orioledb;" %
			    (first // batch_size, columns))
		node.safe_psql('postgres', "CHECKPOINT;")

		# Give the checkpointer a cold backend-local typcache.  A full ordered
		# walk of both sys trees also evicts their early leaves from the shared
		# catalog pool before the checkpointer builds and compares o_cmp keys.
		node.restart()
		node.safe_psql(
		    'postgres',
		    "SELECT length(orioledb_sys_tree_structure(8, 'ne'));\n"
		    "SELECT length(orioledb_sys_tree_structure(12, 'ne'));\n"
		    "SELECT length(orioledb_sys_tree_structure(2, 'ne'));\n"
		    "UPDATE o_cmp SET v = v + 1;\n")
		node.safe_psql('postgres', "CHECKPOINT;")

		with open(node.pg_log_file, errors='replace') as f:
			log = f.read()
		self.assertNotIn('failed Assert("!have_locked_pages()")', log)
		self.assertEqual(
		    node.execute("SELECT count(*) FROM o_cmp;")[0][0], 2000)
