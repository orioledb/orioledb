#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest


class PartialIndexRecoveryTest(BaseTest):
	"""
	A row that leaves or enters a partial index through a non-key column
	(the index key itself unchanged) must be reflected in the index after
	crash recovery replays the UPDATE, exactly as it was before the crash.
	"""

	PREDICATES = [
	    ('o_test_partial_flip_live', 'NOT deleted'),
	    ('o_test_partial_flip_starting', "state = 'starting' AND NOT deleted"),
	    ('o_test_partial_flip_open', 'closed_at IS NULL'),
	    ('o_test_partial_flip_ttl', 'NOT deleted AND ttl IS NOT NULL'),
	]

	def counts(self, node, force_index):
		if force_index:
			gucs = (
			    "SET enable_seqscan = off; SET enable_indexscan = on; "
			    "SET enable_indexonlyscan = on; SET enable_bitmapscan = on;")
		else:
			gucs = (
			    "SET enable_seqscan = on; SET enable_indexscan = off; "
			    "SET enable_indexonlyscan = off; SET enable_bitmapscan = off;")
		result = {}
		for (name, pred) in self.PREDICATES:
			result[name] = node.execute(
			    gucs + " SELECT count(*), string_agg(id, ',' ORDER BY id) "
			    "FROM o_test_partial_flip WHERE " + pred)[0]
		return result

	def check_indexes_match_table(self, node):
		via_index = self.counts(node, True)
		via_seqscan = self.counts(node, False)
		self.assertEqual(via_seqscan, via_index)
		return via_index

	def test_partial_index_predicate_flip_recovery(self):
		node = self.node
		node.start()
		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE o_test_partial_flip (
				id text PRIMARY KEY,
				grp text NOT NULL,
				state text NOT NULL,
				deleted bool NOT NULL DEFAULT false,
				closed_at timestamptz,
				ttl bigint
			) USING orioledb;
			CREATE INDEX o_test_partial_flip_live
				ON o_test_partial_flip (grp) WHERE NOT deleted;
			CREATE INDEX o_test_partial_flip_starting
				ON o_test_partial_flip (grp)
				WHERE state = 'starting' AND NOT deleted;
			CREATE INDEX o_test_partial_flip_open
				ON o_test_partial_flip (id) WHERE closed_at IS NULL;
			CREATE INDEX o_test_partial_flip_ttl
				ON o_test_partial_flip (grp) INCLUDE (id, ttl)
				WHERE NOT deleted AND ttl IS NOT NULL;
			CHECKPOINT;
		""")
		# Everything below is replayed from WAL by crash recovery.
		node.safe_psql("""
			INSERT INTO o_test_partial_flip (id, grp, state, ttl)
				SELECT 'r-' || i, 'g-' || (i % 3), 'starting', 3600
					FROM generate_series(1, 300) i;
			-- leave the 'starting' index; key column unchanged
			UPDATE o_test_partial_flip SET state = 'running'
				WHERE substr(id, 3)::int > 10;
			-- leave the live, open and ttl indexes; key columns unchanged
			UPDATE o_test_partial_flip SET deleted = true, closed_at = now()
				WHERE substr(id, 3)::int % 2 = 0;
			-- ... via ON CONFLICT DO UPDATE too
			INSERT INTO o_test_partial_flip (id, grp, state, ttl)
				VALUES ('r-3', 'g-0', 'running', 3600)
				ON CONFLICT (id) DO UPDATE SET deleted = true, closed_at = now();
			-- re-enter the live and open indexes; key columns unchanged
			UPDATE o_test_partial_flip SET deleted = false, closed_at = NULL
				WHERE id IN ('r-2', 'r-4');
			-- drop the ttl and keep everything else
			UPDATE o_test_partial_flip SET ttl = NULL WHERE id = 'r-5';
			-- purge some soft-deleted rows outright
			DELETE FROM o_test_partial_flip
				WHERE deleted AND substr(id, 3)::int % 4 = 0;
		""")
		before = self.check_indexes_match_table(node)
		self.assertEqual(151, before['o_test_partial_flip_live'][0])

		node.stop(['-m', 'immediate'])
		node.start()

		after = self.check_indexes_match_table(node)
		self.assertEqual(before, after)
		node.stop()
