#!/usr/bin/env python3
# coding: utf-8
# Tests for some user SQL functions

import subprocess

from .base_test import BaseTest
from testgres.exceptions import QueryException


class FunctionTest(BaseTest):

	def test_undo_log_size(self):

		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

            CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO oriole_table(t) select repeat('a', 270) FROM  generate_series(1, 30000) as i;
		""")
		node.safe_psql("""
			UPDATE oriole_table set t = repeat('c', 270) WHERE i > 5000;
		""")

		self.assertGreaterEqual(
		    node.execute(
		        "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'row';"
		    )[0][0], 100000)
		self.assertGreaterEqual(
		    node.execute(
		        "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'page';"
		    )[0][0], 200000)

		node.stop()

	def test_undo_log_size_system(self):

		node = self.node
		node.start()

		node.safe_psql("""
			CREATE EXTENSION IF NOT EXISTS orioledb;

            CREATE TABLE oriole_table (i SERIAL PRIMARY KEY, t text STORAGE PLAIN) USING orioledb;
		""")
		node.safe_psql("""
			INSERT INTO oriole_table(t) select repeat('a', 270) FROM  generate_series(1, 30000) as i;
		""")
		node.safe_psql("""
			UPDATE oriole_table set t = repeat('c', 270) WHERE i > 5000;
		""")
		node.safe_psql("""
		        CREATE INDEX on oriole_table(t);
		        REINDEX TABLE oriole_table;
		        REINDEX TABLE oriole_table;
		        REINDEX TABLE oriole_table;
		""")

		node.safe_psql("""
			CHECKPOINT;
		""")

		self.assertGreaterEqual(
		    node.execute(
		        "SELECT undo_size FROM orioledb_undo_size() WHERE undo_type = 'system';"
		    )[0][0], 10000)

		node.stop()

	def _copy_undo_delta(self, node, table, bulk_on):
		"""
		COPY a fixed payload inside one txn into a fresh table; return
		(row_written, page_written, count_in_txn, pinned_after_commit).
		lastUsedLocation deltas count real per-COPY undo writes;
		orioledb_undo_size reports speculative undo file reservations
		and is unreliable for A/B.  Then run the same payload again
		auto-committed into a second table and report
		lastUsedLocation - cleanedLocation after a no-op query that
		nudges the lazy cleanup pass; a committed COPY has no rollback
		need, so this should reclaim to ~0.
		"""
		setting = "on" if bulk_on else "off"
		node.safe_psql(f"""
			SET orioledb.bulk_insert = {setting};
			CREATE TABLE {table} (id int PRIMARY KEY, v text, w int)
				USING orioledb;
			CREATE INDEX ON {table}(v);
			CREATE INDEX ON {table}(w);
		""")
		row_b, page_b = node.execute("""
			SELECT
			    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
			    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page');
		""")[0]
		row_a, page_a, count = node.execute(f"""
			SET orioledb.bulk_insert = {setting};
			BEGIN;
			COPY {table} FROM PROGRAM
			    'seq -f %.0f 1 30000 | awk ''{{printf("%d\\trow_%d\\t%d\\n", $1, $1, $1 * 7)}}''';
			SELECT
			    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
			    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page'),
			    (SELECT count(*) FROM {table});
		""")[0]
		node.safe_psql("ROLLBACK;")

		# Auto-committed COPY into a fresh table so we can verify
		# pinned undo drops to ~0 after the commit + cleanup nudge.
		node.safe_psql(f"""
			SET orioledb.bulk_insert = {setting};
			CREATE TABLE {table}_c (id int PRIMARY KEY, v text, w int)
				USING orioledb;
			CREATE INDEX ON {table}_c(v);
			CREATE INDEX ON {table}_c(w);
			COPY {table}_c FROM PROGRAM
			    'seq -f %.0f 1 30000 | awk ''{{printf("%d\\trow_%d\\t%d\\n", $1, $1, $1 * 7)}}''';
		""")
		# Poll for the lazy cleanup pass: trivial queries nudge it but
		# each `node.execute` is a fresh psql connection, so it may
		# take a few before the bgwriter / undo cleanup catches up.
		import time as _t
		pinned = None
		for _ in range(20):
			node.execute(f"SELECT count(*) FROM {table}_c;")
			ru, pu, rc, pc = node.execute("""
				SELECT
				    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
				    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page'),
				    (SELECT cleanedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
				    (SELECT cleanedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page');
			""")[0]
			pinned = (int(ru) - int(rc)) + (int(pu) - int(pc))
			if pinned < 64 * 1024:
				break
			_t.sleep(0.1)

		return (int(row_a) - int(row_b),
		        int(page_a) - int(page_b),
		        int(count),
		        pinned)

	def test_undo_log_size_copy(self):
		"""
		Compare row-level and page-level undo bytes written by a COPY
		FROM into a fresh orioledb table with two secondary indexes,
		with orioledb.bulk_insert off vs on.  Run the OFF leg first on a
		fresh node so the OFF baseline is clean; for the ON leg, use a
		different table name so the catalog state from OFF doesn't
		matter -- the rows we measure write to a newly created table's
		btrees and we only diff lastUsedLocation across the COPY.
		"""
		node = self.node
		node.append_conf('postgresql.conf', "checkpoint_timeout = 86400\n")
		node.start()
		node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

		row_off, page_off, n_off, pinned_off = self._copy_undo_delta(
			node, "t_off", bulk_on=False)
		row_on, page_on, n_on, pinned_on = self._copy_undo_delta(
			node, "t_on", bulk_on=True)

		self.assertEqual(n_off, 30_000)
		self.assertEqual(n_on, 30_000)

		# A committed COPY's undo must be reclaimable: the post-cleanup
		# pinned bytes should be ~0 in both legs.  Slack covers
		# whatever the SELECT itself or other miscellany has written.
		SLACK = 64 * 1024
		self.assertLess(pinned_off, SLACK,
		                f"off leg leaked undo after commit + cleanup: "
		                f"{pinned_off}B still pinned")
		self.assertLess(pinned_on, SLACK,
		                f"on leg leaked undo after commit + cleanup: "
		                f"{pinned_on}B still pinned")

		total_off = row_off + page_off
		total_on = row_on + page_on

		# Baseline: per-tuple COPY emits undo on the order of insert size.
		self.assertGreater(total_off, 1_000_000,
		                   f"baseline undo {total_off} unexpectedly small "
		                   f"(row={row_off} page={page_off})")
		# Bulk path must produce strictly less row-level undo (the
		# headline saving: row undo is skipped on bulk-fresh pages).
		# Page-level undo savings depend on data distribution per index;
		# don't pin them here.
		self.assertLess(row_on, row_off,
		                f"bulk_insert=on row undo {row_on} not less than "
		                f"off row undo {row_off}")
		# Total undo should also decrease.
		self.assertLess(total_on, total_off,
		                f"bulk_insert=on undo {total_on} not less than "
		                f"off undo {total_off}")

		node.stop()
