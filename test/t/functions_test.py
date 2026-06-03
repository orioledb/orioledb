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
		the (row, page) bytes that the COPY itself wrote into the undo
		log (lastUsedLocation delta).  orioledb_undo_size only reports
		undo file high-water marks that include speculative reserves,
		so we use orioledb_get_undo_meta() instead.
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
			    'seq 1 30000 | awk ''{{printf("%d\\trow_%d\\t%d\\n", $1, $1, $1 * 7)}}''';
			SELECT
			    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'row'),
			    (SELECT lastUsedLocation FROM orioledb_get_undo_meta() WHERE undo_type = 'page'),
			    (SELECT count(*) FROM {table});
		""")[0]
		node.safe_psql("ROLLBACK;")
		return int(row_a) - int(row_b), int(page_a) - int(page_b), int(count)

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

		row_off, page_off, n_off = self._copy_undo_delta(node, "t_off",
		                                                  bulk_on=False)
		row_on, page_on, n_on = self._copy_undo_delta(node, "t_on",
		                                               bulk_on=True)

		self.assertEqual(n_off, 30_000)
		self.assertEqual(n_on, 30_000)

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
