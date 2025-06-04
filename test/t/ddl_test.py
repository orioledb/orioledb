#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor


class DDLTest(BaseTest):

	def test_1(self):
		# TODO: Add test for recovery of tablespaced table
		with self.node as node:
			node.start()
			node.safe_psql("""
				CREATE EXTENSION orioledb;
				CREATE TABLE rll_test
				(
					key int8 not null,
					value int8 not null,
					uniq int8 not null,
					PRIMARY KEY(key),
					UNIQUE(uniq)
				) USING orioledb;
				TRUNCATE rll_test;
				INSERT INTO rll_test (SELECT i, i, i FROM generate_series(1, 10) AS i);
			""")
			con = node.connect(autocommit=True)
			con.execute("""
				SET allow_in_place_tablespaces = true;
			""")
			con.execute("""
				CREATE TABLESPACE regress_tblspace LOCATION '';
			""")
			con.close()

			with node.connect() as con1:
				with node.connect() as con2:
					con1.begin()
					con2.begin()

					# TODO: Concurrent update and rewrite test
					# TODO: Concurrent update and copy data test
					con1.execute("SELECT * FROM rll_test WHERE key = 1 FOR KEY SHARE;")
					# t2 = ThreadQueryExecutor(con2, "ALTER TABLE rll_test SET TABLESPACE regress_tblspace;")
					t2 = ThreadQueryExecutor(con2, "ALTER TABLE rll_test ALTER value TYPE text USING value::text")
					t2.start()
					con1.commit()

					t2.join()
					con2.commit()
			node.stop()
