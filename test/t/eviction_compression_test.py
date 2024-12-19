#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest


class EvictionCompressionTest(BaseTest):

	def eviction_simple_base(self, compressed):
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()  # start PostgreSQL
		n = 100000
		step = 1000
		arg1 = "WITH (primary_compress)" if compressed else ""
		arg2 = "WITH (compress)" if compressed else ""
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key integer NOT NULL,
				val integer NOT NULL,
				PRIMARY KEY (key)
			) USING orioledb %s;
			CREATE UNIQUE INDEX o_test_ix1 ON o_test (val) %s;
			""" % (arg1, arg2))

		con = node.connect()
		node.safe_psql(
		    'postgres', "INSERT INTO o_test\n"
		    "	(SELECT id, %s - id FROM generate_series(%s, %s, 1) id);\n" %
		    (str(n), str(1), str(n)))

		for i in range(1, n, step):
			self.assertEqual(
			    con.execute("SELECT val FROM o_test WHERE key = %s;", i)[0][0],
			    n - i)
		for i in range(1, n, step):
			self.assertEqual(
			    con.execute("SELECT key FROM o_test WHERE val = %s;", i)[0][0],
			    n - i)
		con.close()
		node.stop()

	def test_eviction_simple(self):
		self.eviction_simple_base(False)

	def test_eviction_compress_simple(self):
		self.eviction_simple_base(True)

	def eviction_toast_base(self, compressed):
		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()  # start PostgreSQL
		n = 2000
		step = 10
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key integer NOT NULL,
				val text,
				PRIMARY KEY (key)
			) USING orioledb %s;
			""" % ("WITH (toast_compress = 2)" if compressed else ""))

		con = node.connect()

		for i in range(1, n):
			con.execute("INSERT INTO o_test VALUES (%s, %s)", i,
			            self.genString(i, 3000))
			con.commit()

		for i in range(1, n, step):
			self.assertEqual(
			    con.execute("SELECT val FROM o_test WHERE key = %s;", i)[0][0],
			    self.genString(i, 3000))

		con.close()
		node.stop()

	def test_eviction_toast(self):
		self.eviction_toast_base(False)

	def test_eviction_compress_toast(self):
		self.eviction_toast_base(True)

	def test_evict_almost_full_page_when_compressed(self):
		# Based on this post: http://blog.chenshuo.com/2014/05/incompressible-zlibdeflate-data.html
		incompressible = []

		for step in range(1, 65):
			for inc in range(0, step):
				for i in range(inc + 1, 256, step):
					incompressible += [i]
		incompressible = bytes(incompressible)

		node = self.node
		node.append_conf('postgresql.conf', "orioledb.main_buffers = 8MB\n")
		node.start()  # start PostgreSQL
		n = 2000
		step = 10
		node.safe_psql(
		    'postgres', """
			CREATE EXTENSION IF NOT EXISTS orioledb;
			CREATE TABLE IF NOT EXISTS o_test (
				key bytea NOT NULL,
				val bytea,
				PRIMARY KEY (key)
			) USING orioledb WITH (compress = 1);
			""")

		con = node.connect()

		n = 4
		offset = 0

		nchars = 192
		key = incompressible[offset:offset + nchars]
		offset += nchars
		nchars = 240
		val = incompressible[offset:offset + nchars]
		offset += nchars
		con.execute("INSERT INTO o_test VALUES (%s, %s)", key, val)
		con.commit()

		for i in range(1, n):
			nchars = 1
			key = incompressible[offset:offset + nchars]
			offset += nchars
			nchars = 2450
			val = incompressible[offset:offset + nchars]
			offset += nchars
			con.execute("INSERT INTO o_test VALUES (%s, %s)", key, val)
			con.commit()

		con.execute("SELECT orioledb_evict_pages('o_test'::regclass, 0);")

		n = 4
		offset = 0

		nchars = 192
		key = incompressible[offset:offset + nchars]
		offset += nchars
		nchars = 240
		val = incompressible[offset:offset + nchars]
		offset += nchars
		self.assertEqual(
		    bytes(
		        con.execute("SELECT val FROM o_test WHERE key = %s",
		                    key)[0][0]), val)
		con.commit()

		for i in range(1, n):
			nchars = 1
			key = incompressible[offset:offset + nchars]
			offset += nchars
			nchars = 2450
			val = incompressible[offset:offset + nchars]
			offset += nchars
			self.assertEqual(
			    bytes(
			        con.execute("SELECT val FROM o_test WHERE key = %s",
			                    key)[0][0]), val)
			con.commit()

		con.close()
		node.stop()


if __name__ == "__main__":
	unittest.main()
