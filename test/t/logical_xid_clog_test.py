#!/usr/bin/env python3
# coding: utf-8

import unittest

from .base_test import BaseTest

N_ROWS = 10000

# About 6.7 MB per transaction, far above logical_decoding_work_mem
BIG_INSERT = """
	insert into {rel}(data)
		select repeat(md5(a::text), 20)
			from generate_series(1, {n}) f(a);
"""

# Connections held open so the writer gets a high proc number
N_IDLE_CONNS = 40

# orioledb.logical_xid_buffers = 1: one block of 32-bit maps
LOGICAL_XIDS_PER_BUFFER = 8192 // 4 * 32

# Aborted subtransactions to cover the writer's logical xid
N_BURNED_XIDS = 70000

COUNT_INSERTS = """
	select count(*)
		from pg_logical_slot_get_changes('s', NULL, NULL)
		where starts_with(data, 'table public.o_data: INSERT');
"""


@unittest.skipIf(BaseTest.get_pg_version() < 18, "PG18+ only")
@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
                 "'test_decoding' is not installed")
class LogicalXidClogTest(BaseTest):
	"""
	PG18 checks CLOG for the largest transaction before spilling it to
	disk.  OrioleDB logical xids are not real heap xids, so CLOG must not
	be asked about them.
	"""

	setup_sql = """
		create extension if not exists orioledb;
		create table o_data(id serial primary key, data text) using orioledb;
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf(
		    'postgresql.conf', "wal_level = logical\n"
		    "logical_decoding_work_mem = 64kB\n"
		    "max_connections = 100\n")

	def create_slot(self, node):
		node.safe_psql(
		    "select pg_create_logical_replication_slot('s', 'test_decoding');")

	def big_insert(self, node):
		with node.connect() as con:
			con.begin()
			con.execute(BIG_INSERT.format(rel='o_data', n=N_ROWS))
			xid = con.execute("select orioledb_get_current_logical_xid();")
			con.commit()
		return xid[0][0]

	def test_spill_wrapped_logical_xid(self):
		"""
		In a young cluster a high proc number gives a logical xid near the
		top of the xid space.  It has no CLOG page.
		"""
		node = self.node
		node.start()
		node.safe_psql(self.setup_sql)
		self.create_slot(node)

		idle = [node.connect() for _ in range(N_IDLE_CONNS)]
		xid = self.big_insert(node)
		for con in idle:
			con.close()
		self.assertGreater(xid, 2**31)

		self.assertEqual(node.execute(COUNT_INSERTS)[0][0], N_ROWS)

	def test_spill_aborted_logical_xid(self):
		"""
		The logical xid matches an old aborted heap xid.  The transaction
		must not be discarded as aborted.
		"""
		node = self.node
		node.append_conf('postgresql.conf',
		                 "orioledb.logical_xid_buffers = 1\n")
		node.start()

		# Each aborted subtransaction consumes one heap xid
		node.safe_psql(self.setup_sql + f"""
			create table h_burn(i int);
			do $$ begin
				for i in 1..{N_BURNED_XIDS} loop
					begin
						insert into h_burn values (i);
						raise exception 'burn';
					exception when others then null;
					end;
				end loop;
			end $$;
		""")
		self.create_slot(node)

		xid = self.big_insert(node)
		self.assertGreaterEqual(xid, LOGICAL_XIDS_PER_BUFFER)
		self.assertLess(xid, N_BURNED_XIDS)
		self.assertEqual(
		    node.execute(f"select pg_xact_status('{xid}'::xid8);")[0][0],
		    'aborted')

		self.assertEqual(node.execute(COUNT_INSERTS)[0][0], N_ROWS)
