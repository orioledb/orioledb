#!/usr/bin/env python3
# coding: utf-8

import time

from .base_test import BaseTest
from .logical_test import wait_ready

# Each row is ~672 bytes, so 10k rows exceed the 64kB
# logical_decoding_work_mem by far.
BIG_INSERT = """
	insert into {rel}(data)
		select repeat(md5(a::text), 20)
			from generate_series(1, {n}) f(a);
"""

WAIT_TIMEOUT_S = 60


class LogicalStreamingTest(BaseTest):
	"""
	OrioleDB does not support streaming of in-progress transactions.
	o_decoding_startup() turns ctx->streaming off when a decoding context
	starts.  These tests check that no transaction gets streamed, so large
	transactions are spilled to disk and sent at commit.
	"""

	o_relname = "o_data"
	h_relname = "h_data"

	setup_sql = f"""
		create extension if not exists orioledb;
		create table {o_relname}(id serial primary key, data text) using orioledb;
		create table {h_relname}(id serial primary key, data text);
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		self.node.append_conf('postgresql.conf',
		                      "logical_decoding_work_mem = 64kB\n")

	def slot_stats(self, node, slot):
		return node.execute(f"""
			select stream_txns, spill_txns
				from pg_stat_replication_slots
				where slot_name = '{slot}';
		""")[0]

	def wait_count(self, node, rel, expected):
		# Poll instead of catchup(): a crashing walsender never catches up
		deadline = time.time() + WAIT_TIMEOUT_S
		count = None
		while time.time() < deadline:
			count = node.execute(f"select count(*) from {rel};")[0][0]
			if count == expected:
				return count
			time.sleep(0.1)
		return count

	def assert_no_crash(self, node):
		with open(node.pg_log_file) as f:
			log = f.read()
		self.assertNotIn("terminated by signal", log)

	def test_streaming_on_large_transaction(self):
		"""
		Two interlaced OrioleDB transactions exceed logical_decoding_work_mem
		with streaming = on.  They must be spilled, not streamed.
		"""
		o_relname = self.o_relname

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				publisher.safe_psql(self.setup_sql)
				subscriber.safe_psql(self.setup_sql)

				pub = publisher.publish('test_pub', tables=[o_relname])
				sub = subscriber.subscribe(pub, 'test_sub', streaming='on')
				wait_ready(subscriber)

				with publisher.connect() as pub1:
					with publisher.connect() as pub2:
						pub1.begin()
						pub1.execute(BIG_INSERT.format(rel=o_relname, n=10000))

						pub2.begin()
						pub2.execute(BIG_INSERT.format(rel=o_relname, n=10000))
						pub2.commit()

						# Make sure the two transactions are interlaced
						pub1.execute(BIG_INSERT.format(rel=o_relname, n=100))
						pub1.commit()

				sub.catchup()

				self.assertEqual(
				    subscriber.execute(f"select count(*) from {o_relname};")[0]
				    [0], 20100)

				stream_txns, spill_txns = self.slot_stats(
				    publisher, 'test_sub')
				self.assertEqual(stream_txns, 0)
				self.assertGreater(spill_txns, 0)

	def test_streaming_heap_before_oriole(self):
		"""
		A large heap transaction is decoded before the first OrioleDB record.
		It must be spilled, not streamed.  If streaming were disabled only
		when an OrioleDB record is decoded, the heap transaction would be
		streamed and its commit would hit Assert(ctx->streaming).

		  heap xact:   BEGIN -- 10k rows (spilled) -------------- COMMIT
		  oriole xact:                       INSERT, COMMIT
		"""
		h_relname = self.h_relname
		o_relname = self.o_relname

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				publisher.safe_psql(self.setup_sql)
				subscriber.safe_psql(self.setup_sql)

				pub = publisher.publish('test_pub',
				                        tables=[h_relname, o_relname])
				subscriber.subscribe(pub, 'test_sub', streaming='on')
				wait_ready(subscriber)

				with publisher.connect() as heap_con:
					heap_con.begin()
					heap_con.execute(BIG_INSERT.format(rel=h_relname, n=10000))

					# Wait until the decoder evicts the heap transaction
					deadline = time.time() + WAIT_TIMEOUT_S
					while sum(self.slot_stats(publisher, 'test_sub')) == 0:
						self.assertLess(time.time(), deadline)
						time.sleep(0.1)

					publisher.safe_psql(
					    f"insert into {o_relname}(data) values ('x');")
					heap_con.commit()

				self.assertEqual(self.wait_count(subscriber, h_relname, 10000),
				                 10000)
				self.assertEqual(self.wait_count(subscriber, o_relname, 1), 1)
				self.assertEqual(self.slot_stats(publisher, 'test_sub')[0], 0)
				self.assert_no_crash(publisher)

	def test_streaming_mixed_xact(self):
		"""
		A transaction modifies a heap table, then an OrioleDB table.  With
		debug_logical_replication_streaming = immediate, every change would be
		streamed at once, so the heap change would be streamed before the
		OrioleDB record is decoded.  Nothing may be streamed.
		"""
		h_relname = self.h_relname
		o_relname = self.o_relname

		with self.node as node:
			node.start()
			node.safe_psql(self.setup_sql)
			node.safe_psql("""
				select pg_create_logical_replication_slot('s', 'test_decoding');
			""")

			with node.connect() as con:
				con.begin()
				con.execute(f"insert into {h_relname}(data) values ('h');")
				con.execute(f"insert into {o_relname}(data) values ('o');")
				con.commit()

			with node.connect() as con:
				con.execute(
				    "set debug_logical_replication_streaming = immediate;")
				rows = con.execute("""
					select data from pg_logical_slot_get_changes('s', NULL, NULL,
						'stream-changes', '1', 'include-xids', '0');
				""")

			data = [r[0] for r in rows]
			self.assertFalse([d for d in data if "stream" in d])
			self.assertIn(
			    "table public.h_data: INSERT: id[integer]:1 data[text]:'h'",
			    data)
			self.assertIn(
			    "table public.o_data: INSERT: id[integer]:1 data[text]:'o'",
			    data)
			self.assert_no_crash(node)
