#!/usr/bin/env python3
# coding: utf-8

import time
import unittest

from .base_test import BaseTest
from .logical_test import wait_ready

# One row is about 640 bytes, so BIG_ROWS rows are 20 times the publisher's
# logical_decoding_work_mem (64kB).
BIG_ROWS = 2000
WAIT_S = 60
# For checks that need the transaction still in flight
INFLIGHT_WAIT_S = 20

O_REL = "o_data"
SERIAL_REL = "o_serial"
SERIAL_SEQ = f"{SERIAL_REL}_id_seq"
# Sequence cache of the documented workaround (limitation 13): one refill
# covers a whole big transaction, so nextval() takes no heap xid inside it
SERIAL_CACHE = 10 * BIG_ROWS
H_REL = "h_data"
SUB = "test_sub"
SLOT = "s"
# Digest expression for the scenarios that add column c on the publisher
DDL_EXPR = "data || ':' || coalesce(c::text, '')"
LOG_MARKERS = ("ERROR:", "FATAL:", "PANIC:", "TRAP:", "terminated by signal")


def big_insert(rel, n=BIG_ROWS, start=1):
	# Explicit ids: nextval() takes a heap xid at each sequence cache refill,
	# which would turn an OrioleDB-first transaction into a heap-first one.
	return f"""
		insert into {rel}(id, data)
			select a, repeat(md5(a::text), 20)
				from generate_series({start}, {start + n - 1}) f(a);
	"""


def big_serial_insert(n=BIG_ROWS):
	return f"""
		insert into {SERIAL_REL}(data)
			select repeat(md5(a::text), 20)
				from generate_series(1, {n}) f(a);
	"""


class StreamingTestBase(BaseTest):
	"""
	Shared machinery: the publisher/subscriber pair, polling, content digests
	and the publisher-side scenarios.  The test classes below run every
	scenario in one of two harnesses.  Scenarios follow the upstream tests
	src/test/subscription/t/015-019_stream*.pl and
	contrib/test_decoding/sql/stream.sql.

	The tables have no sequence on purpose, see big_insert().
	"""

	publisher_sql = f"""
		create extension if not exists orioledb;
		create table {O_REL}(id int primary key, data text) using orioledb;
		create table {H_REL}(id int primary key, data text);
		create table {SERIAL_REL}(id serial primary key, data text) using orioledb;
	"""
	# Column c is for the DDL scenarios, which add it on the publisher
	subscriber_sql = f"""
		create extension if not exists orioledb;
		create table {O_REL}(id int primary key, data text, c int)
			using orioledb;
		create table {H_REL}(id int primary key, data text);
		create table {SERIAL_REL}(id serial primary key, data text) using orioledb;
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		self.node.append_conf('postgresql.conf',
		                      "logical_decoding_work_mem = 64kB\n")

	@staticmethod
	def immediate_guc():
		# PG16 has the old name of the developer option
		if BaseTest.get_pg_version() < 17:
			return "logical_replication_mode"
		return "debug_logical_replication_streaming"

	def immediate_conf(self):
		return f"{self.immediate_guc()} = immediate\n"

	def start_pair(self, streaming='on', pub_conf='', sub_conf=''):
		publisher = self.node
		if pub_conf:
			publisher.append_conf('postgresql.conf', pub_conf)
		publisher.start()

		subscriber = self.getSubscriber()
		if sub_conf:
			subscriber.append_conf('postgresql.conf', sub_conf)
		subscriber.start()

		publisher.safe_psql(self.publisher_sql)
		subscriber.safe_psql(self.subscriber_sql)
		pub = publisher.publish('test_pub', tables=[O_REL, H_REL, SERIAL_REL])
		sub = subscriber.subscribe(pub, SUB, streaming=streaming)
		wait_ready(subscriber)

		# Let the apply worker go idle once after the initial sync: it marks
		# the tables ready itself and sees that only when it next accepts
		# invalidations, and until then it will not start a parallel apply
		# worker (pa_can_start -> AllTablesyncsReady).
		publisher.safe_psql(
		    f"insert into {H_REL}(id, data) values (0, 'warm-up');")
		self.wait_count(subscriber, H_REL, 1)
		return publisher, subscriber, sub

	def start_decoding(self):
		node = self.node
		node.start()
		node.safe_psql(self.publisher_sql)
		node.safe_psql(f"""
			select pg_create_logical_replication_slot('{SLOT}', 'test_decoding');
		""")
		return node

	def decode(self, node, immediate=False):
		with node.connect() as con:
			if immediate:
				con.execute(f"set {self.immediate_guc()} = immediate;")
			rows = con.execute(f"""
				select data from pg_logical_slot_get_changes('{SLOT}', NULL, NULL,
					'stream-changes', '1', 'include-xids', '1');
			""")
		return [r[0] for r in rows]

	def wait_until(self, cond, timeout=WAIT_S):
		# Returns the last value of cond(); the caller asserts on it
		deadline = time.time() + timeout
		while True:
			value = cond()
			if value or time.time() >= deadline:
				return value
			time.sleep(0.1)

	def count(self, node, rel):
		return node.execute(f"select count(*) from {rel};")[0][0]

	def wait_count(self, node, rel, expected):
		# Poll instead of catchup(): a crashing walsender never catches up
		self.wait_until(lambda: self.count(node, rel) == expected)
		return self.count(node, rel)

	def slot_stats(self, node, slot=SUB):
		rows = node.execute(f"""
			select stream_txns, spill_txns
				from pg_stat_replication_slots
				where slot_name = '{slot}';
		""")
		return rows[0] if rows else (0, 0)

	def parallel_workers(self, node):
		return node.execute("""
			select count(*) from pg_stat_subscription
				where leader_pid is not null;
		""")[0][0]

	def digest(self, node, rel, expr='data'):
		return node.execute(f"""
			select count(*), coalesce(sum(id), 0),
				md5(coalesce(string_agg({expr}, ',' order by id), ''))
				from {rel};
		""")[0]

	def log_errors(self, node):
		with open(node.pg_log_file) as f:
			lines = f.readlines()
		bad = [l for l in lines if any(m in l for m in LOG_MARKERS)]
		return "".join(bad[-20:])

	def assert_no_crash(self, node):
		with open(node.pg_log_file) as f:
			log = f.read()
		self.assertNotIn("terminated by signal", log)
		self.assertNotIn("TRAP:", log)

	def assert_replicated(self, publisher, subscriber, rels, expr='data'):
		for rel in rels:
			expected = self.count(publisher, rel)
			got = self.wait_count(subscriber, rel, expected)
			self.assertEqual(
			    got, expected,
			    f"{rel}: subscriber has {got} rows, publisher {expected}\n"
			    f"publisher log:\n{self.log_errors(publisher)}"
			    f"subscriber log:\n{self.log_errors(subscriber)}")
			self.assertEqual(self.digest(subscriber, rel, expr),
			                 self.digest(publisher, rel, expr), rel)
		self.assert_no_crash(publisher)
		self.assert_no_crash(subscriber)

	def assert_streamed(self, publisher, subscriber, rels):
		# Streamed, or the subscriber caught up without streaming: whichever
		# comes first ends the wait
		def done():
			if self.slot_stats(publisher)[0] >= 1:
				return True
			return all(
			    self.count(subscriber, r) == self.count(publisher, r)
			    for r in rels)

		self.wait_until(done, INFLIGHT_WAIT_S)
		stream_txns, spill_txns = self.slot_stats(publisher)
		self.assertGreaterEqual(
		    stream_txns, 1,
		    f"stream_txns={stream_txns} spill_txns={spill_txns}\n"
		    f"publisher log:\n{self.log_errors(publisher)}")

	def assert_decoded_stream(self, lines, *markers):
		shown = "\n".join(lines[:40])
		self.assertTrue(
		    [l for l in lines if l.startswith("opening a streamed block")],
		    f"nothing was streamed:\n{shown}")
		for marker in markers:
			self.assertTrue([l for l in lines if l.startswith(marker)],
			                f"no '{marker}' line:\n{shown}")

	# Scenarios: publisher-side SQL only

	def sc_large_insert(self, pub):
		pub.safe_psql(f"begin; {big_insert(O_REL)} commit;")

	def sc_interleaved(self, pub):
		with pub.connect() as con1, pub.connect() as con2:
			con1.begin()
			con1.execute(big_insert(O_REL))
			con2.begin()
			con2.execute(big_insert(O_REL, start=10001))
			con2.commit()
			# Make sure the two transactions are interleaved
			con1.execute(big_insert(O_REL, 100, start=20001))
			con1.commit()

	def sc_rollback(self, pub):
		pub.safe_psql(f"begin; {big_insert(O_REL)} rollback;")
		# A later commit proves the subscription is still alive
		pub.safe_psql(f"insert into {O_REL}(id, data) values (1, 'alive');")

	def sc_savepoint_rollbacks(self, pub):
		# 018_stream_subxact_abort.pl shape: 600 rows
		pub.safe_psql(f"""
			begin;
			{big_insert(O_REL, 500)}
			savepoint s1;
			{big_insert(O_REL, 500, start=1001)}
			savepoint s2;
			{big_insert(O_REL, 500, start=2001)}
			rollback to savepoint s2;
			{big_insert(O_REL, 500, start=3001)}
			rollback to savepoint s1;
			{big_insert(O_REL, 100, start=4001)}
			commit;
		""")

	def sc_savepoint_release_reuse(self, pub):
		# A released savepoint frees its logical xid slot and the next
		# savepoint takes the same one: 200 rows
		pub.safe_psql(f"""
			begin;
			{big_insert(O_REL, 100)}
			savepoint s1;
			{big_insert(O_REL, 100, start=1001)}
			release savepoint s1;
			savepoint s2;
			{big_insert(O_REL, 100, start=2001)}
			rollback to savepoint s2;
			commit;
		""")

	def sc_first_write_in_savepoint_rollback(self, pub):
		# The savepoint starts before the transaction's first OrioleDB
		# write, so it gets no logical xid of its own: 100 rows
		pub.safe_psql(f"""
			begin;
			savepoint s1;
			{big_insert(O_REL, 100)}
			rollback to savepoint s1;
			{big_insert(O_REL, 100, start=1001)}
			commit;
		""")

	def sc_plpgsql_exception_blocks(self, pub):
		# An exception block is a savepoint released on success: 100 rows
		pub.safe_psql(f"""
			do $$
			begin
				begin
					{big_insert(O_REL, 100)}
				exception when others then
					null;
				end;
				begin
					{big_insert(O_REL, 100, start=1001)}
					raise exception 'roll back the second block';
				exception when others then
					null;
				end;
			end $$;
		""")

	def sc_plpgsql_first_block_fails(self, pub):
		# The first OrioleDB write happens inside a failing block: 100 rows
		pub.safe_psql(f"""
			do $$
			begin
				begin
					{big_insert(O_REL, 100)}
					raise exception 'roll back the first block';
				exception when others then
					null;
				end;
				{big_insert(O_REL, 100, start=1001)}
			end $$;
		""")

	def sc_serial_big_insert(self, pub):
		# Default sequence cache: the warm-up row leaves prelogged values, so
		# nextval() takes the heap xid in the middle of the transaction, at
		# the first refill (O2H)
		pub.safe_psql(f"insert into {SERIAL_REL}(data) values ('warm-up');")
		pub.safe_psql(f"""
			begin;
			{big_serial_insert()}
			commit;
		""")

	def sc_serial_cached_insert(self, pub):
		# The documented workaround: a cache that covers the transaction and
		# a reused session keep it xid-less, the refill lands in the warm-up
		pub.safe_psql(f"alter sequence {SERIAL_SEQ} cache {SERIAL_CACHE};")
		with pub.connect() as con:
			con.execute(f"insert into {SERIAL_REL}(data) values ('warm-up');")
			con.commit()
			con.begin()
			con.execute(big_serial_insert())
			xidless = con.execute(
			    "select orioledb_get_current_heap_xid()::text = '0';")[0][0]
			con.commit()
		# The scenario needs the xid-less path
		self.assertTrue(xidless, "the big transaction got a heap xid")

	def sc_heap_then_oriole(self, pub):
		pub.safe_psql(f"""
			begin;
			{big_insert(H_REL, 10)}
			{big_insert(O_REL)}
			commit;
		""")

	def sc_oriole_then_heap(self, pub):
		pub.safe_psql(f"""
			begin;
			{big_insert(O_REL)}
			{big_insert(H_REL, 10)}
			commit;
		""")

	def sc_oriole_savepoint_then_heap(self, pub):
		pub.safe_psql(f"""
			begin;
			{big_insert(O_REL, 100)}
			savepoint s1;
			{big_insert(O_REL, 100, start=1001)}
			release savepoint s1;
			insert into {H_REL}(id, data) values (1, 'h');
			commit;
		""")

	def sc_ddl_then_dml(self, pub):
		pub.safe_psql(f"""
			begin;
			alter table {O_REL} add column c int;
			insert into {O_REL}(id, data, c)
				select a, repeat(md5(a::text), 20), a
					from generate_series(1, {BIG_ROWS}) f(a);
			commit;
		""")

	def sc_dml_then_ddl(self, pub):
		pub.safe_psql(f"""
			begin;
			{big_insert(O_REL)}
			alter table {O_REL} add column c int;
			insert into {O_REL}(id, data, c)
				select a, repeat(md5(a::text), 20), a
					from generate_series(10001, 10100) f(a);
			commit;
		""")

	def sc_toast(self, pub):
		pub.safe_psql(
		    f"alter table {O_REL} alter column data set storage external;")
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data)
				select a, repeat('x', 100000) || a
					from generate_series(1, 20) f(a);
			commit;
		""")

	def sc_immediate(self, pub):
		# Small heap-first transaction, for immediate_conf() on the publisher
		pub.safe_psql(f"""
			begin;
			insert into {H_REL}(id, data) values (1, 'h');
			insert into {O_REL}(id, data) values (1, 'o');
			commit;
		""")


# OrioleDB turns streaming off, see o_decoding_startup()
@unittest.expectedFailure
class LogicalStreamingFunctionalTest(StreamingTestBase):
	"""
	The transaction was streamed: pg_stat_replication_slots.stream_txns for a
	subscription, "opening a streamed block" lines for test_decoding.
	"""

	def check_streamed(self, scenario, streaming='on', rels=(O_REL, ), **conf):
		pub, sub, _ = self.start_pair(streaming, **conf)
		scenario(pub)
		self.assert_streamed(pub, sub, rels)

	def test_stream_large_insert(self):
		self.check_streamed(self.sc_large_insert)

	def test_stream_interleaved(self):
		self.check_streamed(self.sc_interleaved)

	def test_stream_rollback(self):
		self.check_streamed(self.sc_rollback)

	def test_stream_serial_big_insert(self):
		self.check_streamed(self.sc_serial_big_insert, rels=(SERIAL_REL, ))

	def test_stream_serial_cached_insert(self):
		self.check_streamed(self.sc_serial_cached_insert, rels=(SERIAL_REL, ))

	def test_stream_heap_then_oriole(self):
		self.check_streamed(self.sc_heap_then_oriole, rels=(O_REL, H_REL))

	def test_stream_oriole_then_heap(self):
		self.check_streamed(self.sc_oriole_then_heap, rels=(O_REL, H_REL))

	def test_stream_immediate(self):
		self.check_streamed(self.sc_immediate,
		                    rels=(O_REL, H_REL),
		                    pub_conf=self.immediate_conf())

	def test_stream_heap_txn_before_oriole(self):
		"""
		A large heap transaction is decoded before the first OrioleDB record.

		  heap xact:   BEGIN -- 2k rows (evicted) ---------------- COMMIT
		  oriole xact:                       INSERT, COMMIT
		"""
		pub, sub, _ = self.start_pair()
		with pub.connect() as heap_con:
			heap_con.begin()
			heap_con.execute(big_insert(H_REL))
			# Wait until the decoder evicts the open heap transaction
			evicted = self.wait_until(lambda: sum(self.slot_stats(pub)) >= 1)
			self.assertTrue(evicted, self.log_errors(pub))
			pub.safe_psql(f"insert into {O_REL}(id, data) values (1, 'x');")
			heap_con.commit()
		self.assert_streamed(pub, sub, (H_REL, O_REL))

	def test_stream_parallel_worker(self):
		"""
		With streaming = parallel a parallel apply worker shows up in
		pg_stat_subscription while the streamed transaction is open.
		"""
		pub, sub, _ = self.start_pair('parallel')
		with pub.connect() as con:
			con.begin()
			con.execute(big_insert(O_REL))
			streamed = self.wait_until(lambda: self.slot_stats(pub)[0] >= 1,
			                           INFLIGHT_WAIT_S)
			workers = 0
			if streamed:
				workers = self.wait_until(lambda: self.parallel_workers(sub),
				                          INFLIGHT_WAIT_S)
			con.commit()
		self.assertTrue(streamed,
		                f"not streamed while open\n{self.log_errors(pub)}")
		self.assertGreaterEqual(workers, 1, "no parallel apply worker")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_decode_large_insert(self):
		node = self.start_decoding()
		self.sc_large_insert(node)
		self.assert_decoded_stream(self.decode(node),
		                           "committing streamed transaction")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_decode_savepoints(self):
		node = self.start_decoding()
		self.sc_savepoint_rollbacks(node)
		self.assert_decoded_stream(self.decode(node),
		                           "aborting streamed (sub)transaction")

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_decode_oriole_then_heap(self):
		node = self.start_decoding()
		self.sc_oriole_then_heap(node)
		self.assert_decoded_stream(self.decode(node))

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_decode_immediate_mixed(self):
		node = self.start_decoding()
		self.sc_immediate(node)
		self.assert_decoded_stream(self.decode(node, immediate=True))


class LogicalStreamingCorrectnessTest(StreamingTestBase):
	"""
	The subscriber ends up with the publisher's data and nobody crashed.
	"""

	def check_apply(self,
	                scenario,
	                streaming='on',
	                rels=(O_REL, ),
	                expr='data',
	                **conf):
		pub, sub, _ = self.start_pair(streaming, **conf)
		scenario(pub)
		self.assert_replicated(pub, sub, rels, expr)

	def test_apply_large_insert(self):
		self.check_apply(self.sc_large_insert)

	def test_apply_large_insert_parallel(self):
		self.check_apply(self.sc_large_insert, 'parallel')

	def test_apply_interleaved(self):
		self.check_apply(self.sc_interleaved)

	def test_apply_interleaved_parallel(self):
		self.check_apply(self.sc_interleaved, 'parallel')

	def test_apply_rollback(self):
		self.check_apply(self.sc_rollback)

	def test_apply_rollback_parallel(self):
		self.check_apply(self.sc_rollback, 'parallel')

	def test_apply_savepoint_rollbacks(self):
		self.check_apply(self.sc_savepoint_rollbacks)

	def test_apply_savepoint_rollbacks_parallel(self):
		self.check_apply(self.sc_savepoint_rollbacks, 'parallel')

	def test_apply_savepoint_release_reuse(self):
		self.check_apply(self.sc_savepoint_release_reuse)

	def test_apply_first_write_in_savepoint_rollback(self):
		self.check_apply(self.sc_first_write_in_savepoint_rollback)

	def test_apply_plpgsql_exception_blocks(self):
		self.check_apply(self.sc_plpgsql_exception_blocks)

	def test_apply_plpgsql_first_block_fails(self):
		self.check_apply(self.sc_plpgsql_first_block_fails)

	def test_apply_serial_big_insert(self):
		self.check_apply(self.sc_serial_big_insert, rels=(SERIAL_REL, ))

	def test_apply_serial_big_insert_parallel(self):
		self.check_apply(self.sc_serial_big_insert,
		                 'parallel',
		                 rels=(SERIAL_REL, ))

	def test_apply_serial_cached_insert(self):
		self.check_apply(self.sc_serial_cached_insert, rels=(SERIAL_REL, ))

	def test_apply_heap_then_oriole(self):
		self.check_apply(self.sc_heap_then_oriole, rels=(O_REL, H_REL))

	def test_apply_oriole_then_heap(self):
		self.check_apply(self.sc_oriole_then_heap, rels=(O_REL, H_REL))

	def test_apply_oriole_then_heap_parallel(self):
		self.check_apply(self.sc_oriole_then_heap,
		                 'parallel',
		                 rels=(O_REL, H_REL))

	def test_apply_oriole_savepoint_then_heap(self):
		self.check_apply(self.sc_oriole_savepoint_then_heap,
		                 rels=(O_REL, H_REL))

	def test_apply_ddl_then_dml(self):
		self.check_apply(self.sc_ddl_then_dml, expr=DDL_EXPR)

	def test_apply_dml_then_ddl(self):
		self.check_apply(self.sc_dml_then_ddl, expr=DDL_EXPR)

	def test_apply_toast(self):
		self.check_apply(self.sc_toast)

	def test_apply_immediate(self):
		self.check_apply(self.sc_immediate,
		                 rels=(O_REL, H_REL),
		                 pub_conf=self.immediate_conf())

	def test_apply_immediate_parallel(self):
		self.check_apply(self.sc_immediate,
		                 'parallel',
		                 rels=(O_REL, H_REL),
		                 pub_conf=self.immediate_conf())

	def test_apply_subscriber_serializes(self):
		# The leader spools the stream to a file and the parallel apply
		# worker applies it at commit (015_stream.pl tail)
		self.check_apply(self.sc_large_insert,
		                 'parallel',
		                 sub_conf=self.immediate_conf())

	def test_apply_heap_txn_before_oriole(self):
		pub, sub, _ = self.start_pair()
		with pub.connect() as heap_con:
			heap_con.begin()
			heap_con.execute(big_insert(H_REL))
			# Wait until the decoder evicts the open heap transaction
			evicted = self.wait_until(lambda: sum(self.slot_stats(pub)) >= 1)
			self.assertTrue(evicted, self.log_errors(pub))
			pub.safe_psql(f"insert into {O_REL}(id, data) values (1, 'x');")
			heap_con.commit()
		self.assert_replicated(pub, sub, (H_REL, O_REL))

	def test_apply_crash_in_flight(self):
		"""
		The publisher crashes with a big transaction open.  Recovery emits a
		rollback for it, so the subscriber must end up with the row committed
		after the restart only.
		"""
		pub, sub, _ = self.start_pair()
		con = pub.connect()
		con.begin()
		con.execute(big_insert(O_REL))
		# Wait until the decoder evicts the open transaction
		evicted = self.wait_until(lambda: sum(self.slot_stats(pub)) >= 1)
		self.assertTrue(evicted, self.log_errors(pub))

		pub.stop(['-m', 'immediate'])
		try:
			con.close()
		except Exception:
			pass
		pub.start()

		pub.safe_psql(
		    f"insert into {O_REL}(id, data) values (1, 'after crash');")
		self.assert_replicated(pub, sub, (O_REL, ))
