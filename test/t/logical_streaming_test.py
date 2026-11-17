#!/usr/bin/env python3
# coding: utf-8

import os
import time
import unittest

from testgres.utils import get_pg_config

from .base_test import BaseTest
from .logical_test import wait_ready

# One row is about 640 bytes, so BIG_ROWS rows are 20 times the publisher's
# logical_decoding_work_mem (64kB).
BIG_ROWS = 2000
WAIT_S = 60
# For checks that need the transaction still in flight
INFLIGHT_WAIT_S = 20

O_REL = "o_data"
THRESHOLD_GUC = "orioledb.logical_heap_xid_threshold"
SERIAL_REL = "o_serial"
SERIAL_SEQ = f"{SERIAL_REL}_id_seq"
# Sequence cache of the documented workaround (limitation 13): one refill
# covers a whole big transaction, so nextval() takes no heap xid inside it
SERIAL_CACHE = 10 * BIG_ROWS
H_REL = "h_data"
SEQ = "o_seq"
NEVER = f"{THRESHOLD_GUC} = -1\n"
NEW_REL = "o_new"
ENUM = "o_color"
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
		# Below the big inserts of the scenarios, above their small ones
		self.node.append_conf('postgresql.conf', f"{THRESHOLD_GUC} = 64kB\n")

	_immediate_guc = None

	@classmethod
	def immediate_guc(cls):
		"""
		The developer option that streams every transaction at once.

		It was renamed in 17 and the rename was backpatched to 16, and
		--describe-config does not list it, so read the name out of the
		server program.
		"""
		if cls._immediate_guc is None:
			new = "debug_logical_replication_streaming"
			with open(os.path.join(get_pg_config()["BINDIR"], "postgres"),
			          "rb") as server:
				found = new.encode() in server.read()
			cls._immediate_guc = new if found else "logical_replication_mode"
		return cls._immediate_guc

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

	def sc_savepoints_around_switch(self, pub):
		# s1 starts before the heap xid is assigned (inside the first big
		# insert) and is released after it; s2 and s3 start after: 701 rows
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			savepoint s1;
			{big_insert(O_REL, 500, start=1001)}
			savepoint s2;
			{big_insert(O_REL, 500, start=2001)}
			rollback to savepoint s2;
			{big_insert(O_REL, 100, start=3001)}
			release savepoint s1;
			savepoint s3;
			{big_insert(O_REL, 100, start=4001)}
			rollback to savepoint s3;
			{big_insert(O_REL, 100, start=5001)}
			commit;
		""")

	def sc_switch_in_rolled_back_savepoint(self, pub):
		# The savepoint has no logical xid of its own, so its rollback drops
		# the whole logical transaction in the decoder, link included.  The
		# second insert must be linked again, and streamed: BIG_ROWS rows
		pub.safe_psql(f"""
			begin;
			savepoint s1;
			{big_insert(O_REL)}
			rollback to savepoint s1;
			{big_insert(O_REL, start=10001)}
			commit;
		""")

	def sc_heap_then_xidless_savepoint_rollback(self, pub):
		# Same with the heap xid assigned before the savepoint
		pub.safe_psql(f"""
			begin;
			insert into {H_REL}(id, data) values (1, 'h');
			savepoint s1;
			{big_insert(O_REL)}
			rollback to savepoint s1;
			{big_insert(O_REL, start=10001)}
			commit;
		""")

	# The reader takes the heap xid of a container from its xid record, or
	# from a later switch record.  Here the xid record does not know the
	# heap xid yet.

	def sc_spilled_savepoint_heap_inside(self, pub):
		# Heap xid before the release: the release writes a joint commit
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			savepoint s1;
			{big_insert(O_REL, start=1001)}
			insert into {H_REL}(id, data) values (1, 'h');
			release savepoint s1;
			commit;
		""")

	def sc_spilled_savepoint_heap_after(self, pub):
		# Heap xid after the release: the switch at commit adopts both
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			savepoint s1;
			{big_insert(O_REL, start=1001)}
			release savepoint s1;
			insert into {H_REL}(id, data) values (1, 'h');
			commit;
		""")

	def sc_switch_in_aborted_savepoint(self, pub):
		# The heap xid assigned inside s1 outlives its rollback: 2001 rows
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			savepoint s1;
			{big_insert(O_REL, start=1001)}
			rollback to savepoint s1;
			{big_insert(O_REL, start=10001)}
			commit;
		""")

	def sc_savepoint_after_nextval(self, pub):
		# nextval() assigns the heap xid with no OrioleDB record in between,
		# so the savepoint is the first record to name it
		pub.safe_psql(f"create sequence {SEQ};")
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			select nextval('{SEQ}');
			savepoint s1;
			{big_insert(O_REL, start=1001)}
			release savepoint s1;
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
		# The documented workaround: the cache covers the transaction and
		# the refill lands in the warm-up, so nextval() takes no heap xid.
		# The big insert gets one from the threshold.
		pub.safe_psql(f"alter sequence {SERIAL_SEQ} cache {SERIAL_CACHE};")
		with pub.connect() as con:
			con.execute(f"insert into {SERIAL_REL}(data) values ('warm-up');")
			con.commit()
			con.begin()
			con.execute(f"insert into {SERIAL_REL}(data) values ('small');")
			xidless = con.execute(
			    "select orioledb_get_current_heap_xid()::text = '0';")[0][0]
			con.execute(big_serial_insert())
			con.commit()
		# The scenario needs the xid-less path
		self.assertTrue(xidless, "nextval() took a heap xid")

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

	def sc_ddl_before_switch(self, pub):
		# The catalog write assigns the heap xid below the threshold
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			alter table {O_REL} add column c int;
			insert into {O_REL}(id, data, c)
				select a, repeat(md5(a::text), 20), a
					from generate_series(2, {BIG_ROWS}) f(a);
			commit;
		""")

	def sc_rollback_after_stream(self, pub):
		# s1 is rolled back once the transaction is being streamed, so the
		# subscriber gets a stream abort; what follows goes on under the same
		# heap xid: 2600 rows
		with pub.connect() as con:
			con.begin()
			con.execute(big_insert(O_REL))
			streamed = self.wait_until(lambda: self.slot_stats(pub)[0] >= 1)
			self.assertTrue(streamed, self.log_errors(pub))
			con.execute(f"""
				savepoint s1;
				{big_insert(O_REL, 500, start=10001)}
				rollback to savepoint s1;
				savepoint s2;
				{big_insert(O_REL, 500, start=20001)}
				release savepoint s2;
				{big_insert(O_REL, 100, start=30001)}
			""")
			con.commit()

	def sc_terminated_while_streamed(self, pub):
		# The backend dies while its transaction is being streamed.  The
		# subscriber must roll the stream back and keep the later row only
		con = pub.connect()
		con.begin()
		con.execute(big_insert(O_REL))
		streamed = self.wait_until(lambda: self.slot_stats(pub)[0] >= 1)
		self.assertTrue(streamed, self.log_errors(pub))
		pub.safe_psql(f"select pg_terminate_backend({con.pid});")
		try:
			con.close()
		except Exception:
			pass
		pub.safe_psql(f"insert into {O_REL}(id, data) values (1, 'after');")

	def sc_plpgsql_switch_in_failed_block(self, pub):
		# The heap xid arrives inside a block that then fails.  The block
		# started before the first OrioleDB write, so its rollback drops the
		# whole logical transaction in the decoder, link included; the
		# second insert must be linked again and streamed: BIG_ROWS rows
		pub.safe_psql(f"""
			do $$
			begin
				begin
					{big_insert(O_REL)}
					raise exception 'roll back the first block';
				exception when others then
					null;
				end;
				{big_insert(O_REL, start=10001)}
			end $$;
		""")

	def sc_oversized_record_crosses(self, pub):
		# A REPLICA IDENTITY FULL update of a TOASTed row writes the old
		# tuple inline, in a WAL container of its own.  The TOAST chunks stay
		# under the threshold, the old tuple crosses it, and the small insert
		# after it gets the heap xid
		pub.safe_psql(f"""
			alter table {O_REL} replica identity full;
			insert into {O_REL}(id, data)
				select 1, string_agg(md5(a::text), '')
					from generate_series(1, 700) f(a);
		""")
		with pub.connect() as con:
			con.begin()
			con.execute(f"set {THRESHOLD_GUC} = 32;")
			con.execute(f"update {O_REL} set data = data || 'y' where id = 1;")
			self.assertEqual(
			    con.execute("select orioledb_get_current_heap_xid();")[0][0],
			    0, "the TOAST chunks crossed the threshold")
			con.execute(f"insert into {O_REL}(id, data) values (2, 'small');")
			self.assertNotEqual(
			    con.execute("select orioledb_get_current_heap_xid();")[0][0],
			    0, "the old tuple did not count")
			con.commit()

	def sc_spilled_then_adopted(self, pub):
		# For spilled_conf(): the start of the transaction is spilled under
		# the logical xid, the rest streamed under the heap xid
		pub.safe_psql(f"begin; {big_insert(O_REL, 2 * BIG_ROWS)} commit;")

	@staticmethod
	def spilled_conf():
		# Threshold above logical_decoding_work_mem, below the transaction
		return f"{THRESHOLD_GUC} = 1MB\n"

	def sc_ddl_after_nextval_in_savepoint(self, pub):
		# nextval() assigns the heap xid inside s1 with no OrioleDB record in
		# between.  The first OrioleDB writes after it are the catalog and
		# type cache entries of the new table: BIG_ROWS + 1 rows
		pub.safe_psql(f"create sequence {SEQ};")
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			savepoint s1;
			select nextval('{SEQ}');
			create type {ENUM} as enum ('r', 'g');
			create table {NEW_REL}(id int primary key, c {ENUM})
				using orioledb;
			insert into {NEW_REL} values (1, 'r');
			release savepoint s1;
			{big_insert(O_REL, start=1001)}
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

	def sc_update_delete_toast(self, pub):
		# Streamed changes other than inserts: every row updated, every
		# other row deleted, TOASTed values rewritten and replaced
		pub.safe_psql(f"""
			alter table {O_REL} alter column data set storage external;
			{big_insert(O_REL)}
			insert into {O_REL}(id, data)
				select a, repeat('x', 100000) || a
					from generate_series({BIG_ROWS + 1}, {BIG_ROWS + 10}) f(a);
		""")
		pub.safe_psql(f"""
			begin;
			update {O_REL} set data = data || 'u';
			delete from {O_REL} where id % 2 = 0;
			update {O_REL} set data = repeat('y', 100000) || id
				where id > {BIG_ROWS};
			{big_insert(O_REL, start=20001)}
			commit;
		""")

	def sc_update_delete_full(self, pub):
		# The same with the old tuples in the WAL
		pub.safe_psql(f"alter table {O_REL} replica identity full;")
		self.sc_update_delete_toast(pub)

	def sc_release_in_switch_container(self, pub):
		# The threshold is crossed inside s1, so the switch of the top-level
		# logical xid and the one the release writes for s1 land in the same
		# WAL container: 2151 rows
		pub.safe_psql(f"""
			begin;
			insert into {O_REL}(id, data) values (1, 'first');
			savepoint s1;
			{big_insert(O_REL, 150, start=1001)}
			release savepoint s1;
			{big_insert(O_REL, start=10001)}
			commit;
		""")

	def sc_walsender_restart_while_streamed(self, pub):
		# The walsender dies mid-stream.  The next one decodes the open
		# transaction again from the slot's restart point, switch included
		with pub.connect() as con:
			con.begin()
			con.execute(big_insert(O_REL))
			streamed = self.wait_until(lambda: self.slot_stats(pub)[0] >= 1)
			self.assertTrue(streamed, self.log_errors(pub))
			pub.safe_psql(f"""
				select pg_terminate_backend(active_pid)
					from pg_replication_slots where slot_name = '{SUB}';
			""")
			con.execute(big_insert(O_REL, 500, start=10001))
			con.commit()

	def sc_ddl_in_savepoint_every_xid(self, pub):
		# For every_xid_conf(): the catalog and type cache writes of the new
		# table are the first OrioleDB records, some of them in autonomous
		# transactions, and s1 started before any of them
		pub.safe_psql(f"""
			begin;
			savepoint s1;
			create type {ENUM} as enum ('r', 'g');
			create table {NEW_REL}(id int primary key, c {ENUM})
				using orioledb;
			insert into {NEW_REL} values (1, 'r');
			release savepoint s1;
			{big_insert(O_REL)}
			commit;
		""")

	@staticmethod
	def every_xid_conf():
		return f"{THRESHOLD_GUC} = 0\n"

	def sc_switch_inside_toast(self, pub):
		# One TOASTed row crosses the threshold in the middle of its chunks
		pub.safe_psql(
		    f"alter table {O_REL} alter column data set storage external;")
		pub.safe_psql(f"""
			begin;
			set local {THRESHOLD_GUC} = 1;
			insert into {O_REL}(id, data) values (1, repeat('x', 100000));
			{big_insert(O_REL, start=2)}
			commit;
		""")


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

	def test_stream_savepoints_around_switch(self):
		self.check_streamed(self.sc_savepoints_around_switch)

	def test_stream_ddl_before_switch(self):
		self.check_streamed(self.sc_ddl_before_switch)

	def test_stream_savepoint_after_nextval(self):
		self.check_streamed(self.sc_savepoint_after_nextval)

	def test_stream_rollback_after_stream(self):
		self.check_streamed(self.sc_rollback_after_stream)

	def test_stream_plpgsql_switch_in_failed_block(self):
		self.check_streamed(self.sc_plpgsql_switch_in_failed_block)

	def test_stream_ddl_after_nextval_in_savepoint(self):
		self.check_streamed(self.sc_ddl_after_nextval_in_savepoint)

	def test_stream_spilled_then_adopted(self):
		pub, sub, _ = self.start_pair(pub_conf=self.spilled_conf())
		self.sc_spilled_then_adopted(pub)
		self.assert_streamed(pub, sub, (O_REL, ))
		stream_txns, spill_txns = self.slot_stats(pub)
		self.assertGreaterEqual(spill_txns, 1, "nothing was spilled first")

	def test_stream_update_delete_toast(self):
		self.check_streamed(self.sc_update_delete_toast)

	def test_stream_release_in_switch_container(self):
		self.check_streamed(self.sc_release_in_switch_container)

	def test_stream_walsender_restart_while_streamed(self):
		self.check_streamed(self.sc_walsender_restart_while_streamed)

	def test_stream_switch_inside_toast(self):
		self.check_streamed(self.sc_switch_inside_toast)

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

	def check_streamed_after_abort(self, scenario):
		# The rollback drops the streamed logical transaction; what follows
		# must be streamed again, under the same heap xid
		node = self.start_decoding()
		scenario(node)
		lines = self.decode(node)
		self.assert_decoded_stream(lines, "aborting streamed (sub)transaction")
		aborted = next(i for i, l in enumerate(lines)
		               if l.startswith("aborting streamed"))
		self.assertTrue([
		    l for l in lines[aborted:]
		    if l.startswith("opening a streamed block")
		], "nothing streamed after the abort:\n" +
		                "\n".join(lines[aborted:][:40]))

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_decode_switch_in_rolled_back_savepoint(self):
		self.check_streamed_after_abort(
		    self.sc_switch_in_rolled_back_savepoint)

	@unittest.skipIf(not BaseTest.extension_installed("test_decoding"),
	                 "'test_decoding' is not installed")
	def test_decode_heap_then_xidless_savepoint_rollback(self):
		self.check_streamed_after_abort(
		    self.sc_heap_then_xidless_savepoint_rollback)

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

	def test_apply_savepoints_around_switch(self):
		self.check_apply(self.sc_savepoints_around_switch)

	def test_apply_savepoints_around_switch_parallel(self):
		self.check_apply(self.sc_savepoints_around_switch, 'parallel')

	def test_apply_switch_in_rolled_back_savepoint(self):
		self.check_apply(self.sc_switch_in_rolled_back_savepoint)

	def test_apply_heap_then_xidless_savepoint_rollback(self):
		self.check_apply(self.sc_heap_then_xidless_savepoint_rollback,
		                 rels=(O_REL, H_REL))

	def test_apply_spilled_savepoint_heap_inside(self):
		self.check_apply(self.sc_spilled_savepoint_heap_inside,
		                 rels=(O_REL, H_REL))

	def test_apply_spilled_savepoint_heap_inside_never(self):
		self.check_apply(self.sc_spilled_savepoint_heap_inside,
		                 rels=(O_REL, H_REL),
		                 pub_conf=NEVER)

	def test_apply_spilled_savepoint_heap_after(self):
		self.check_apply(self.sc_spilled_savepoint_heap_after,
		                 rels=(O_REL, H_REL))

	def test_apply_spilled_savepoint_heap_after_never(self):
		self.check_apply(self.sc_spilled_savepoint_heap_after,
		                 rels=(O_REL, H_REL),
		                 pub_conf=NEVER)

	def test_apply_switch_in_aborted_savepoint(self):
		self.check_apply(self.sc_switch_in_aborted_savepoint)

	def test_apply_savepoint_after_nextval(self):
		self.check_apply(self.sc_savepoint_after_nextval)

	def test_apply_savepoint_after_nextval_never(self):
		self.check_apply(self.sc_savepoint_after_nextval, pub_conf=NEVER)

	def test_apply_oriole_then_heap_never(self):
		self.check_apply(self.sc_oriole_then_heap,
		                 rels=(O_REL, H_REL),
		                 pub_conf=NEVER)

	def test_apply_rollback_after_stream(self):
		self.check_apply(self.sc_rollback_after_stream)

	def test_apply_rollback_after_stream_parallel(self):
		self.check_apply(self.sc_rollback_after_stream, 'parallel')

	def test_apply_terminated_while_streamed(self):
		self.check_apply(self.sc_terminated_while_streamed)

	def test_apply_terminated_while_streamed_parallel(self):
		self.check_apply(self.sc_terminated_while_streamed, 'parallel')

	def test_apply_plpgsql_switch_in_failed_block(self):
		self.check_apply(self.sc_plpgsql_switch_in_failed_block)

	def test_apply_oversized_record_crosses(self):
		self.check_apply(self.sc_oversized_record_crosses)

	def test_apply_spilled_then_adopted(self):
		self.check_apply(self.sc_spilled_then_adopted,
		                 pub_conf=self.spilled_conf())

	def test_apply_spilled_then_adopted_parallel(self):
		self.check_apply(self.sc_spilled_then_adopted,
		                 'parallel',
		                 pub_conf=self.spilled_conf())

	def test_apply_ddl_after_nextval_in_savepoint(self):
		self.check_apply(self.sc_ddl_after_nextval_in_savepoint)

	def test_apply_update_delete_toast(self):
		self.check_apply(self.sc_update_delete_toast)

	def test_apply_update_delete_toast_parallel(self):
		self.check_apply(self.sc_update_delete_toast, 'parallel')

	def test_apply_update_delete_full(self):
		self.check_apply(self.sc_update_delete_full)

	def test_apply_update_delete_full_parallel(self):
		self.check_apply(self.sc_update_delete_full, 'parallel')

	def test_apply_release_in_switch_container(self):
		self.check_apply(self.sc_release_in_switch_container)

	def test_apply_walsender_restart_while_streamed(self):
		self.check_apply(self.sc_walsender_restart_while_streamed)

	def test_apply_walsender_restart_while_streamed_parallel(self):
		self.check_apply(self.sc_walsender_restart_while_streamed, 'parallel')

	def test_apply_ddl_in_savepoint_every_xid(self):
		self.check_apply(self.sc_ddl_in_savepoint_every_xid,
		                 pub_conf=self.every_xid_conf())

	def test_apply_switch_inside_toast(self):
		self.check_apply(self.sc_switch_inside_toast)

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

	def test_apply_ddl_before_switch(self):
		self.check_apply(self.sc_ddl_before_switch, expr=DDL_EXPR)

	def test_apply_threshold_disabled(self):
		# Without a heap xid the transaction can only be spilled
		pub, sub, _ = self.start_pair(pub_conf=f"{THRESHOLD_GUC} = -1\n")
		self.sc_large_insert(pub)
		self.assert_replicated(pub, sub, (O_REL, ))
		stream_txns, spill_txns = self.slot_stats(pub)
		self.assertEqual(stream_txns, 0)
		self.assertGreaterEqual(spill_txns, 1)

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

	def fill_until_flushed(self, pub, con):
		"""
		Insert single rows until the local WAL buffer is written out.  The
		next container stays in memory and is lost with a crash, so the WAL
		ends with the one just written.
		"""
		lsn = pub.execute("select pg_current_wal_insert_lsn();")[0][0]
		for i in range(1, 100):
			con.execute(big_insert(O_REL, 1, start=i))
			if pub.execute("select pg_current_wal_insert_lsn();")[0][0] != lsn:
				return
		self.fail("the local WAL buffer never flushed")

	def crash_and_move_past(self, pub, sub, con):
		"""
		Crash the publisher with con's transaction open and check that the
		slot moves past it after the restart.
		"""
		self.assertNotEqual(
		    con.execute("select orioledb_get_current_heap_xid();")[0][0], 0)
		# Get the last container to disk, or the crash drops it and nothing
		# is tested
		pub.safe_psql("select pg_switch_wal();")
		lsn = pub.execute("select pg_current_wal_flush_lsn();")[0][0]

		pub.stop(['-m', 'immediate'])
		try:
			con.close()
		except Exception:
			pass
		pub.start()

		# The slot moves on only once the decoder forgot the crashed
		# transaction; checkpoints give the walsender running-xacts records
		for i in range(1, 6):
			pub.safe_psql(
			    f"insert into {O_REL}(id, data) values ({1000 + i}, 'after');")
			self.wait_count(sub, O_REL, i)
			pub.safe_psql("checkpoint;")
		moved = self.wait_until(lambda: pub.execute(f"""
			select restart_lsn > '{lsn}'::pg_lsn
				from pg_replication_slots where slot_name = '{SUB}';
		""")[0][0])
		self.assertTrue(moved, f"restart_lsn stuck\n{self.log_errors(pub)}")
		self.assert_replicated(pub, sub, (O_REL, ))

	def test_apply_crash_after_switch(self):
		"""
		The publisher crashes right after the container that carries the
		switch to the heap xid was written.  The container's xid record
		predates the heap xid, so recovery can only learn it from the
		switch.  It must abort the heap xid in the decoder too, or the slot
		can never move past it.
		"""
		pub, sub, _ = self.start_pair()
		con = pub.connect()
		con.begin()
		con.execute(f"set {THRESHOLD_GUC} = 1;")
		# Two rows cross the threshold, inside the first container
		self.fill_until_flushed(pub, con)
		self.crash_and_move_past(pub, sub, con)

	def test_apply_crash_heap_first(self):
		"""
		The transaction has its heap xid before its first OrioleDB write, so
		the switch is the first record of its container, ahead of the xid
		record.  Recovery has no transaction state at the switch then and
		must take the heap xid from the xid record instead.
		"""
		pub, sub, _ = self.start_pair()
		con = pub.connect()
		con.begin()
		con.execute(f"insert into {H_REL}(id, data) values (1, 'h');")
		self.fill_until_flushed(pub, con)
		self.crash_and_move_past(pub, sub, con)

	def test_apply_crash_after_checkpoint(self):
		"""
		A checkpoint lands between the switch and the crash.  Recovery
		starts after the switch and learns the heap xid from the xid record
		of a later container, written under it.
		"""
		pub, sub, _ = self.start_pair()
		con = pub.connect()
		con.begin()
		con.execute(big_insert(O_REL))
		streamed = self.wait_until(lambda: self.slot_stats(pub)[0] >= 1)
		self.assertTrue(streamed, self.log_errors(pub))
		pub.safe_psql("checkpoint;")
		con.execute(big_insert(O_REL, 100, start=10001))
		self.crash_and_move_past(pub, sub, con)


class LazyHeapXidTest(StreamingTestBase):
	"""
	A transaction gets a heap xid once it wrote more OrioleDB WAL than
	orioledb.logical_heap_xid_threshold, and not before.
	"""

	def heap_xid_after(self, con, sql):
		con.execute(sql)
		return con.execute("select orioledb_get_current_heap_xid();")[0][0]

	def test_heap_xid_above_threshold(self):
		node = self.node
		node.start()
		node.safe_psql(self.publisher_sql)
		with node.connect() as con:
			con.begin()
			self.assertEqual(
			    self.heap_xid_after(
			        con, f"insert into {O_REL}(id, data) values (1, 'x');"), 0)
			self.assertNotEqual(
			    self.heap_xid_after(con, big_insert(O_REL, start=2)), 0)
			con.commit()

	def test_no_heap_xid_without_logical_wal(self):
		# The xid is for logical decoding only, so a big transaction stays
		# xid-less while nothing can decode it
		node = self.node
		node.append_conf('postgresql.conf', "wal_level = replica\n")
		node.start()
		node.safe_psql(self.publisher_sql)
		with node.connect() as con:
			con.begin()
			self.assertEqual(self.heap_xid_after(con, big_insert(O_REL)), 0)
			con.commit()

	def test_no_heap_xid_when_disabled(self):
		node = self.node
		node.start()
		node.safe_psql(self.publisher_sql)
		with node.connect() as con:
			con.begin()
			con.execute(f"set {THRESHOLD_GUC} = -1;")
			self.assertEqual(self.heap_xid_after(con, big_insert(O_REL)), 0)
			con.commit()

	def test_threshold_per_role_and_transaction(self):
		node = self.node
		node.start()
		node.safe_psql(self.publisher_sql)
		node.safe_psql(f"alter role current_user set {THRESHOLD_GUC} = -1;")
		with node.connect() as con:
			con.begin()
			self.assertEqual(self.heap_xid_after(con, big_insert(O_REL)), 0)
			con.commit()

			con.begin()
			con.execute(f"set local {THRESHOLD_GUC} = 0;")
			self.assertNotEqual(
			    self.heap_xid_after(
			        con,
			        f"insert into {O_REL}(id, data) values (10001, 'x');"), 0)
			con.commit()

			# SET LOCAL ended with its transaction, the role setting is back
			con.begin()
			self.assertEqual(
			    self.heap_xid_after(con, big_insert(O_REL, start=20001)), 0)
			con.commit()
