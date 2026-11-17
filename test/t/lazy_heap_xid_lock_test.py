#!/usr/bin/env python3
# coding: utf-8

import time
import unittest
from enum import Enum

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from .logical_test import wait_ready

THRESHOLD_GUC = "orioledb.logical_heap_xid_threshold"
# A waiter that is still blocked after this long is taken as blocked for good
BLOCKED_S = 1.0
WAIT_S = 60
BIG_ROWS = 2000
SUB = "lock_sub"
UNIQUE_VIOLATION = "23505"
FK_VIOLATION = "23503"
DEADLOCK = "40P01"


class Kind(Enum):
	"""How a transaction gets, or avoids, a heap xid."""
	O = "oriole"  # OrioleDB only, never a heap xid
	S = "streamed"  # OrioleDB only, heap xid from the threshold
	H = "heap"  # heap only
	M = "mixed"  # OrioleDB first, then a heap write
	SM = "streamed mixed"  # S, then a heap write


# Pairs that touch the same OrioleDB row, and the same heap row.  A heap-only
# transaction never reaches an OrioleDB row.
ORIOLE_PAIRS = [(Kind.S, Kind.O), (Kind.O, Kind.S), (Kind.S, Kind.S),
                (Kind.S, Kind.M), (Kind.M, Kind.S), (Kind.SM, Kind.O),
                (Kind.O, Kind.SM), (Kind.SM, Kind.M), (Kind.SM, Kind.SM)]
HEAP_PAIRS = [(Kind.SM, Kind.H), (Kind.H, Kind.SM), (Kind.SM, Kind.M),
              (Kind.M, Kind.SM), (Kind.SM, Kind.SM)]


def pgcode(e):
	return getattr(e, 'pgcode', None)


class LazyHeapXidLockTest(BaseTest):
	"""
	Row locks of a transaction whose heap xid comes from
	orioledb.logical_heap_xid_threshold.

	Such a transaction commits through the heap like a mixed one.  Its locks
	must behave like any other transaction's: a conflicting statement waits
	until it ends, then sees the committed version or the original one.
	"""

	# Streaming variant: big priming writes, a subscriber checks the data
	big = False

	tables = """
		create extension if not exists orioledb;
		create table o_row(id int primary key, v int) using orioledb;
		create table h_row(id int primary key, v int);
		create table o_prime(id int primary key, data text) using orioledb;
		create table h_prime(id int primary key, data text);
		create table o_parent(id int primary key) using orioledb;
		create table o_child(id int primary key,
			pid int references o_parent) using orioledb;
		create table h_parent(id int primary key);
		create table h_child(id int primary key, pid int references h_parent);
	"""
	data_tables = ("o_row", "h_row", "o_prime", "h_prime", "o_parent",
	               "o_child", "h_parent", "h_child")

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		# Below the big priming insert of the streaming variant
		self.node.append_conf('postgresql.conf', f"{THRESHOLD_GUC} = 64kB\n")
		self.next_id = 1000

	def start(self):
		node = self.node
		node.start()
		node.safe_psql(self.tables)
		return node

	def new_id(self):
		self.next_id += 1
		return self.next_id

	def prime_insert(self, rows):
		start = self.new_id()
		self.next_id += rows
		return f"""
			insert into o_prime(id, data)
				select a, repeat(md5(a::text), 20)
					from generate_series({start}, {start + rows - 1}) f(a);
		"""

	def begin(self, con, kind):
		"""Open a transaction of the kind on con and prove it is one."""
		con.begin()
		if kind == Kind.O:
			con.execute(f"set {THRESHOLD_GUC} = -1;")
			con.execute(self.prime_insert(1))
		elif kind in (Kind.S, Kind.SM):
			if self.big:
				con.execute(self.prime_insert(BIG_ROWS))
			else:
				con.execute(f"set {THRESHOLD_GUC} = 1;")
				con.execute(self.prime_insert(4))
		elif kind == Kind.M:
			con.execute(self.prime_insert(1))
		if kind in (Kind.M, Kind.SM):
			con.execute(f"insert into h_prime values ({self.new_id()}, 'h');")

		if kind != Kind.H:
			heap_xid = con.execute(
			    "select orioledb_get_current_heap_xid();")[0][0]
			self.assertEqual(heap_xid == 0, kind == Kind.O,
			                 f"{kind}: heap xid {heap_xid}")

	def is_blocked(self, node, pid):
		"""The backend is waiting on something and has not finished."""
		rows = node.execute(f"""
			select wait_event_type, state from pg_stat_activity
				where pid = {pid};
		""")
		if not rows:
			return False
		wait_event_type, state = rows[0]
		return state == 'active' and wait_event_type is not None

	def wait_blocked(self, node, pid, thread, what):
		"""Wait until the thread's backend is blocked for good."""
		deadline = time.time() + WAIT_S
		since = None
		while time.time() < deadline:
			self.assertTrue(thread.is_alive(), f"did not wait: {what}")
			if self.is_blocked(node, pid):
				since = since or time.time()
				if time.time() - since >= BLOCKED_S:
					return True
			else:
				since = None
			time.sleep(0.05)
		return False

	def collect(self, node, con, thread, pid):
		"""
		Join the thread and end its transaction.  Returns its error code, or
		None.  A thread that will not finish has its backend terminated: the
		connection must be idle before anybody else may use it.
		"""
		code = None
		stuck = False
		try:
			thread.join(WAIT_S)
			if thread.is_alive():
				stuck = True
				node.execute(f"select pg_terminate_backend({pid});")
				thread.join(WAIT_S)
			con.commit()
		except Exception as e:
			code = pgcode(e)
			self.assertIsNotNone(code, repr(e))
			try:
				con.rollback()
			except Exception:
				pass
		return "stuck" if stuck else code

	def run_blocked(self, node, holder, hkind, hsql, waiter, wkind, wsql,
	                commit):
		"""
		holder runs hsql, waiter runs wsql and must block on it until holder
		commits or aborts.  Returns the waiter's error code, or None.
		"""
		self.begin(holder, hkind)
		holder.execute(hsql)
		self.begin(waiter, wkind)
		# Before the thread takes the connection over
		pid = waiter.pid
		thread = ThreadQueryExecutor(waiter, wsql)
		thread.start()
		blocked = self.wait_blocked(node, pid, thread, f"{wkind} on {wsql}")

		if commit:
			holder.commit()
		else:
			holder.rollback()
		code = self.collect(node, waiter, thread, pid)
		self.assertTrue(blocked, f"{wkind.value} never blocked on {wsql}")
		return code

	def value(self, node, rel, id):
		return node.execute(f"select v from {rel} where id = {id};")[0][0]

	def exists(self, node, rel, id):
		return node.execute(
		    f"select count(*) from {rel} where id = {id};")[0][0] == 1

	def check_conflicts(self, node, t, pairs, commit):
		"""
		Every conflict type for every (holder, waiter) pair on table kind t
		('o' or 'h'), holder ending with a commit or a rollback.
		"""
		row = f"{t}_row"
		parent = f"{t}_parent"
		child = f"{t}_child"
		for hkind, wkind in pairs:
			with node.connect() as holder, node.connect() as waiter:
				tag = f"{hkind.value} holds, {wkind.value} waits, {t}"

				r = self.new_id()
				node.safe_psql(f"insert into {row} values ({r}, 0);")
				update = f"update {row} set v = v + 1 where id = {r};"
				code = self.run_blocked(node, holder, hkind, update, waiter,
				                        wkind, update, commit)
				self.assertIsNone(code, tag)
				self.assertEqual(self.value(node, row, r), 2 if commit else 1,
				                 f"update/update: {tag}")

				r = self.new_id()
				node.safe_psql(f"insert into {row} values ({r}, 0);")
				code = self.run_blocked(
				    node, holder, hkind,
				    f"select v from {row} where id = {r} for update;", waiter,
				    wkind, f"update {row} set v = v + 1 where id = {r};",
				    commit)
				self.assertIsNone(code, tag)
				self.assertEqual(self.value(node, row, r), 1,
				                 f"for update/update: {tag}")

				r = self.new_id()
				insert = f"insert into {row} values ({r}, 0);"
				code = self.run_blocked(node, holder, hkind, insert, waiter,
				                        wkind, insert, commit)
				self.assertEqual(code, UNIQUE_VIOLATION if commit else None,
				                 f"insert/insert: {tag}")
				self.assertTrue(self.exists(node, row, r), tag)

				p = self.new_id()
				c = self.new_id()
				node.safe_psql(f"insert into {parent} values ({p});")
				code = self.run_blocked(
				    node, holder, hkind,
				    f"insert into {child} values ({c}, {p});", waiter, wkind,
				    f"delete from {parent} where id = {p};", commit)
				self.assertEqual(code, FK_VIOLATION if commit else None,
				                 f"child insert/parent delete: {tag}")
				self.assertEqual(self.exists(node, parent, p), commit, tag)

	def check_deadlock(self, node, akind, bkind, a_first, b_first):
		"""
		a locks a_first's row, b locks b_first's row, then each asks for the
		other's.  One of them must be chosen as the victim, the other
		finishes.
		"""
		with node.connect() as a, node.connect() as b:
			self.begin(a, akind)
			a.execute(a_first)
			self.begin(b, bkind)
			b.execute(b_first)
			apid, bpid = a.pid, b.pid

			ta = ThreadQueryExecutor(a, b_first)
			ta.start()
			self.assertTrue(self.wait_blocked(node, apid, ta, b_first),
			                "a did not block on b's row")
			tb = ThreadQueryExecutor(b, a_first)
			tb.start()

			codes = [
			    self.collect(node, a, ta, apid),
			    self.collect(node, b, tb, bpid)
			]
			self.assertEqual([c for c in codes if c is not None], [DEADLOCK],
			                 f"{akind.value} vs {bkind.value}: {codes}")

	def wait_until(self, cond, timeout=WAIT_S):
		deadline = time.time() + timeout
		while True:
			value = cond()
			if value or time.time() >= deadline:
				return value
			time.sleep(0.1)

	def finish(self, node):
		"""Hook for the streaming variant: check the subscriber."""
		pass

	def test_oriole_row_commit(self):
		node = self.start()
		self.check_conflicts(node, 'o', ORIOLE_PAIRS, commit=True)
		self.finish(node)

	def test_oriole_row_abort(self):
		node = self.start()
		self.check_conflicts(node, 'o', ORIOLE_PAIRS, commit=False)
		self.finish(node)

	def test_heap_row_commit(self):
		node = self.start()
		self.check_conflicts(node, 'h', HEAP_PAIRS, commit=True)
		self.finish(node)

	def test_heap_row_abort(self):
		node = self.start()
		self.check_conflicts(node, 'h', HEAP_PAIRS, commit=False)
		self.finish(node)

	def test_deadlock_oriole_rows(self):
		node = self.start()
		node.safe_psql("insert into o_row values (1, 0), (2, 0);")
		self.check_deadlock(node, Kind.S, Kind.O,
		                    "update o_row set v = v + 1 where id = 1;",
		                    "update o_row set v = v + 1 where id = 2;")
		self.finish(node)

	def test_deadlock_heap_rows(self):
		node = self.start()
		node.safe_psql("insert into h_row values (1, 0), (2, 0);")
		self.check_deadlock(node, Kind.SM, Kind.H,
		                    "update h_row set v = v + 1 where id = 1;",
		                    "update h_row set v = v + 1 where id = 2;")
		self.finish(node)

	def test_deadlock_across_tables(self):
		# The cycle runs through a heap row and an OrioleDB row
		node = self.start()
		node.safe_psql("insert into o_row values (1, 0);"
		               "insert into h_row values (1, 0);")
		self.check_deadlock(node, Kind.SM, Kind.M,
		                    "update o_row set v = v + 1 where id = 1;",
		                    "update h_row set v = v + 1 where id = 1;")
		self.finish(node)


class LazyHeapXidLockStreamingTest(LazyHeapXidLockTest):
	"""
	The same with the transactions streamed to a subscriber while they wait.
	"""

	big = True

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf',
		                      "logical_decoding_work_mem = 64kB\n")

	def start(self):
		node = super().start()
		self.subscriber = self.getSubscriber()
		self.subscriber.start()
		self.subscriber.safe_psql(self.tables)
		pub = node.publish('lock_pub', tables=list(self.data_tables))
		self.subscriber.subscribe(pub, SUB, streaming='on')
		wait_ready(self.subscriber)
		return node

	def digest(self, node, rel):
		return node.execute(f"""
			select count(*), coalesce(sum(id), 0),
				md5(coalesce(string_agg(t::text, ',' order by id), ''))
				from {rel} t;
		""")[0]

	def finish(self, node):
		self.assertTrue(
		    self.wait_until(lambda: all(
		        self.digest(node, r) == self.digest(self.subscriber, r)
		        for r in self.data_tables)),
		    "subscriber differs: " + ", ".join(
		        f"{r} {self.digest(node, r)} vs {self.digest(self.subscriber, r)}"
		        for r in self.data_tables))
		stream_txns = node.execute(f"""
			select stream_txns from pg_stat_replication_slots
				where slot_name = '{SUB}';
		""")[0][0]
		self.assertGreaterEqual(stream_txns, 1, "nothing was streamed")
