#!/usr/bin/env python3
# coding: utf-8
"""
CREATE PUBLICATION / CREATE SUBSCRIPTION between a heap table on one node and
an OrioleDB table on the other, in both directions.

The tests are written once, in LogicalHeapOrioleMixin, and instantiated for
each direction by the two concrete classes at the bottom.
"""

import time

from testgres.exceptions import QueryException

from .base_test import BaseTest
from .logical_test import wait_ready

WAIT_TIMEOUT_S = 60


class LogicalHeapOrioleMixin:
	# Table access methods on the publisher and on the subscriber
	pub_am = None
	sub_am = None

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")

	def create_tables(self, publisher, subscriber, ddl):
		"""
		Create the tables from the list of `ddl` statement templates on both
		nodes, with the publisher's and the subscriber's table access method
		respectively.  The templates may contain the `{am}` placeholder.
		"""
		setup = ["CREATE EXTENSION IF NOT EXISTS orioledb"]
		pub_sql = ";".join(setup +
		                   [stmt.format(am=self.pub_am) for stmt in ddl])
		sub_sql = ";".join(setup +
		                   [stmt.format(am=self.sub_am) for stmt in ddl])
		publisher.safe_psql(pub_sql)
		subscriber.safe_psql(sub_sql)

	@staticmethod
	def other_am(am):
		return 'heap' if am == 'orioledb' else 'orioledb'

	def create_mixed_tables(self, publisher, subscriber):
		"""
		Create t1 with each node's own table access method and t2 with the
		other one, so a transaction touching both is mixed on the publisher
		as well as on the subscriber.
		"""
		ddl = ["CREATE TABLE t1(id int PRIMARY KEY, data text) USING {am}"]
		self.create_tables(publisher, subscriber, ddl)
		stmt = "CREATE TABLE t2(id int PRIMARY KEY, data text) USING {am}"
		publisher.safe_psql(stmt.format(am=self.other_am(self.pub_am)))
		subscriber.safe_psql(stmt.format(am=self.other_am(self.sub_am)))

	def execute_transaction(self, node, statements, rollback=False):
		"""
		Run `statements` as one transaction on `node`, the publisher or the
		subscriber alike.  `statements` is a list of statements, or a single
		statement.  The transaction is committed, or rolled back if
		`rollback` is set.
		"""
		if isinstance(statements, str):
			statements = [statements]
		end = "ROLLBACK" if rollback else "COMMIT"
		node.safe_psql(";".join(["BEGIN"] + statements + [end]))

	def wait_for_log(self, node, text, timeout=WAIT_TIMEOUT_S):
		"""Wait until `text` shows up in the server log of `node`."""
		deadline = time.time() + timeout
		while time.time() < deadline:
			with open(node.pg_log_file) as f:
				if text in f.read():
					return True
			time.sleep(0.2)
		return False

	def verify_oriole(self, node, am, relname):
		"""Check the B-trees of `relname` if it is an OrioleDB table."""
		if am == 'orioledb':
			self.assertEqual(
			    node.execute(
			        f"SELECT * FROM verify_orioledb('{relname}'::regclass)"),
			    [])

	def test_dml(self):
		"""INSERT, UPDATE and DELETE in separate transactions."""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				publisher.safe_psql(
				    "INSERT INTO t SELECT i, 'v' || i FROM generate_series(1, 5) i"
				)
				sub.catchup()
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(1, 'v1'), (2, 'v2'), (3, 'v3'), (4, 'v4'), (5, 'v5')])

				publisher.safe_psql(
				    "UPDATE t SET data = 'upd' WHERE id IN (2, 4)")
				sub.catchup()
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(1, 'v1'), (2, 'upd'), (3, 'v3'), (4, 'upd'), (5, 'v5')])

				publisher.safe_psql("DELETE FROM t WHERE id IN (1, 5)")
				sub.catchup()
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(2, 'upd'), (3, 'v3'), (4, 'upd')])

	def test_initial_sync(self):
		"""Rows present before CREATE SUBSCRIPTION are copied, then streamed."""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)
				publisher.safe_psql("INSERT INTO t SELECT i, 'pre' || i "
				                    "FROM generate_series(1, 1000) i")

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				self.assertEqual(
				    subscriber.execute('SELECT count(*), min(id), max(id) '
				                       'FROM t'), [(1000, 1, 1000)])

				sql = [
				    "INSERT INTO t VALUES (1001, 'post')",
				    "UPDATE t SET data = 'changed' WHERE id = 1",
				]
				publisher.safe_psql(";".join(sql))
				sub.catchup()

				self.assertEqual(subscriber.execute('SELECT count(*) FROM t'),
				                 [(1001, )])
				self.assertEqual(
				    subscriber.execute(
				        'SELECT data FROM t WHERE id IN (1, 1001) '
				        'ORDER BY id'), [('changed', ), ('post', )])

	def test_pkey_update(self):
		"""An UPDATE that changes the primary key."""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				sql = [
				    "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
				    "UPDATE t SET id = 10 WHERE id = 1",
				    "UPDATE t SET id = id + 100, data = data || '!' "
				    "WHERE id IN (2, 3)",
				]
				publisher.safe_psql(";".join(sql))
				sub.catchup()

				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(10, 'a'), (102, 'b!'), (103, 'c!')])

	def test_toast(self):
		"""
		Values too large and too random to stay inline, inserted and updated.

		The data has to be incompressible.  A compressible value such as
		repeat('x', n) shrinks below the TOAST threshold and is never stored
		out of line, so it would not reach the TOAST decoding path at all.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, small text, big text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				# Random hex does not compress.  The subquery depends on the
				# row, so each row gets a different value.
				big = (
				    "(SELECT string_agg(md5(random()::text || g || {row}), '') "
				    "FROM generate_series(1, {n}) g)")
				content = ("SELECT id, small, length(big), md5(big) "
				           "FROM t ORDER BY id")

				publisher.safe_psql("INSERT INTO t SELECT i, 's' || i, " +
				                    big.format(row='i', n=500) +
				                    " FROM generate_series(1, 5) i")
				sub.catchup()
				self.assertEqual(
				    subscriber.execute("SELECT count(*), min(length(big)), "
				                       "max(length(big)) FROM t"),
				    [(5, 16000, 16000)])
				self.assertEqual(subscriber.execute(content),
				                 publisher.execute(content))

				# Replace a TOASTed value with a much larger one
				publisher.safe_psql("UPDATE t SET big = " +
				                    big.format(row='id', n=3000) +
				                    " WHERE id = 1")
				sub.catchup()
				self.assertEqual(
				    subscriber.execute("SELECT length(big) FROM t "
				                       "WHERE id = 1"), [(96000, )])
				self.assertEqual(subscriber.execute(content),
				                 publisher.execute(content))

				# Update only the small column: the TOASTed value is not
				# part of the change and must be left as it was.
				before = subscriber.execute("SELECT md5(big) FROM t "
				                            "WHERE id = 2")
				publisher.safe_psql(
				    "UPDATE t SET small = 'changed' WHERE id = 2")
				sub.catchup()
				self.assertEqual(
				    subscriber.execute("SELECT small, md5(big) FROM t "
				                       "WHERE id = 2"),
				    [('changed', before[0][0])])
				self.assertEqual(subscriber.execute(content),
				                 publisher.execute(content))

	def test_replica_identity_full(self):
		"""No primary key, REPLICA IDENTITY FULL on both sides."""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(a int, b text) USING {am}",
				    "ALTER TABLE t REPLICA IDENTITY FULL",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				sql = [
				    "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
				    "UPDATE t SET b = 'TWO' WHERE a = 2",
				    "DELETE FROM t WHERE a = 3",
				]
				publisher.safe_psql(";".join(sql))
				sub.catchup()

				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY a'),
				    [(1, 'one'), (2, 'TWO')])

	def test_secondary_index_and_rollback(self):
		"""
		Rolled back and savepoint-rolled-back changes are not replicated, and
		leave nothing behind in the secondary index.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				    "CREATE INDEX t_data_idx ON t(data)",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				sql = [
				    "INSERT INTO t VALUES (1, 'gone')",
				]
				self.execute_transaction(publisher, sql, rollback=True)
				sql = [
				    "INSERT INTO t VALUES (2, 'kept')",
				    "SAVEPOINT s",
				    "INSERT INTO t VALUES (3, 'gone')",
				    "ROLLBACK TO s",
				    "INSERT INTO t VALUES (4, 'kept')",
				]
				self.execute_transaction(publisher, sql)
				sub.catchup()

				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(2, 'kept'), (4, 'kept')])

				# Go through the secondary index on both nodes: with a
				# sequential scan the check would pass even if the index
				# were inconsistent.
				for node, am in ((publisher, self.pub_am), (subscriber,
				                                            self.sub_am)):
					with node.connect() as con:
						con.execute("SET enable_seqscan = off")
						con.execute("SET enable_bitmapscan = off")
						plan = "\n".join(row[0] for row in con.execute(
						    "EXPLAIN SELECT id FROM t WHERE data = 'kept'"))
						self.assertIn("t_data_idx", plan)
						self.assertEqual(
						    con.execute("SELECT id FROM t WHERE data = 'kept' "
						                "ORDER BY id"), [(2, ), (4, )])
						self.assertEqual(
						    con.execute(
						        "SELECT id FROM t WHERE data = 'gone'"), [])
					self.verify_oriole(node, am, 't')

	def test_mixed_transaction(self):
		"""
		One transaction modifies both a heap and an OrioleDB table, which are
		published together.  t1 uses the node's own table access method and t2
		the other one, so the transaction is mixed on both nodes.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				self.create_mixed_tables(publisher, subscriber)

				pub = publisher.publish('test_pub', tables=['t1', 't2'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				sql = [
				    "INSERT INTO t1 VALUES (1, 'a')",
				    "INSERT INTO t2 VALUES (1, 'x')",
				    "INSERT INTO t1 VALUES (2, 'b')",
				    "UPDATE t2 SET data = 'y' WHERE id = 1",
				    "DELETE FROM t1 WHERE id = 1",
				]
				self.execute_transaction(publisher, sql)
				sub.catchup()

				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t1 ORDER BY id'),
				    [(2, 'b')])
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t2 ORDER BY id'),
				    [(1, 'y')])

	def test_truncate(self):
		"""TRUNCATE on the publisher is applied on the subscriber."""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				publisher.safe_psql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
				sub.catchup()
				self.assertEqual(subscriber.execute('SELECT count(*) FROM t'),
				                 [(2, )])

				sql = [
				    "TRUNCATE t",
				    "INSERT INTO t VALUES (3, 'c')",
				]
				publisher.safe_psql(";".join(sql))
				sub.catchup()
				self.assertListEqual(subscriber.execute('SELECT * FROM t'),
				                     [(3, 'c')])

	def test_no_replica_identity(self):
		"""
		UPDATE and DELETE are refused on the publisher when the table has no
		replica identity, while INSERT keeps working.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = ["CREATE TABLE t(a int, b text) USING {am}"]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				publisher.safe_psql("INSERT INTO t VALUES (1, 'a')")
				sub.catchup()

				with self.assertRaises(QueryException) as e:
					publisher.safe_psql("UPDATE t SET b = 'x' WHERE a = 1")
				self.assertIn(
				    "does not have a replica identity and publishes updates",
				    str(e.exception))
				with self.assertRaises(QueryException) as e:
					publisher.safe_psql("DELETE FROM t WHERE a = 1")
				self.assertIn(
				    "does not have a replica identity and publishes deletes",
				    str(e.exception))

				# The refused statements changed nothing, and replication
				# carries on.
				publisher.safe_psql("INSERT INTO t VALUES (2, 'b')")
				sub.catchup()
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY a'),
				    [(1, 'a'), (2, 'b')])

	def test_insert_conflict_on_subscriber(self):
		"""
		An INSERT that violates a unique constraint on the subscriber fails
		the apply of the whole transaction, and replication recovers once the
		conflicting row is removed.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				subscriber.safe_psql("INSERT INTO t VALUES (1, 'local')")
				sql = [
				    "INSERT INTO t VALUES (1, 'remote')",
				    "INSERT INTO t VALUES (2, 'remote2')",
				]
				publisher.safe_psql(";".join(sql))

				self.assertTrue(
				    self.wait_for_log(
				        subscriber,
				        "duplicate key value violates unique constraint"))
				# The transaction is applied as a whole or not at all
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(1, 'local')])

				subscriber.safe_psql("DELETE FROM t WHERE id = 1")
				sub.catchup()
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(1, 'remote'), (2, 'remote2')])
				self.verify_oriole(subscriber, self.sub_am, 't')

	def test_missing_row_on_subscriber(self):
		"""
		UPDATE and DELETE of a row that is missing on the subscriber are
		skipped, and do not stop later changes from being applied.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				publisher.safe_psql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
				sub.catchup()

				subscriber.safe_psql("DELETE FROM t WHERE id = 1")
				sql = [
				    "UPDATE t SET data = 'x' WHERE id = 1",
				    "DELETE FROM t WHERE id = 1",
				    "INSERT INTO t VALUES (3, 'c')",
				]
				publisher.safe_psql(";".join(sql))
				sub.catchup()

				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t ORDER BY id'),
				    [(2, 'b'), (3, 'c')])

	def test_rollback_mixed_transaction(self):
		"""
		ROLLBACK and ROLLBACK TO SAVEPOINT in a transaction that touches both
		a heap and an OrioleDB table, starting with either of them.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				self.create_mixed_tables(publisher, subscriber)

				pub = publisher.publish('test_pub', tables=['t1', 't2'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				sql = [
				    "INSERT INTO t1 VALUES (1, 'gone')",
				    "INSERT INTO t2 VALUES (1, 'gone')",
				]
				self.execute_transaction(publisher, sql, rollback=True)
				# t1 first, then t2 inside the savepoint
				sql = [
				    "INSERT INTO t1 VALUES (2, 'kept')",
				    "SAVEPOINT s",
				    "INSERT INTO t2 VALUES (2, 'gone')",
				    "INSERT INTO t1 VALUES (3, 'gone')",
				    "ROLLBACK TO s",
				    "INSERT INTO t2 VALUES (4, 'kept')",
				]
				self.execute_transaction(publisher, sql)
				# t2 first, then t1 inside the savepoint
				sql = [
				    "INSERT INTO t2 VALUES (5, 'kept')",
				    "SAVEPOINT s",
				    "INSERT INTO t1 VALUES (6, 'gone')",
				    "ROLLBACK TO s",
				    "INSERT INTO t1 VALUES (7, 'kept')",
				]
				self.execute_transaction(publisher, sql)
				sub.catchup()

				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t1 ORDER BY id'),
				    [(2, 'kept'), (7, 'kept')])
				self.assertListEqual(
				    subscriber.execute('SELECT * FROM t2 ORDER BY id'),
				    [(4, 'kept'), (5, 'kept')])

	def test_unpublished_table(self):
		"""
		Changes to tables outside the publication, of either kind, are not
		replicated even when they share a transaction with published ones.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				self.create_mixed_tables(publisher, subscriber)
				ddl = [
				    "CREATE TABLE t3(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub = publisher.publish('test_pub', tables=['t1'])
				sub = subscriber.subscribe(pub, 'test_sub')
				wait_ready(subscriber)

				sql = [
				    "INSERT INTO t2 VALUES (1, 'unpublished')",
				    "INSERT INTO t1 VALUES (1, 'published')",
				    "INSERT INTO t3 VALUES (1, 'unpublished')",
				]
				self.execute_transaction(publisher, sql)
				sub.catchup()

				self.assertListEqual(subscriber.execute('SELECT * FROM t1'),
				                     [(1, 'published')])
				self.assertListEqual(subscriber.execute('SELECT * FROM t2'),
				                     [])
				self.assertListEqual(subscriber.execute('SELECT * FROM t3'),
				                     [])
				# They were written on the publisher
				self.assertListEqual(publisher.execute('SELECT * FROM t2'),
				                     [(1, 'unpublished')])
				self.assertListEqual(publisher.execute('SELECT * FROM t3'),
				                     [(1, 'unpublished')])

	def test_copy_data_false(self):
		"""With copy_data = false the existing rows are not copied."""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)
				publisher.safe_psql("INSERT INTO t SELECT i, 'old' || i "
				                    "FROM generate_series(1, 10) i")

				pub = publisher.publish('test_pub', tables=['t'])
				sub = subscriber.subscribe(pub, 'test_sub', copy_data='false')
				wait_ready(subscriber)

				self.assertEqual(subscriber.execute('SELECT count(*) FROM t'),
				                 [(0, )])

				publisher.safe_psql("INSERT INTO t VALUES (11, 'new')")
				sub.catchup()
				self.assertListEqual(subscriber.execute('SELECT * FROM t'),
				                     [(11, 'new')])

	def test_origin_none_no_echo(self):
		"""
		Two nodes replicate the same table to each other with origin = none.
		A change applied by a subscription must not be sent back to the node
		it came from: the echo would violate the primary key and keep the
		apply worker failing.
		"""
		with self.node as publisher:
			publisher.start()
			subscriber = self.getSubsriber()
			with subscriber.start() as subscriber:
				ddl = [
				    "CREATE TABLE t(id int PRIMARY KEY, data text) USING {am}",
				]
				self.create_tables(publisher, subscriber, ddl)

				pub_a = publisher.publish('pub_a', tables=['t'])
				pub_b = subscriber.publish('pub_b', tables=['t'])
				sub_b = subscriber.subscribe(pub_a, 'sub_b', origin='none')
				sub_a = publisher.subscribe(pub_b, 'sub_a', origin='none')
				wait_ready(subscriber)
				wait_ready(publisher)

				publisher.safe_psql("INSERT INTO t VALUES (1, 'from_a')")
				sub_b.catchup()
				subscriber.safe_psql("INSERT INTO t VALUES (2, 'from_b')")
				sub_a.catchup()
				# Give an echo, if there were one, time to travel back
				sub_b.catchup()
				sub_a.catchup()

				expected = [(1, 'from_a'), (2, 'from_b')]
				for node in (publisher, subscriber):
					self.assertListEqual(
					    node.execute('SELECT * FROM t ORDER BY id'), expected)
					with open(node.pg_log_file) as f:
						self.assertNotIn("duplicate key", f.read())


class LogicalHeapToOrioleTest(LogicalHeapOrioleMixin, BaseTest):
	pub_am = 'heap'
	sub_am = 'orioledb'


class LogicalOrioleToHeapTest(LogicalHeapOrioleMixin, BaseTest):
	pub_am = 'orioledb'
	sub_am = 'heap'

	def test_unsupported_replica_identity(self):
		"""
		REPLICA IDENTITY NOTHING and USING INDEX are refused on an OrioleDB
		table, and leave the replica identity unchanged.
		"""
		with self.node as node:
			node.start()
			setup = [
			    "CREATE EXTENSION IF NOT EXISTS orioledb",
			    "CREATE TABLE t(a int NOT NULL, b text) USING orioledb",
			    "CREATE UNIQUE INDEX t_a_idx ON t(a)",
			]
			node.safe_psql(";".join(setup))

			for identity in ("NOTHING", "USING INDEX t_a_idx"):
				with self.assertRaises(QueryException) as e:
					node.safe_psql(
					    f"ALTER TABLE t REPLICA IDENTITY {identity}")
				self.assertIn("is not supported for OrioleDB tables yet",
				              str(e.exception))
				self.assertEqual(
				    node.execute("SELECT relreplident::text FROM pg_class "
				                 "WHERE relname = 't'"), [('d', )])
