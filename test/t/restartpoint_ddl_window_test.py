#!/usr/bin/env python3
# coding: utf-8

import time

from testgres.enums import NodeStatus

from .base_test import BaseTest, ThreadQueryExecutor


class RestartpointDdlWindowTest(BaseTest):

	def wait_replay_parked(self, replica):
		"""Wait until the recovery leader sits on the replay_on_record event."""
		deadline = time.time() + 60
		while time.time() < deadline:
			parked = replica.execute("""
				SELECT EXISTS(
					SELECT 1 FROM pg_stopevents() se
					WHERE se.stopevent = 'replay_on_record'
					  AND array_length(se.waiter_pids, 1) > 0);
			""")[0][0]
			if parked:
				return
			time.sleep(0.1)
		raise AssertionError("replay did not stop at replay_on_record")

	def test_restartpoint_inside_replayed_ddl_window(self):
		"""
		A restartpoint must not build an index descriptor out of a half-applied
		DDL.

		A primary index descriptor pairs two sys-tree rows: the leaf tuple
		descriptor from the OIndex record, the constraints from the OTable
		record.  A DDL writes them one after the other, and the checkpointer
		reads both under o_non_deleted_snapshot, which shows uncommitted rows.
		On the primary oTablesMetaLock keeps the two apart; recovery does not
		take it, so a standby's restartpoint can land between the writes.

		Park replay right before the O_TABLES modifies of an ALTER TABLE whose
		O_INDICES modifies are already in, then make the standby take a
		restartpoint.  It used to fail
		Assert("tupdesc->natts == all_attrs") in o_tupdesc_load_constr().
		"""
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.enable_stopevents = true\n"
		    "checkpoint_timeout = 1d\n")
		node.start()

		with self.getReplica().start() as replica:
			node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")
			# The standby can only take a restartpoint once it has replayed a
			# checkpoint record, so get that out of the way before the table
			# exists -- the restartpoint it triggers must not warm the
			# checkpointer's descriptor cache for the table under test, or the
			# restartpoint below would reuse the cached descriptor instead of
			# reading the two sys-tree rows again.
			node.safe_psql("CHECKPOINT;")
			self.catchup_orioledb(replica)

			node.safe_psql("""
				CREATE TABLE o_test (
					id int NOT NULL,
					val text NOT NULL,
					PRIMARY KEY (id)
				) USING orioledb;
			""")
			node.safe_psql(
			    "INSERT INTO o_test "
			    "(SELECT id, 'x' FROM generate_series(1, 2000) id);")
			self.catchup_orioledb(replica)

			# Stop the recovery leader on the record that switches to the
			# O_TABLES sys tree: everything the ALTER wrote to O_INDICES is
			# applied, nothing it wrote to O_TABLES is.
			replica.safe_psql("SELECT pg_stopevent_set('replay_on_record', "
			                  "'$.type == \"RELATION\" && $.systree == 2');")

			node.safe_psql("ALTER TABLE o_test ADD COLUMN val2 int;")
			self.wait_replay_parked(replica)

			# CHECKPOINT on a standby is a restartpoint.  It runs in its own
			# thread: past the tree walk it waits for the recovery workers,
			# which cannot answer while replay is parked.
			con = replica.connect()
			t = ThreadQueryExecutor(con, "CHECKPOINT;")
			t.start()
			time.sleep(3)

			self.assertEqual(replica.status(), NodeStatus.Running,
			                 "the standby died during the restartpoint")

			replica.safe_psql("SELECT pg_stopevent_reset('replay_on_record');")
			t.join()
			con.close()

			self.catchup_orioledb(replica)
			self.assertEqual(
			    replica.execute("SELECT count(*) FROM o_test;")[0][0], 2000)
			self.assertEqual(
			    replica.execute("SELECT count(*) FROM o_test "
			                    "WHERE val2 IS NULL;")[0][0], 2000)
			node.safe_psql("INSERT INTO o_test VALUES (2001, 'y', 7);")
			self.catchup_orioledb(replica)
			self.assertEqual(
			    replica.execute("SELECT val2 FROM o_test WHERE id = 2001;")[0]
			    [0], 7)
