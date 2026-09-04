#!/usr/bin/env python3
# coding: utf-8

import time
import unittest
from threading import Thread

from .base_test import BaseTest


class UndoReservedRaceTest(BaseTest):

	def test_reserved_location_does_not_pull_frontiers_back(self):
		"""minProcReservedLocation must never move the drain frontier back.

		undo_write_internal() publishes reservedUndoLocation from the
		writeInProgressLocation it read a moment earlier, then re-checks that
		frontier and retries if it has moved.  Between those two steps the
		reservation is visible to update_min_undo_locations(), which -- unlike
		the two retain locations beside it -- publishes minProcReservedLocation
		without clamping it to its previous value.  So a scan landing in that
		window lowers it below writeInProgressLocation, and the next eviction
		clamps its target to that, dragging writeInProgressLocation and then
		writtenLocation backwards.

		A backend that had already concluded from the higher
		writeInProgressLocation that its undo was covered then finds
		writtenLocation short of what it needs.
		"""
		node = self.node
		node.append_conf('orioledb.enable_stopevents = true\n')
		node.append_conf('orioledb.undo_buffers = 256\n')
		node.append_conf('orioledb.main_buffers = 64MB\n')
		node.append_conf('checkpoint_timeout = 1h\n')
		node.start()
		node.safe_psql("""
			CREATE EXTENSION orioledb;
			CREATE TABLE o_race (id int PRIMARY KEY, v int, pad text) USING orioledb;
			INSERT INTO o_race SELECT g, 0, repeat('x', 500)
				FROM generate_series(1, 2000) g;
			CREATE TABLE o_bulk (id int PRIMARY KEY, pad text) USING orioledb;
			INSERT INTO o_bulk SELECT g, repeat('b', 2000)
				FROM generate_series(1, 20000) g;
		""")

		def undo_meta(con):
			return con.execute("""
				SELECT writeInProgressLocation, writtenLocation,
				       minProcReservedLocation
				FROM orioledb_get_undo_meta() WHERE undo_type = 'row';
			""")[0]

		ctl = node.connect()
		locker = node.connect()
		park = node.connect()
		bulk = node.connect()

		def undo_meta_of(con):
			return con.execute("""
				SELECT writeInProgressLocation, writtenLocation,
				       minProcReservedLocation
				FROM orioledb_get_undo_meta() WHERE undo_type = 'row';
			""")[0]

		def parked_pids():
			return [
			    p for row in ctl.execute(
			        "SELECT waiter_pids FROM pg_stopevents();") if row[0]
			    for p in row[0]
			]

		def wait_parked(what):
			deadline = time.time() + 60
			while time.time() < deadline:
				pids = parked_pids()
				if pids:
					return pids[0]
				time.sleep(0.1)
			self.fail("nothing parked at %s" % what)

		def churn(n):
			for _ in range(n):
				bulk.begin()
				bulk.execute("UPDATE o_bulk SET pad = repeat('c', 2000);")
				bulk.commit()

		try:
			# Give the row an undo chain, so there is a chain to clean later.
			locker.begin()
			locker.execute("UPDATE o_race SET v = v + 1 WHERE id = 1;")
			locker.commit()

			ctl.execute(
			    "SELECT pg_stopevent_set('undo_write_before_reserve', 'true');"
			)
			ctl.execute(
			    "SELECT pg_stopevent_set('undo_write_after_reserve', 'true');")

			# One transaction locks the tuple, a second queues behind it.  When
			# the first finishes it is the waiter that strips the lock-only
			# record from the chain -- clean_chain_has_locks_flag() ->
			# undo_write() -- which is the path both events sit on.
			locker.begin()
			locker.execute("SELECT * FROM o_race WHERE id = 1 FOR UPDATE;")

			def parker():
				try:
					park.begin()
					park.execute(
					    "SELECT * FROM o_race WHERE id = 1 FOR UPDATE;")
					park.commit()
				except Exception as e:
					print("parker: %s" % str(e)[:100], flush=True)

			thread = Thread(target=parker)
			thread.start()
			time.sleep(1.0)
			locker.commit()

			wait_parked("undo_write_before_reserve")

			# Nothing is published yet, so the frontier is free: drive it far
			# past the location the waiter already read.
			churn(3)
			advanced = undo_meta_of(ctl)

			# Let it publish that now-stale reservation and stop again before
			# it can re-check.
			ctl.execute(
			    "SELECT pg_stopevent_reset('undo_write_before_reserve');")
			wait_parked("undo_write_after_reserve")

			# Somebody scans the per-proc locations while the stale value is
			# published.  This is the step that lowers minProcReservedLocation.
			ctl.execute("SELECT orioledb_get_undo_meta();")
			bulk.begin()
			bulk.execute("SELECT count(*) FROM o_bulk;")
			bulk.commit()
			scanned = undo_meta_of(ctl)

			ctl.execute(
			    "SELECT pg_stopevent_reset('undo_write_after_reserve');")
			thread.join(timeout=60)

			# And now an eviction, whose target is clamped by that value.
			churn(2)
			evicted = undo_meta_of(ctl)

			print("advanced wip=%d written=%d reserved=%d" % advanced,
			      flush=True)
			print("scanned  wip=%d written=%d reserved=%d" % scanned,
			      flush=True)
			print("evicted  wip=%d written=%d reserved=%d" % evicted,
			      flush=True)

			# The invariant is not that this value never decreases -- a
			# reservation is taken at the current frontier, so it legitimately
			# reads lower than a moment when nobody held one.  What must hold
			# is that it never drops *below* the frontier: evict_undo_to_disk()
			# clamps its target to it, so a lower value drags the frontier
			# back and breaks what reserve_undo_size_extended() concluded.
			self.assertGreaterEqual(
			    scanned[2], scanned[0],
			    "minProcReservedLocation fell below writeInProgressLocation")
			self.assertGreaterEqual(evicted[0], advanced[0],
			                        "writeInProgressLocation went backwards")
			self.assertGreaterEqual(evicted[1], advanced[1],
			                        "writtenLocation went backwards")
		finally:
			for ev in ('undo_write_before_reserve',
			           'undo_write_after_reserve'):
				try:
					ctl.execute("SELECT pg_stopevent_reset('%s');" % ev)
				except Exception:
					pass
			for con in (park, bulk, locker, ctl):
				try:
					con.close()
				except Exception:
					pass
