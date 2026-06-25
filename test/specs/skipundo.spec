# Regression tests for the skip-undo split / merge path in
# o_btree_insert_split() and btree_try_merge_pages(), and for the
# associated first-key lokey fallback in btree_find_context_lokey().
#
# Two permutations:
#
#  bscan_lokey  -- Drives a backward scan over pages that were produced
#                  by the skip-undo split path, hitting the fallback
#                  (load_page_refind temporarily unsets KEEP_LOKEY) on
#                  non-LEFTMOST leaves at parent offset 0.  Verifies
#                  the scan still returns the exact pre-split key set
#                  in descending order, i.e. the split-key invariant
#                  the fallback relies on holds.  Companion test for
#                  load_refind_page.spec, which already trips the
#                  fallback but with dense sequential keys where any
#                  returned lokey value would happen to work.
#
#  seqscan_race -- Covers the lazy numSeqScans registration window:
#                  init_btree_seq_scan() (which bumps the counter) is
#                  called on the first btree_seq_scan_getnext(), not at
#                  scan creation.  The skip-undo writer's retain-horizon
#                  test already covers any seq scan with an active
#                  snapshot (orioledb_snapshot_hook() publishes the
#                  page-level retain before the executor can fetch),
#                  but this permutation is the explicit guard: a seq
#                  scan that starts before concurrent skip-undo splits
#                  still sees every snapshot-visible row exactly once.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	CREATE TABLE IF NOT EXISTS o_skipfb (
		id text NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	TRUNCATE o_skipfb;

	CREATE TABLE IF NOT EXISTS o_skipss (
		id int NOT NULL,
		pad text NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;
	TRUNCATE o_skipss;
}

teardown
{
	DROP TABLE o_skipfb;
	DROP TABLE o_skipss;
}

session "s1"

# --- bscan_lokey steps ---------------------------------------------------

step "bs_s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

step "bs_s1_seed" {
	INSERT INTO o_skipfb
		(SELECT id || repeat('x', 2600)
		 FROM generate_series(18, 21, 1) id);
}

step "bs_s1_evict" {
	SELECT orioledb_evict_pages('o_skipfb'::regclass, 1);
}

step "bs_s1_arm_refind" {
	SELECT pg_stopevent_set('load_page_refind',
		'$.level == 0 && $applicationName == "s2"');
}

# Each of these inserts triggers a no-drop split.  The source leaf has
# undoLocation = Invalid (no prior page-level undo) and no tuple is
# dropped, so o_btree_insert_split() takes the skip-undo branch: both
# halves get csn = COMMITSEQNO_FROZEN and undoLocation = Invalid.
step "bs_s1_insert_low1" {
	INSERT INTO o_skipfb
		(SELECT id || repeat('x', 2600)
		 FROM generate_series(10, 13, 1) id);
}

step "bs_s1_insert_low2" {
	INSERT INTO o_skipfb
		(SELECT id || repeat('x', 2600)
		 FROM generate_series(14, 17, 1) id);
}

step "bs_s1_continue" {
	SELECT pg_stopevent_reset('load_page_refind');
}

# --- seqscan_race steps --------------------------------------------------

step "sr_s1_seed" {
	INSERT INTO o_skipss
		(SELECT i, repeat('p', 1000)
		 FROM generate_series(1, 200, 1) id(i));
}

step "sr_s1_begin" { BEGIN ISOLATION LEVEL REPEATABLE READ; }

# Materialise the snapshot at statement start; subsequent reads in this
# transaction see only rows visible at this point.  This count(*) does
# NOT use a seq scan over the orioledb btree (it's a planner-level
# count), so it does not register the scan via init_btree_seq_scan().
step "sr_s1_take_snap" {
	SELECT count(*) FROM o_skipss WHERE id <= 0;
}

# Force a seq scan in this transaction.  The point is to register the
# scan AFTER concurrent splits, then verify it still sees every
# snapshot-visible row exactly once.
step "sr_s1_seqscan" {
	SET enable_indexscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*), min(id), max(id), sum(id::bigint) FROM o_skipss;
}

step "sr_s1_commit" { COMMIT; }

session "s2"

# --- bscan_lokey steps ---------------------------------------------------

step "bs_s2_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's2';
	SET enable_seqscan = off;
}

# Backward scan over the surviving rows.  s2 is READ COMMITTED, so its
# snapshot is taken at statement start -- s1's later 10..17 inserts are
# invisible.  The scan parks at the level-0 refind, then resumes after
# bs_s1_continue and walks left through pages that are now frozen split
# halves.  Each backward step invokes btree_find_context_lokey() with
# LOKEY_EXISTS unset (load_page_refind dropped it during the parent
# re-find) and parent_off = 0, falling into the first-key fallback.
#
# Expected: 21, 20, 19, 18 in descending order.  If the fallback ever
# misreports a lokey (split-key invariant broken), one or more of these
# rows will be missing or returned out of order.
step "bs_s2_bscan" {
	SELECT substr(id, 1, 2) FROM o_skipfb ORDER BY id DESC;
}

# --- seqscan_race steps --------------------------------------------------

# Inserts above the existing range.  All sources have undoLocation =
# Invalid (no prior page-level undo), drop no tuple, and run while
# numSeqScans = 0 (s1 hasn't called getnext() yet), so they take the
# skip-undo branch and freeze their resulting halves.
step "sr_s2_split_inserts" {
	INSERT INTO o_skipss
		(SELECT i, repeat('p', 1000)
		 FROM generate_series(201, 400, 1) id(i));
}

# Counterpart deletes -- these don't change the seq scan's snapshot
# but stress the per-tuple MVCC path the scan now has to rely on.
step "sr_s2_deletes" {
	DELETE FROM o_skipss WHERE id BETWEEN 50 AND 60;
}

permutation "bs_s1_setup" "bs_s2_setup" "bs_s1_seed" "bs_s1_evict"
            "bs_s1_arm_refind" "bs_s2_bscan" "bs_s1_insert_low1"
            "bs_s1_insert_low2" "bs_s1_continue"

permutation "sr_s1_seed" "sr_s1_begin" "sr_s1_take_snap"
            "sr_s2_split_inserts" "sr_s2_deletes" "sr_s1_seqscan"
            "sr_s1_commit"
