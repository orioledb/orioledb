# Reproduces the case where IMAGE-mode find_page (used by btree
# iterators) ends up holding a real page lock at targetLevel + 1
# whose contents differ from the descent's cached parentImg.  If the
# descent leaves the parent's locator chunk pointer bound to shared
# memory (the real parent page) instead of parentImg, the iterator's
# subsequent find_right_page navigates through a pointer into a
# shared-memory page that the descent has already unlocked -- and
# that any concurrent insert/split can mutate.
#
# We drive the iterator via an ordered scan over the primary key:
# enable_seqscan = off forces an index scan, which uses
# o_btree_iterator_create -> find_page in IMAGE mode with targetLevel
# = 0 (so target+1 == 1, the parent of the leaf).
#
# Race we want, in find.c terms:
#   1. s1 (image-mode find_page) reads the parent at level == target+1
#      into parentImg with no lock, sees DOWNLINK_IS_ON_DISK, sets
#      needLock = true and `continue`s.  The descent parks on the
#      after_find_downlink stopevent right after page_find_downlink
#      filled in `loc` from the no-lock partial read of parentImg.
#   2. While s1 is parked, s2 itself drives an ordered scan that loads
#      every leaf under that parent into shared memory.  Every downlink
#      in the parent's shared-memory image is rewritten in place from
#      DOWNLINK_IS_ON_DISK to DOWNLINK_IS_IN_MEMORY.
#   3. s1 wakes, re-enters the loop with needLock = true, takes the
#      lock on the parent (intCxt.pagePtr is now the *real* in-memory
#      page, not parentImg), and finds DOWNLINK_IS_IN_MEMORY -- so it
#      falls through to step-down via the IN_MEMORY else-branch
#      without going through load_page, which would have refreshed
#      parentImg.  At that point intCxt.pagePtr != context->parentImg
#      and intCxt.haveLock is true: refresh_parent_img_chunk() must
#      fire and rebind the parent locator's chunk pointer to parentImg.
#   4. s1 then parks again on the step_right stopevent (between the
#      leaf and find_right_page).  s2 inserts rows that are smaller
#      than every existing key, splitting the leftmost level-1 page in
#      shared memory.  Without step 3's rebind, find_right_page would
#      now navigate through a chunk pointer into a mutated shared page
#      and produce a wrong row count (or worse).

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	DROP TABLE IF EXISTS public.o_grow;
	CREATE TABLE public.o_grow (
		id bytea NOT NULL,
		val text NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb WITH (autovacuum_enabled = false);

	-- Big keys so a btree page only holds a few entries.  ~4000 rows of
	-- ~500-byte keys / ~200-byte values gives a depth-3 tree (root at
	-- level 2, internals at level 1, leaves at level 0).  We leave a
	-- gap below the smallest key so that s2_continue can insert keys
	-- on the left that force a split of the leftmost level-1 page.
	INSERT INTO o_grow (id, val)
		SELECT decode(to_char(id, 'fm0000') || repeat('a', 500), 'hex'),
			   repeat('v', 200)
		FROM generate_series(100, 4096, 1) id;

	-- Evict every leaf (level 0).  Root and level-1 pages stay in
	-- shared memory; level-1's downlinks all point at on-disk leaves,
	-- which is the precondition for the needLock = true branch of
	-- find_page at level == targetLevel + 1.
	SELECT orioledb_evict_pages('public.o_grow'::regclass, 0);
}

teardown
{
	DROP TABLE o_grow;
}

session "s1"

step "s1_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1';
}

# An ordered scan over the primary key forces an index scan, which
# uses o_btree_iterator_create -> find_page in IMAGE mode with
# targetLevel = 0.  After the iterator exhausts each leaf, it calls
# find_right_page, which navigates the parent (level 1) via the
# locator that the patch rebinds to parentImg.  We expect the fix
# branch (at level == targetLevel + 1 == 1) to fire while descending
# from the leftmost level-1 page into the leftmost leaf.
step "s1_scan" {
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) FROM (SELECT id FROM o_grow ORDER BY id) t;
}

session "s2"

step "s2_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's2';
}

# Park s1 immediately after page_find_downlink fills `loc` at level 1
# -- i.e., after the no-lock partial read of the level-1 page into
# parentImg, but before the descent has noticed the on-disk downlink
# and set needLock = true.  Filtering on applicationName keeps s2's
# own find_page calls from parking on this event.
step "s2_arm" {
	SELECT pg_stopevent_set('after_find_downlink',
		'$.treeName == "o_grow_pkey" && $.level == 1 && $applicationName == "s1"');
}

# While s1 is parked, drive an ordered scan from s2.  This iterator
# call hits the lock-and-load path on every level-1 downlink, flipping
# every one of them from DOWNLINK_IS_ON_DISK to DOWNLINK_IS_IN_MEMORY
# in the level-1 shared-memory pages.  parentImg in s1 still holds the
# pre-load copy of the leftmost level-1 page.
#
# Then arm step_right so s1 pauses between exhausting the leftmost
# leaf and running find_right_page, and release after_find_downlink so
# s1 resumes.  When s1 resumes it sets needLock = true, re-acquires
# the level-1 lock, sees the now-IN_MEMORY downlink and steps down via
# the IN_MEMORY else-branch.  At that point intCxt.pagePtr is the
# shared-memory level-1 page (not parentImg), so
# refresh_parent_img_chunk() must rebind the parent locator's chunk
# pointer to parentImg.
step "s2_load_and_reset" {
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT count(*) FROM (SELECT id FROM o_grow ORDER BY id) t;
	SELECT pg_stopevent_set('step_right',
		'$.treeName == "o_grow_pkey" && $applicationName == "s1"');
	SELECT pg_stopevent_reset('after_find_downlink');
}

# s1 is parked at step_right; the level-1 lock has long since been
# released.  Insert keys below every existing key.  These rows hit the
# leftmost level-1 page and force a split, mutating the shared-memory
# page that the descent's locator would still point into without the
# fix.  Then release step_right so s1 enters find_right_page on a
# parent whose shared-memory layout has just changed.
step "s2_continue" {
	INSERT INTO o_grow (id, val)
		SELECT decode(to_char(id, 'fm0000') || repeat('a', 500), 'hex'),
			   repeat('v', 200)
		FROM generate_series(1, 99, 1) id;
	SELECT pg_stopevent_reset('step_right');
}

permutation
	"s1_setup" "s2_setup" "s2_arm" "s1_scan" "s2_load_and_reset" "s2_continue"
