# A sequential scan must see the scanning transaction's own writes even when
# the leaf it reads has been given a page-level undo image by somebody else.
#
# The descent used to read leaf pages at the scan's snapshot csn, i.e. already
# rolled back through page-level undo.  For a tree that is a single leaf page
# that image *is* the whole scan, and the live/historical merge in
# btree_seq_scan_getnext_internal() -- the only thing that puts the scanning
# transaction's own uncommitted versions back into the result -- had no live
# page left to merge them from.  So a REPEATABLE READ transaction would read
# the pre-update version of a row it had just written itself, or miss a row it
# had just inserted.
#
# The setup fills a single leaf close to capacity.  s2 then shrinks most of the
# rows, which turns their space into vacated bytes, and inserts a row too big
# for the remaining contiguous free space: that forces
# perform_page_compaction(), which stores the old page in undo and stamps the
# live page with a fresh csn -- newer than s1's snapshot.  From then on any
# read of that page at s1's snapshot csn goes through page-level undo.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_seqscan_own (
		id int4 NOT NULL,
		val text NOT NULL,
		PRIMARY KEY (id)
	) USING orioledb;

	-- 22 rows of ~340 bytes: one leaf, a few hundred bytes of free space left.
	INSERT INTO o_seqscan_own
		SELECT i * 10, repeat('x', 300) FROM generate_series(1, 22) i;
}

teardown
{
	DROP TABLE o_seqscan_own;
}

session "s1"
step "s1_begin" {
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_bitmapscan = off;
	SET LOCAL enable_indexonlyscan = off;
	SET LOCAL enable_seqscan = on; }

# Pins the snapshot, and with it the undo the page image below is built from.
step "s1_snapshot" {
	SELECT count(*) FROM o_seqscan_own; }

step "s1_update" {
	UPDATE o_seqscan_own SET val = 'mine' WHERE id = 200; }
step "s1_insert" {
	INSERT INTO o_seqscan_own VALUES (5, 'fresh'); }

# Sequential scans: these are what used to answer from a rolled-back image.
step "s1_seqscan_updated" {
	SELECT id, val FROM o_seqscan_own WHERE id = 200; }
step "s1_seqscan_inserted" {
	SELECT id, val FROM o_seqscan_own WHERE id = 5; }
step "s1_seqscan_count" {
	SELECT count(*) FROM o_seqscan_own; }

step "s1_commit" { COMMIT; }

session "s2"
# Shrinking the rows leaves their space behind as vacated bytes.
step "s2_shrink" {
	UPDATE o_seqscan_own SET val = repeat('z', 10) WHERE id <= 150; }
# Does not fit the contiguous free space, does fit after compaction -- so this
# is what gives the leaf its page-level undo image and a fresh csn.
step "s2_compact" {
	INSERT INTO o_seqscan_own VALUES (115, repeat('w', 1200)); }

permutation "s1_begin" "s1_snapshot" "s2_shrink" "s2_compact"
	"s1_update" "s1_seqscan_updated"
	"s1_insert" "s1_seqscan_inserted" "s1_seqscan_count" "s1_commit"
