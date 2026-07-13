# Regression test for the composite-PK bitmap scan losing a row under
# FOR UPDATE + EvalPlanQual.
#
# A "(w,i) IN (...)"/"i = ANY(...)" FOR UPDATE over a composite primary key is
# executed as an orioledb bitmap scan (o_scan).  When one of the locked rows is
# updated by a concurrent transaction, LockRows re-checks it through
# EvalPlanQual.  The bitmap branch of o_exec_custom_scan() must return the
# single EPQ-substituted tuple; if it instead re-executes the whole bitmap scan
# it yields that scan's first row (the range minimum) in place of the updated
# row, duplicating the minimum and dropping the concurrently-updated key.
#
# s2 locks i=30, s1's FOR UPDATE blocks on it, s2 commits, s1 rechecks i=30 via
# EPQ.  Correct output is {10,20,30,40}; the bug yields {10,20,10,40}.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE o_bfu (
		w	int,
		i	int,
		q	int,
		PRIMARY KEY (w, i)
	) USING orioledb;
	INSERT INTO o_bfu
		SELECT w, i, 0 FROM generate_series(1, 2) w, generate_series(1, 1000) i;
}

teardown
{
	DROP TABLE o_bfu;
}

session "s1"
# Force the composite-PK bitmap scan for this session's queries.
setup {
	SET enable_seqscan = off;
	SET enable_indexscan = off;
	SET enable_indexonlyscan = off;
}
step "s1_forupdate" {
	SELECT i FROM o_bfu WHERE w = 1 AND i IN (10, 20, 30, 40) FOR UPDATE;
}

session "s2"
step "s2_update" {
	BEGIN;
	UPDATE o_bfu SET q = q + 1 WHERE w = 1 AND i = 30;
}
step "s2_commit" {
	COMMIT;
}

# s2 locks i=30; s1 scans/locks 10,20 then blocks on 30; s2 commits; s1
# rechecks the updated i=30 through EPQ and must still return it.
permutation "s2_update" "s1_forupdate" "s2_commit"
