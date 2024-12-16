setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_merge (
		id int8 NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_merge;
	INSERT INTO o_merge (SELECT i FROM generate_series(1, 1000) AS i);
	SET enable_seqscan = off;
}

teardown
{
	DROP TABLE o_merge;
}

session "s1"

step "s1_count" { SET enable_seqscan = off; SELECT COUNT(*) FROM o_merge; }
step "s1_bcount" { SET enable_seqscan = off; SELECT COUNT(*) FROM ( SELECT * FROM o_merge ORDER BY id DESC) t; }
step "s1_begin" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s1_commit" { COMMIT; }
step "s1_structure" { SELECT orioledb_idx_structure('o_merge'::regclass, 'o_merge_pkey', 'nue', 1); }
step "s1_delete500_begin" { DELETE FROM o_merge WHERE id BETWEEN 1 AND 500; }
step "s1_delete500_end" { DELETE FROM o_merge WHERE id BETWEEN 501 AND 1000; }
step "s1_delete_80_perc" { DELETE FROM o_merge WHERE id % 5 <> 0; }
step "s1_evict" { SELECT orioledb_evict_pages('o_merge'::regclass, 1); }

session "s2"

step "s2_count" { SET enable_seqscan = off; SELECT COUNT(*) FROM o_merge; }
step "s2_bcount" { SET enable_seqscan = off; SELECT COUNT(*) FROM ( SELECT * FROM o_merge ORDER BY id DESC) t; }
step "s2_begin" { BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; }
step "s2_commit" { COMMIT; }
step "s2_structure" { SELECT orioledb_tbl_structure('o_merge'::regclass, 'nue', 1); }
step "s2_evict" { SELECT orioledb_evict_pages('o_merge'::regclass, 1); }

permutation "s1_structure" "s1_delete500_begin" "s1_evict" "s1_count" "s1_structure" "s1_delete500_end" "s1_evict" "s1_count" "s1_structure"
permutation "s1_structure" "s1_delete_80_perc" "s1_evict" "s1_count" "s1_structure"

permutation "s1_count" "s1_delete500_begin" "s1_evict" "s1_count" "s1_delete500_end" "s1_count"
permutation "s1_count" "s1_delete_80_perc" "s1_evict" "s1_count"
permutation "s2_count" "s1_delete500_begin" "s1_evict" "s2_count" "s1_delete500_end" "s1_evict" "s2_count"
permutation "s2_count" "s1_delete_80_perc" "s1_evict" "s2_count"

permutation "s1_bcount" "s1_delete500_begin" "s1_evict" "s1_bcount" "s1_delete500_end" "s1_evict" "s1_bcount"
permutation "s1_bcount" "s1_delete_80_perc" "s1_evict" "s1_bcount"
permutation "s2_bcount" "s1_delete500_begin" "s1_evict" "s2_bcount" "s1_delete500_end" "s1_evict" "s2_bcount"
permutation "s2_bcount" "s1_delete_80_perc" "s1_evict" "s2_bcount"

permutation "s1_begin" "s2_begin" "s1_count" "s1_delete500_begin" "s1_count" "s1_delete500_end" "s1_count" "s1_commit" "s1_evict" "s1_count" "s1_structure" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_count" "s1_delete_80_perc" "s1_count" "s1_commit" "s1_evict" "s1_count" "s1_structure" "s2_commit"
permutation "s1_begin" "s2_begin" "s2_count" "s1_delete500_begin" "s2_count" "s1_delete500_end" "s2_count" "s1_commit" "s2_evict" "s2_count" "s2_structure" "s2_commit"
permutation "s1_begin" "s2_begin" "s2_count" "s1_delete_80_perc" "s2_count" "s1_commit" "s2_evict" "s2_count" "s2_structure" "s2_commit"

permutation "s1_begin" "s2_begin" "s1_bcount" "s1_delete500_begin" "s1_bcount" "s1_delete500_end" "s1_bcount" "s1_commit" "s1_evict" "s1_bcount" "s1_structure" "s2_commit"
permutation "s1_begin" "s2_begin" "s1_bcount" "s1_delete_80_perc" "s1_bcount" "s1_commit" "s1_evict" "s1_bcount" "s1_structure" "s2_commit"
permutation "s1_begin" "s2_begin" "s2_bcount" "s1_delete500_begin" "s2_bcount" "s1_delete500_end" "s2_bcount" "s1_commit" "s2_evict" "s2_bcount" "s2_structure" "s2_commit"
permutation "s1_begin" "s2_begin" "s2_bcount" "s1_delete_80_perc" "s2_bcount" "s1_commit" "s2_evict" "s2_bcount" "s2_structure" "s2_commit"
