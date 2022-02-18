setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_rightlink (
		id text NOT NULL,
		PRIMARY KEY(id)
	) USING orioledb;
	TRUNCATE o_rightlink;
}

teardown
{
	DROP TABLE o_rightlink;
}

session "s1"

step "s1_setup"  {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's1'; }

step "s1_split_leaf_prepare" { INSERT INTO o_rightlink (SELECT repeat('x', 2600) || id FROM generate_series(10, 26, 4) id); }
step "s1_split_leaf" { INSERT INTO o_rightlink (SELECT repeat('x', 2600) || id FROM generate_series(27, 27, 1) id); }
step "s1_split_leaf_v2" { INSERT INTO o_rightlink (SELECT repeat('x', 250) || id FROM generate_series(11, 11, 1) id); }

step "s1_split_node_prepare" { INSERT INTO o_rightlink (SELECT repeat('x', 2600) || id FROM generate_series(10, 66, 4) id); }
step "s1_split_node" { INSERT INTO o_rightlink (SELECT repeat('x', 2600) || id FROM generate_series(52, 52, 1) id); }
step "s1_split_node_v2" { INSERT INTO o_rightlink (SELECT repeat('x', 2600) || id FROM generate_series(31, 31, 1) id); }

session "s2"

step "s2_setup" {
	SET orioledb.enable_stopevents = true;
	SET application_name = 's2';
	SET enable_seqscan = off; }
step "s2_setup_seq" {
	SET orioledb.enable_stopevents = true; }

step "s2_bp_split_leaf" { SELECT pg_stopevent_set('page_split', '$.level == 0'); }
step "s2_bp_split_node" { SELECT pg_stopevent_set('page_split', '$.level > 0'); }

step "s2_select" { SELECT * FROM o_rightlink; }
step "s2_bselect" { SELECT * FROM o_rightlink ORDER BY id DESC; }
step "s2_select_struct" { SELECT orioledb_idx_structure('o_rightlink'::regclass,
														'o_rightlink_pkey',
														'nue'); }
step "s2_reset_s1" { SELECT pg_stopevent_reset('page_split'); }

session "s3"

step "s3_bp_split_node" {
	SELECT pg_stopevent_set('page_split',
							'$.level > 0 && $applicationName == "s1"'); }
step "s3_bp_down_rightlink"
{
	SELECT
		pg_stopevent_set('step_down',
					     format('$.downlink.blkno == %s && $applicationName == "s2"',
								(SELECT blkno
								 FROM orioledb_table_pages('o_rightlink'::regclass)
					 			 WHERE rightlink IS NOT NULL))::jsonpath);
}
step "s3_bp_down" {
	SELECT pg_stopevent_set('step_down',
							'$applicationName == "s2"'); }
step "s3_reset_split_node" { SELECT pg_stopevent_reset('page_split'); }
step "s3_reset_down" { SELECT pg_stopevent_reset('step_down'); }

###
# rightlinks on leafs test
###

#      (root) (downlinks on page1 and page2)
# (page1)  (page2) -> (page3)
# forward
permutation "s1_setup" "s2_setup" "s1_split_leaf_prepare" "s2_bp_split_leaf" "s1_split_leaf" "s2_select" "s2_select_struct" "s2_reset_s1"
# backward
permutation "s1_setup" "s2_setup" "s1_split_leaf_prepare" "s2_bp_split_leaf" "s1_split_leaf" "s2_bselect" "s2_select_struct" "s2_reset_s1"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_leaf_prepare" "s2_bp_split_leaf" "s1_split_leaf" "s2_select" "s2_select_struct" "s2_reset_s1"

#          (root) (downlinks on page2 and page1)
# (page2) -> (page3) (page1)
# forward
permutation "s1_setup" "s2_setup" "s1_split_leaf_prepare" "s2_bp_split_leaf" "s1_split_leaf_v2" "s2_select" "s2_select_struct" "s2_reset_s1"
# backward
permutation "s1_setup" "s2_setup" "s1_split_leaf_prepare" "s2_bp_split_leaf" "s1_split_leaf_v2" "s2_bselect" "s2_select_struct" "s2_reset_s1"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_leaf_prepare" "s2_bp_split_leaf" "s1_split_leaf_v2" "s2_select" "s2_select_struct" "s2_reset_s1"

###
# rightlinks on nodes test
###

#       (root) (downlinks on page1 and page2)
# (page1)  (page2) -> (page3)                    # nodes
# ()()()() ()()()()   ()()()() -> ()             # leafs
# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s2_bp_split_node" "s1_split_node" "s2_select" "s2_select_struct" "s2_reset_s1"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s2_bp_split_node" "s1_split_node" "s2_bselect" "s2_select_struct" "s2_reset_s1"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s2_bp_split_node" "s1_split_node" "s2_select" "s2_select_struct" "s2_reset_s1"

#       (root) (downlinks on page2 and page1)
# (page2) -> (page3)      (page1)                # nodes
# ()()()()   ()()()->()   ()()()()               # leafs
# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s2_bp_split_node" "s1_split_node_v2" "s2_select" "s2_select_struct" "s2_reset_s1"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s2_bp_split_node" "s1_split_node_v2" "s2_bselect" "s2_select_struct" "s2_reset_s1"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s2_bp_split_node" "s1_split_node_v2" "s2_select" "s2_select_struct" "s2_reset_s1"

###
# rightlink has been removed when other process executes find_page at 0 lvl page test
# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

###
# rightlink has been removed when other process executes find_page at 1 lvl page test
###
# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

###
# rightlink has been removed on node mix (it does not test something meaningful cases but can be useful on errors catch)
###
# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_bselect" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_node_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

###
# rightlink has been removed on leaf mix (it does not test something meaningful cases but can be useful on errors catch)
###
# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_bselect" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_bselect" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"

# forward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# backward
permutation "s1_setup" "s2_setup" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_bselect" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
# sequential
permutation "s1_setup" "s2_setup_seq" "s1_split_node_prepare" "s3_bp_split_node" "s1_split_leaf_v2" "s3_bp_down_rightlink" "s2_select" "s3_bp_down" "s3_reset_split_node" "s3_reset_down"
