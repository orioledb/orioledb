CREATE SCHEMA stats;
SET SESSION search_path = 'stats';
CREATE EXTENSION orioledb;

-- record dboid for later use
SELECT oid AS dboid from pg_database where datname = current_database() \gset

-- test effects of TRUNCATE on n_live_tup/n_dead_tup counters
CREATE TABLE trunc_stats_test(id serial) USING orioledb;
CREATE TABLE trunc_stats_test1(id serial, stuff text) USING orioledb;
CREATE TABLE trunc_stats_test2(id serial) USING orioledb;
CREATE TABLE trunc_stats_test3(id serial, stuff text) USING orioledb;
CREATE TABLE trunc_stats_test4(id serial) USING orioledb;

-- check that n_live_tup is reset to 0 after truncate
INSERT INTO trunc_stats_test DEFAULT VALUES;
INSERT INTO trunc_stats_test DEFAULT VALUES;
INSERT INTO trunc_stats_test DEFAULT VALUES;
TRUNCATE trunc_stats_test;

-- test involving a truncate in a transaction; 4 ins but only 1 live
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
UPDATE trunc_stats_test1 SET id = id + 10 WHERE id IN (1, 2);
DELETE FROM trunc_stats_test1 WHERE id = 3;

BEGIN;
UPDATE trunc_stats_test1 SET id = id + 100;
TRUNCATE trunc_stats_test1;
INSERT INTO trunc_stats_test1 DEFAULT VALUES;
COMMIT;

-- use a savepoint: 1 insert, 1 live
BEGIN;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
SAVEPOINT p1;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
TRUNCATE trunc_stats_test2;
INSERT INTO trunc_stats_test2 DEFAULT VALUES;
RELEASE SAVEPOINT p1;
COMMIT;

-- rollback a savepoint: this should count 4 inserts and have 2
-- live tuples after commit (and 2 dead ones due to aborted subxact)
BEGIN;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
SAVEPOINT p1;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
TRUNCATE trunc_stats_test3;
INSERT INTO trunc_stats_test3 DEFAULT VALUES;
ROLLBACK TO SAVEPOINT p1;
COMMIT;

-- rollback a truncate: this should count 2 inserts and produce 2 dead tuples
BEGIN;
INSERT INTO trunc_stats_test4 DEFAULT VALUES;
INSERT INTO trunc_stats_test4 DEFAULT VALUES;
TRUNCATE trunc_stats_test4;
INSERT INTO trunc_stats_test4 DEFAULT VALUES;
ROLLBACK;

-- ensure pending stats are flushed
SELECT pg_stat_force_next_flush();

-- check effects
BEGIN;
SET LOCAL stats_fetch_consistency = snapshot;

SELECT relname, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup
  FROM pg_stat_user_tables
 WHERE relname like 'trunc_stats_test%' order by relname;
COMMIT;

-- Check that stats for relations are dropped. For that we need to access stats
-- by oid after the DROP TABLE. Save oids.
CREATE TABLE drop_stats_test() USING orioledb;
INSERT INTO drop_stats_test DEFAULT VALUES;
SELECT 'drop_stats_test'::regclass::oid AS drop_stats_test_oid \gset

CREATE TABLE drop_stats_test_xact() USING orioledb;
INSERT INTO drop_stats_test_xact DEFAULT VALUES;
SELECT 'drop_stats_test_xact'::regclass::oid AS drop_stats_test_xact_oid \gset

CREATE TABLE drop_stats_test_subxact() USING orioledb;
INSERT INTO drop_stats_test_subxact DEFAULT VALUES;
SELECT 'drop_stats_test_subxact'::regclass::oid AS drop_stats_test_subxact_oid \gset

SELECT pg_stat_force_next_flush();

SELECT pg_stat_get_live_tuples(:drop_stats_test_oid);
DROP TABLE drop_stats_test;
SELECT pg_stat_get_live_tuples(:drop_stats_test_oid);
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_oid);

-- check that rollback protects against having stats dropped and that local
-- modifications don't pose a problem
SELECT pg_stat_get_live_tuples(:drop_stats_test_xact_oid);
SELECT pg_stat_get_tuples_inserted(:drop_stats_test_xact_oid);
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_xact_oid);
BEGIN;
INSERT INTO drop_stats_test_xact DEFAULT VALUES;
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_xact_oid);
DROP TABLE drop_stats_test_xact;
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_xact_oid);
ROLLBACK;
SELECT pg_stat_force_next_flush();
SELECT pg_stat_get_live_tuples(:drop_stats_test_xact_oid);
SELECT pg_stat_get_tuples_inserted(:drop_stats_test_xact_oid);

-- transactional drop
SELECT pg_stat_get_live_tuples(:drop_stats_test_xact_oid);
SELECT pg_stat_get_tuples_inserted(:drop_stats_test_xact_oid);
BEGIN;
INSERT INTO drop_stats_test_xact DEFAULT VALUES;
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_xact_oid);
DROP TABLE drop_stats_test_xact;
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_xact_oid);
COMMIT;
SELECT pg_stat_force_next_flush();
SELECT pg_stat_get_live_tuples(:drop_stats_test_xact_oid);
SELECT pg_stat_get_tuples_inserted(:drop_stats_test_xact_oid);

-- savepoint rollback (2 levels)
SELECT pg_stat_get_live_tuples(:drop_stats_test_subxact_oid);
BEGIN;
INSERT INTO drop_stats_test_subxact DEFAULT VALUES;
SAVEPOINT sp1;
INSERT INTO drop_stats_test_subxact DEFAULT VALUES;
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_subxact_oid);
SAVEPOINT sp2;
DROP TABLE drop_stats_test_subxact;
ROLLBACK TO SAVEPOINT sp2;
SELECT pg_stat_get_xact_tuples_inserted(:drop_stats_test_subxact_oid);
COMMIT;
SELECT pg_stat_force_next_flush();
SELECT pg_stat_get_live_tuples(:drop_stats_test_subxact_oid);

-- savepoint rolback (1 level)
SELECT pg_stat_get_live_tuples(:drop_stats_test_subxact_oid);
BEGIN;
SAVEPOINT sp1;
DROP TABLE drop_stats_test_subxact;
SAVEPOINT sp2;
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
SELECT pg_stat_get_live_tuples(:drop_stats_test_subxact_oid);

-- and now actually drop
SELECT pg_stat_get_live_tuples(:drop_stats_test_subxact_oid);
BEGIN;
SAVEPOINT sp1;
DROP TABLE drop_stats_test_subxact;
SAVEPOINT sp2;
RELEASE SAVEPOINT sp1;
COMMIT;
SELECT pg_stat_get_live_tuples(:drop_stats_test_subxact_oid);

DROP TABLE trunc_stats_test, trunc_stats_test1, trunc_stats_test2, trunc_stats_test3, trunc_stats_test4;


-----
-- Test that last_seq_scan, last_idx_scan are correctly maintained
--
-- Perform test using a temporary table. That way autovacuum etc won't
-- interfere. To be able to check that timestamps increase, we sleep for 100ms
-- between tests, assuming that there aren't systems with a coarser timestamp
-- granularity.
-----

BEGIN;
CREATE TEMPORARY TABLE test_last_scan(idx_col int primary key, noidx_col int) USING orioledb;
INSERT INTO test_last_scan(idx_col, noidx_col) VALUES(1, 1);
SELECT pg_stat_force_next_flush();
SELECT last_seq_scan, last_idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;
COMMIT;

SELECT pg_stat_reset_single_table_counters('test_last_scan'::regclass);
SELECT seq_scan, idx_scan FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;

-- ensure we start out with exactly one index and sequential scan
BEGIN;
SET LOCAL enable_seqscan TO on;
SET LOCAL enable_indexscan TO on;
SET LOCAL enable_bitmapscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SET LOCAL enable_seqscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT pg_stat_force_next_flush();
COMMIT;

-- fetch timestamps from before the next test
SELECT last_seq_scan AS test_last_seq, last_idx_scan AS test_last_idx
FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass \gset
SELECT pg_sleep(0.1); -- assume a minimum timestamp granularity of 100ms

-- cause one sequential scan
BEGIN;
SET LOCAL enable_seqscan TO on;
SET LOCAL enable_indexscan TO off;
SET LOCAL enable_bitmapscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SELECT count(*) FROM test_last_scan WHERE noidx_col = 1;
SELECT pg_stat_force_next_flush();
COMMIT;
-- check that just sequential scan stats were incremented
SELECT seq_scan, :'test_last_seq' < last_seq_scan AS seq_ok, idx_scan, :'test_last_idx' = last_idx_scan AS idx_ok
FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;

-- fetch timestamps from before the next test
SELECT last_seq_scan AS test_last_seq, last_idx_scan AS test_last_idx
FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass \gset
SELECT pg_sleep(0.1);

-- cause one index scan
BEGIN;
SET LOCAL enable_seqscan TO off;
SET LOCAL enable_indexscan TO on;
SET LOCAL enable_bitmapscan TO off;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT pg_stat_force_next_flush();
COMMIT;
-- check that just index scan stats were incremented
SELECT seq_scan, :'test_last_seq' = last_seq_scan AS seq_ok, idx_scan, :'test_last_idx' < last_idx_scan AS idx_ok
FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;

-- fetch timestamps from before the next test
SELECT last_seq_scan AS test_last_seq, last_idx_scan AS test_last_idx
FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass \gset
SELECT pg_sleep(0.1);

-- cause one bitmap index scan
BEGIN;
SET LOCAL enable_seqscan TO off;
SET LOCAL enable_indexscan TO off;
SET LOCAL enable_bitmapscan TO on;
EXPLAIN (COSTS off) SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT count(*) FROM test_last_scan WHERE idx_col = 1;
SELECT pg_stat_force_next_flush();
COMMIT;
-- check that just index scan stats were incremented
SELECT seq_scan, :'test_last_seq' = last_seq_scan AS seq_ok, idx_scan, :'test_last_idx' < last_idx_scan AS idx_ok
FROM pg_stat_all_tables WHERE relid = 'test_last_scan'::regclass;


-- pg_stat_have_stats returns true for committed index creation
CREATE table stats_test_tab1 USING orioledb as select generate_series(1,10) a;
CREATE index stats_test_idx1 on stats_test_tab1(a);
SELECT 'stats_test_idx1'::regclass::oid AS stats_test_idx1_oid \gset
SET enable_seqscan TO off;
select a from stats_test_tab1 where a = 3;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);

-- pg_stat_have_stats returns false for dropped index with stats
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);
DROP index stats_test_idx1;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);

-- pg_stat_have_stats returns false for rolled back index creation
BEGIN;
CREATE index stats_test_idx1 on stats_test_tab1(a);
SELECT 'stats_test_idx1'::regclass::oid AS stats_test_idx1_oid \gset
select a from stats_test_tab1 where a = 3;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);
ROLLBACK;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);

-- pg_stat_have_stats returns true for reindex CONCURRENTLY
CREATE index stats_test_idx1 on stats_test_tab1(a);
SELECT 'stats_test_idx1'::regclass::oid AS stats_test_idx1_oid \gset
select a from stats_test_tab1 where a = 3;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);
REINDEX index CONCURRENTLY stats_test_idx1;
-- true since REINDEX INDEX CONCURRENTLY was changed to plain REINDEX
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);
-- true for new oid
SELECT 'stats_test_idx1'::regclass::oid AS stats_test_idx1_oid \gset
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);

-- pg_stat_have_stats returns true for a rolled back drop index with stats
BEGIN;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);
DROP index stats_test_idx1;
ROLLBACK;
SELECT pg_stat_have_stats('relation', :dboid, :stats_test_idx1_oid);

DROP TABLE stats_test_tab1;

-- put enable_seqscan back to on
SET enable_seqscan TO on;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA opclass CASCADE;
RESET search_path;
