-- test with ctid
CREATE DATABASE reindex_concurrently_test_db;
\c reindex_concurrently_test_db

CREATE SCHEMA orioledb;
CREATE EXTENSION orioledb SCHEMA orioledb;
create table t_reindex
(
    id int,
    someint int,
    balance int
) using orioledb;

CREATE TABLE test_step (
    test_step int
);

CREATE TABLE done_items (
    done_items int
);

INSERT INTO t_reindex SELECT t, 99998 - t, 0 FROM generate_series(1,99999) as t;

create index CONCURRENTLY t_reindex_idx on t_reindex(someint);

SET enable_seqscan = off;
EXPLAIN SELECT COUNT(*) FROM t_reindex;
SELECT COUNT(*) FROM t_reindex;
SET enable_seqscan = on;

\! (i=1; while [ "$(psql -t -c "SELECT COUNT(1) FROM test_step WHERE test_step = 1" reindex_concurrently_test_db | xargs )" -eq 0 ]; do psql -q -c "UPDATE t_reindex SET balance = balance + 1 WHERE someint = (($i * 1753) % 99995)::int + 1; INSERT INTO done_items VALUES (1);" reindex_concurrently_test_db ; i=$((i+1)); done ;  psql -q -c "INSERT INTO test_step VALUES (2);" reindex_concurrently_test_db ;) &

SELECT pg_sleep(5);

REINDEX INDEX CONCURRENTLY t_reindex_idx;

INSERT INTO test_step VALUES (1);

-- wait until 2 appears in test_step
DO $$
DECLARE
    v int;
BEGIN
    LOOP
        SELECT test_step INTO v FROM test_step WHERE test_step = 2;
        IF v IS NOT NULL THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END $$;

SELECT pg_sleep(2);

-- check that after reindex is done, balance was incremented with loop count
SELECT CASE 
         WHEN a.sum_balance = b.sum_done 
         THEN 1 
         ELSE 0 
       END 
FROM 
  (SELECT SUM(balance) AS sum_balance FROM t_reindex) a,
  (SELECT SUM(done_items) AS sum_done FROM done_items) b;


TRUNCATE test_step;
TRUNCATE done_items;

UPDATE t_reindex SET balance = 0;

\! (i=1; while [ "$(psql -t -c "SELECT COUNT(1) FROM test_step WHERE test_step = 1" reindex_concurrently_test_db | xargs )" -eq 0 ]; do psql -q -c "INSERT INTO t_reindex VALUES ($i+100000000, $i, 1); INSERT INTO done_items VALUES (1);" reindex_concurrently_test_db ; i=$((i+1)); done ;  psql -q -c "INSERT INTO test_step VALUES (2);" reindex_concurrently_test_db ;) &

SELECT pg_sleep(5);

REINDEX INDEX CONCURRENTLY t_reindex_idx;

INSERT INTO test_step VALUES (1);

-- wait until 2 appears in test_step
DO $$
DECLARE
    v int;
BEGIN
    LOOP
        SELECT test_step INTO v FROM test_step WHERE test_step = 2;
        IF v IS NOT NULL THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END $$;

SELECT pg_sleep(2);

-- check that after reindex is done, balance was incremented with loop count
SELECT CASE 
         WHEN a.sum_balance = b.sum_done 
         THEN 1 
         ELSE 0 
       END 
FROM 
  (SELECT SUM(balance) AS sum_balance FROM t_reindex) a,
  (SELECT SUM(done_items) AS sum_done FROM done_items) b;


\c regression
DROP DATABASE reindex_concurrently_test_db;

-- test with pk
CREATE DATABASE reindex_concurrently_test_db;
\c reindex_concurrently_test_db

CREATE SCHEMA orioledb;
CREATE EXTENSION orioledb SCHEMA orioledb;
create table t_reindex
(
    id int PRIMARY KEY,
    someint int,
    balance int
) using orioledb;

CREATE TABLE test_step (
    test_step int
);

CREATE TABLE done_items (
    done_items int
);

INSERT INTO t_reindex SELECT t, 99998 - t, 0 FROM generate_series(1,99999) as t;

create index CONCURRENTLY t_reindex_idx on t_reindex(someint);

SET enable_seqscan = off;
EXPLAIN SELECT COUNT(*) FROM t_reindex;
SELECT COUNT(*) FROM t_reindex;
SET enable_seqscan = on;

\! (i=1; while [ "$(psql -t -c "SELECT COUNT(1) FROM test_step WHERE test_step = 1" reindex_concurrently_test_db | xargs )" -eq 0 ]; do psql -q -c "UPDATE t_reindex SET balance = balance + 1 WHERE someint = (($i * 1753) % 99995)::int + 1; INSERT INTO done_items VALUES (1);" reindex_concurrently_test_db ; i=$((i+1)); done ;  psql -q -c "INSERT INTO test_step VALUES (2);" reindex_concurrently_test_db ;) &

SELECT pg_sleep(5);

REINDEX INDEX CONCURRENTLY t_reindex_idx;

INSERT INTO test_step VALUES (1);

-- wait until 2 appears in test_step
DO $$
DECLARE
    v int;
BEGIN
    LOOP
        SELECT test_step INTO v FROM test_step WHERE test_step = 2;
        IF v IS NOT NULL THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END $$;

SELECT pg_sleep(2);

-- check that after reindex is done, balance was incremented with loop count
SELECT CASE 
         WHEN a.sum_balance = b.sum_done 
         THEN 1 
         ELSE 0 
       END 
FROM 
  (SELECT SUM(balance) AS sum_balance FROM t_reindex) a,
  (SELECT SUM(done_items) AS sum_done FROM done_items) b;


TRUNCATE test_step;
TRUNCATE done_items;

UPDATE t_reindex SET balance = 0;

\! (i=1; while [ "$(psql -t -c "SELECT COUNT(1) FROM test_step WHERE test_step = 1" reindex_concurrently_test_db | xargs )" -eq 0 ]; do psql -q -c "UPDATE t_reindex SET id = id + 100000000, balance = balance + 1 WHERE someint = (($i * 1753) % 99995)::int + 1; INSERT INTO done_items VALUES (1);" reindex_concurrently_test_db ; i=$((i+1)); done ;  psql -q -c "INSERT INTO test_step VALUES (2);" reindex_concurrently_test_db ;) &

SELECT pg_sleep(5);

REINDEX INDEX CONCURRENTLY t_reindex_idx;

INSERT INTO test_step VALUES (1);

-- wait until 2 appears in test_step
DO $$
DECLARE
    v int;
BEGIN
    LOOP
        SELECT test_step INTO v FROM test_step WHERE test_step = 2;
        IF v IS NOT NULL THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END $$;

SELECT pg_sleep(2);

-- check that after reindex is done, balance was incremented with loop count
SELECT CASE 
         WHEN a.sum_balance = b.sum_done 
         THEN 1 
         ELSE 0 
       END 
FROM 
  (SELECT SUM(balance) AS sum_balance FROM t_reindex) a,
  (SELECT SUM(done_items) AS sum_done FROM done_items) b;


TRUNCATE test_step;
TRUNCATE done_items;

UPDATE t_reindex SET balance = 0;

\! (i=1; while [ "$(psql -t -c "SELECT COUNT(1) FROM test_step WHERE test_step = 1" reindex_concurrently_test_db | xargs )" -eq 0 ]; do psql -q -c "UPDATE t_reindex SET someint = someint + 100000000, balance = balance + 1 WHERE someint = (($i * 1753) % 99995)::int + 1; INSERT INTO done_items VALUES (1);" reindex_concurrently_test_db ; i=$((i+1)); done ;  psql -q -c "INSERT INTO test_step VALUES (2);" reindex_concurrently_test_db ;) &

SELECT pg_sleep(5);

REINDEX INDEX CONCURRENTLY t_reindex_idx;
\d+ t_reindex;

INSERT INTO test_step VALUES (1);

-- wait until 2 appears in test_step
DO $$
DECLARE
    v int;
BEGIN
    LOOP
        SELECT test_step INTO v FROM test_step WHERE test_step = 2;
        IF v IS NOT NULL THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END $$;

SELECT pg_sleep(2);

-- check that after reindex is done, balance was incremented with loop count
SELECT CASE 
         WHEN a.sum_balance = b.sum_done 
         THEN 1 
         ELSE 0 
       END 
FROM 
  (SELECT SUM(balance) AS sum_balance FROM t_reindex) a,
  (SELECT SUM(done_items) AS sum_done FROM done_items) b;


\c regression
DROP DATABASE reindex_concurrently_test_db;


