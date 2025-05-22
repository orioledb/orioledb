CREATE SCHEMA rewind;
SET SESSION search_path = 'rewind';
CREATE EXTENSION orioledb;
\set VERBOSITY terse
\set VERBOSITY default

CREATE TABLE o_test_rewind (i int) USING orioledb;

INSERT INTO o_test_rewind VALUES (1);
SELECT pg_sleep(1);
INSERT INTO o_test_rewind VALUES (2);
SELECT pg_sleep(1);
INSERT INTO o_test_rewind VALUES (3);
SELECT pg_sleep(1);
INSERT INTO o_test_rewind VALUES (4);
SELECT pg_sleep(1);
INSERT INTO o_test_rewind VALUES (5);
SELECT pg_sleep(5);

select * from o_test_rewind;
select orioledb_rewind(600);
select orioledb_rewind(0);
select * from o_test_rewind;
select orioledb_rewind(10);
select * from o_test_rewind;

DROP TABLE o_test_rewind;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA rewind CASCADE;
RESET search_path;
