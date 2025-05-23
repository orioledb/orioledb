CREATE SCHEMA rewind;
SET SESSION search_path = 'rewind';
CREATE EXTENSION orioledb;
\set VERBOSITY terse
\set VERBOSITY default

CREATE table heap_table (i int) USING heap;
CREATE TABLE o_test_rewind (i int) USING orioledb;

INSERT INTO heap_table VALUES (1);

INSERT INTO o_test_rewind VALUES (1);
INSERT INTO o_test_rewind VALUES (2);
INSERT INTO o_test_rewind VALUES (3);
INSERT INTO o_test_rewind VALUES (4);
INSERT INTO o_test_rewind VALUES (5);

-- Should fail
select orioledb_rewind(600);
select orioledb_rewind(0);

BEGIN;
INSERT INTO heap_table VALUES (2);
PREPARE TRANSACTION 'prep';
ROLLBACK;

select orioledb_rewind(10);

COMMIT PREPARED 'prep';

DROP TABLE heap_table;
DROP TABLE o_test_rewind;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA rewind CASCADE;
RESET search_path;
