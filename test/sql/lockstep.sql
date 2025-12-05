--
-- Test orioledb indexes:
--	lockstep scanning with IN clauses
--

CREATE SCHEMA lockstep;
SET SESSION search_path = 'lockstep';
CREATE EXTENSION orioledb;
SELECT orioledb_parallel_debug_start();

CREATE TABLE o_test_lockstep
(
	a int8 NOT NULL,
	b int8 NOT NULL,
	PRIMARY KEY(a,b)
) USING orioledb;

INSERT INTO o_test_lockstep SELECT t, t FROM generate_series(1, 1000) t;

SELECT COUNT(*) FROM o_test_lockstep WHERE a in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
SELECT COUNT(*) FROM o_test_lockstep WHERE a in (1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17);
SELECT COUNT(*) FROM o_test_lockstep WHERE a in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) AND 
										   b in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
SELECT COUNT(*) FROM o_test_lockstep WHERE a in (1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17) AND
										   b in (1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17);

DROP TABLE o_test_lockstep;

SELECT orioledb_parallel_debug_stop();
DROP EXTENSION orioledb CASCADE;
DROP SCHEMA lockstep CASCADE;
RESET search_path;
