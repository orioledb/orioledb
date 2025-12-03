-- Test bridge index behavior when table is truncated.

CREATE EXTENSION orioledb;

CREATE TABLE brin_test_multi (a INT, b BIGINT) using orioledb WITH (fillfactor=10) ;
INSERT INTO brin_test_multi
SELECT i/5 + mod(911 * i + 483, 25),
       i/10 + mod(751 * i + 221, 41)
  FROM generate_series(1,1000) s(i);

SELECT COUNT(*) FROM brin_test_multi WHERE a < 37;

CREATE INDEX brin_test_multi_idx ON brin_test_multi USING brin (a int4_minmax_multi_ops) WITH (pages_per_range=5);

SELECT orioledb_tbl_indices('brin_test_multi'::regclass, true);
\d+ brin_test_multi

TRUNCATE brin_test_multi;

SELECT orioledb_tbl_indices('brin_test_multi'::regclass, true);
\d+ brin_test_multi

INSERT INTO brin_test_multi
SELECT i/5 + mod(911 * i + 483, 25),
       i/10 + mod(751 * i + 221, 41)
  FROM generate_series(1,1000) s(i);

set enable_seqscan = off;
EXPLAIN SELECT COUNT(*) FROM brin_test_multi WHERE a < 37;
SELECT COUNT(*) FROM brin_test_multi WHERE a < 37;


DROP TABLE brin_test_multi;
DROP EXTENSION orioledb;
RESET enable_seqscan;
