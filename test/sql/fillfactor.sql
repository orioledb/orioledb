CREATE SCHEMA fillfactor;
SET SESSION search_path = 'fillfactor';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_fillfactor
(
	f1 text,
	f2 varchar,
	f3 integer,
	f4 integer,
	f5 integer,
	f6 integer,
	PRIMARY KEY(f1)
) USING orioledb WITH (fillfactor = 10);

CREATE INDEX o_test_fillfactor_ix1 ON o_test_fillfactor(f3) WITH (fillfactor = 10);
CREATE INDEX o_test_fillfactor_ix2 ON o_test_fillfactor(f4) WITH (fillfactor = 40);
CREATE INDEX o_test_fillfactor_ix3 ON o_test_fillfactor(f5) WITH (fillfactor = 60);
CREATE INDEX o_test_fillfactor_ix4 ON o_test_fillfactor(f6);
\d+ o_test_fillfactor

INSERT INTO o_test_fillfactor SELECT (10000-v+7)%3000, 10-v, (v+5)%7, (v+15)%7, (v+77)%7, (v-177)%7
    FROM generate_series(1, 1000) v;
SELECT index, level, real_fillfactor
    FROM orioledb_table_pages('o_test_fillfactor'::regclass)
        WHERE index = 'o_test_fillfactor_pkey';
SELECT index, level, real_fillfactor
    FROM orioledb_table_pages('o_test_fillfactor'::regclass)
        WHERE index = 'o_test_fillfactor_ix1';
SELECT index, level, real_fillfactor
    FROM orioledb_table_pages('o_test_fillfactor'::regclass)
        WHERE index = 'o_test_fillfactor_ix2';
SELECT index, level, real_fillfactor
    FROM orioledb_table_pages('o_test_fillfactor'::regclass)
        WHERE index = 'o_test_fillfactor_ix3';
SELECT index, level, real_fillfactor
    FROM orioledb_table_pages('o_test_fillfactor'::regclass)
        WHERE index = 'o_test_fillfactor_ix4';

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA fillfactor CASCADE;
RESET search_path;
