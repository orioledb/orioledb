CREATE SCHEMA index_bridging;
SET SESSION search_path = 'index_bridging';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_ix_ams (
	i int NOT NULL,
	j int4[],
	p point,
	pk1 int,
	pk2 int
) USING orioledb;
-- ) USING orioledb WITH (index_bridging);
INSERT INTO o_test_ix_ams VALUES (1, ARRAY[2,3], point(4, 5), 6, 7);
CREATE INDEX o_test_ix_ams_ix1 on o_test_ix_ams using btree (j);
-- CREATE INDEX o_test_ix_ams_ix2 ON o_test_ix_ams USING hash (j);
-- CREATE INDEX o_test_ix_ams_ix3 ON o_test_ix_ams USING gin (j);
-- CREATE INDEX o_test_ix_ams_ix4 ON o_test_ix_ams USING gist (p);
\d o_test_ix_ams
SELECT orioledb_table_description('o_test_ix_ams'::regclass);
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);
ALTER TABLE o_test_ix_ams ADD PRIMARY KEY (pk2, pk1);
SELECT orioledb_tbl_indices('o_test_ix_ams'::regclass, true);

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA index_bridging CASCADE;
RESET search_path;
