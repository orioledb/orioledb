-- print evicted tree
CREATE EXTENSION orioledb;

CREATE TABLE o_print_evicted (
       id integer NOT NULL,
       val text NOT NULL
) USING orioledb;
SELECT orioledb_tbl_structure('o_print_evicted'::regclass, 'e');

-- orioledb_tbl_structure options checked
CREATE TABLE o_test1 (
       id integer NOT NULL,
       val text NOT NULL
) USING orioledb;

INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(1, 10, 1) id);
SELECT orioledb_tbl_structure('o_test1'::regclass, 'ne');
SELECT orioledb_tbl_structure('o_test1'::regclass, 'ne');
UPDATE o_test1 SET val = 'xxx1' WHERE id BETWEEN 5 AND 10;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');
DELETE FROM o_test1 WHERE id BETWEEN 2 AND 4;
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(-1000, 0, 1) id);
INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(1001, 2000, 1) id);
INSERT INTO o_test1 (SELECT id, id || 'val' FROM generate_series(11, 1000, 1) id);
SELECT orioledb_tbl_structure('o_test1'::regclass, 'nue');

DROP EXTENSION orioledb CASCADE;
