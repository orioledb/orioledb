CREATE SCHEMA truncate;
SET SESSION search_path = 'truncate';
CREATE EXTENSION orioledb;

CREATE VIEW o_relnames AS
    SELECT c.relname FROM (
        SELECT reloid, relnode FROM orioledb_table_oids()
        UNION
        SELECT index_reloid, index_relnode FROM orioledb_index_oids()) ot
        JOIN pg_class c ON c.oid = ot.reloid
        WHERE c.relfilenode = ot.relnode
        ORDER BY relname;

CREATE TABLE o_non_transactional_truncate (
  a int UNIQUE,
  b char
) USING orioledb;

BEGIN;
SELECT * FROM o_relnames WHERE relname LIKE 'o_non_transactional_truncate%';
INSERT INTO o_non_transactional_truncate VALUES (3, 'b');
TRUNCATE o_non_transactional_truncate;
SELECT * FROM o_relnames WHERE relname LIKE 'o_non_transactional_truncate%';
COMMIT;

BEGIN;

CREATE TABLE o_transactional_truncate (
  a int UNIQUE,
  b char
) USING orioledb;
SELECT * FROM o_relnames WHERE relname LIKE 'o_transactional_truncate%';
INSERT INTO o_transactional_truncate VALUES (3, 'b');
TRUNCATE o_transactional_truncate;
SELECT * FROM o_relnames WHERE relname LIKE 'o_transactional_truncate%';
COMMIT;

CREATE TABLE o_non_transactional_truncate_pkey (
  a int PRIMARY KEY,
  b char UNIQUE
) USING orioledb;

BEGIN;
SELECT * FROM o_relnames WHERE relname LIKE 'o_non_transactional_truncate_pkey%';
INSERT INTO o_non_transactional_truncate VALUES (3, 'b');
TRUNCATE o_non_transactional_truncate;
SELECT * FROM o_relnames WHERE relname LIKE 'o_non_transactional_truncate_pkey%';
COMMIT;

BEGIN;

CREATE TABLE o_transactional_truncate_pkey (
  a int PRIMARY KEY,
  b char UNIQUE
) USING orioledb;
SELECT * FROM o_relnames WHERE relname LIKE 'o_transactional_truncate_pkey%';
INSERT INTO o_transactional_truncate_pkey VALUES (3, 'b');
TRUNCATE o_transactional_truncate_pkey;
SELECT * FROM o_relnames WHERE relname LIKE 'o_transactional_truncate_pkey%';
COMMIT;

CREATE TABLE o_compress_truncate (
  id int
) USING orioledb WITH (compress = 2);

SELECT orioledb_table_description('o_compress_truncate'::regclass);
TRUNCATE o_compress_truncate;
SELECT orioledb_table_description('o_compress_truncate'::regclass);

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA truncate CASCADE;
RESET search_path;
