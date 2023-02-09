-- Check for collation support
CREATE SCHEMA collate_schema;
SET SESSION search_path = 'collate_schema';
CREATE EXTENSION orioledb;

CREATE TABLE o_collate (
	t text COLLATE "en-x-icu" NOT NULL,
	PRIMARY KEY(t)
) USING orioledb;
DROP TABLE IF EXISTS o_collate;

CREATE TABLE o_collate (
	t text COLLATE "POSIX" NOT NULL,
	PRIMARY KEY(t)
) USING orioledb;
DROP TABLE IF EXISTS o_collate;

CREATE TABLE o_collate (
	t text COLLATE "C" NOT NULL,
	PRIMARY KEY(t)
) USING orioledb;

CREATE UNIQUE INDEX o_collate_idx_c on o_collate(t COLLATE "C");
CREATE UNIQUE INDEX o_collate_idx_posix on o_collate(t COLLATE "POSIX");
CREATE UNIQUE INDEX o_collate_idx_icu on o_collate(t COLLATE "en-x-icu");

INSERT INTO o_collate VALUES ('x'), ('X'), ('y'), ('Y'), ('z'), ('Z');
ANALYZE o_collate;

SET enable_seqscan = OFF;
SELECT * FROM o_collate;
EXPLAIN (COSTS off) SELECT * FROM o_collate WHERE t >= 'y';
SELECT * FROM o_collate WHERE t >= 'y';
EXPLAIN (COSTS off) SELECT * FROM o_collate WHERE t >= 'y' COLLATE "C";
SELECT * FROM o_collate WHERE t >= 'y' COLLATE "C";
EXPLAIN (COSTS off) SELECT * FROM o_collate WHERE t >= 'y' COLLATE "POSIX";
SELECT * FROM o_collate WHERE t >= 'y' COLLATE "POSIX";
EXPLAIN (COSTS off) SELECT * FROM o_collate WHERE t >= 'y' COLLATE "en-x-icu";
SELECT * FROM o_collate WHERE t >= 'y' COLLATE "en-x-icu";

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA collate_schema CASCADE;
RESET search_path;
