CREATE DATABASE orioledb_template;
\c orioledb_template

CREATE EXTENSION orioledb;

-- Oriole_table without PK
CREATE TABLE o_tb_no_pk (
    k int NOT NULL,
    val text NOT NULL
) USING orioledb;
CREATE INDEX o_tb_no_pk_idx ON o_tb_no_pk (k);
INSERT INTO o_tb_no_pk VALUES (10, 'A'), (20, 'B');

-- PRIMARY KEY
CREATE TABLE o_tb (
    id int PRIMARY KEY,
    val text NOT NULL
) USING orioledb;
INSERT INTO o_tb VALUES (1, 'one'), (2, 'two');

-- SECONDARY INDEX
CREATE TABLE o_tb_secondary_k (
    id int PRIMARY KEY,
    k int NOT NULL,
    val text
) USING orioledb;
CREATE INDEX o_tb_secondary_k_idx ON o_tb_secondary_k (k);
INSERT INTO o_tb_secondary_k VALUES (10, 100, 'Val1'), (50, 500, 'Val2');

-- BRIDGE INDEX
CREATE TABLE o_tb_bridge (
    id int PRIMARY KEY,
    tag int
) USING orioledb;
CREATE INDEX o_tb_bridge_idx ON o_tb_bridge USING btree (tag)
    WITH (orioledb_index = off, deduplicate_items = off);
INSERT INTO o_tb_bridge VALUES (1, 11), (2, 22);

-- TOAST INDEX
CREATE TABLE o_tb_toast (
    id int PRIMARY KEY,
    t_key text NOT NULL,
    t_big text NOT NULL
) USING orioledb;
CREATE INDEX o_tb_toast_idx ON o_tb_toast (t_key);
INSERT INTO o_tb_toast VALUES
    (1, 'k1', repeat('x', 3000)),
    (2, 'k2', repeat('y', 3500));

-- HEAP TABLE
CREATE TABLE heap_table (
    id int PRIMARY KEY,
    k int NOT NULL
) USING heap;
CREATE INDEX heap_table_idx ON heap_table (k);
INSERT INTO heap_table VALUES (1, 10), (2, 20);

-- USER TABLESPACE
SET allow_in_place_tablespaces = true;
CREATE TABLESPACE db_template_tblspc LOCATION '';

CREATE TABLE o_tb_tblspc (
    id int PRIMARY KEY,
    n int NOT NULL
) USING orioledb TABLESPACE db_template_tblspc;
CREATE INDEX o_tb_tblspc_idx ON o_tb_tblspc (n) TABLESPACE db_template_tblspc;
INSERT INTO o_tb_tblspc VALUES (1, 111), (2, 222);

------------------CHECK IN TEMPLATE -------------------------------------------------
SELECT extname FROM pg_extension WHERE extname = 'orioledb';

-- Oriole_table without PK
SELECT * FROM o_tb_no_pk;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT val FROM o_tb_no_pk WHERE k = 20;
SELECT val FROM o_tb_no_pk WHERE k = 20;
COMMIT;


-- PRIMARY KEY
SELECT id, val FROM o_tb ORDER BY id;

-- SECONDARY INDEX
SELECT * FROM o_tb_secondary_k;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_secondary_k WHERE k = 500;
SELECT id FROM o_tb_secondary_k WHERE k = 500;
COMMIT;

-- BRIDGE INDEX
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_bridge WHERE tag = 11;
SELECT id FROM o_tb_bridge WHERE tag = 11;
COMMIT;

-- TOAST INDEX
SELECT id, t_key, length(t_big) AS t_big_len
FROM o_tb_toast
ORDER BY id;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_toast WHERE t_key = 'k2';
SELECT id FROM o_tb_toast WHERE t_key = 'k2';
COMMIT;

-- HEAP TABLE
SELECT id, k FROM heap_table ORDER BY id;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM heap_table WHERE k = 20;
SELECT id FROM heap_table WHERE k = 20;
COMMIT;

-- USER TABLESPACE
SELECT id, n FROM o_tb_tblspc ORDER BY id;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_tblspc WHERE n = 222;
SELECT id FROM o_tb_tblspc WHERE n = 222;
COMMIT;

---------------------------------------------------------------------------------------

\c postgres
CREATE DATABASE orioledb_from_template TEMPLATE orioledb_template;

\c orioledb_from_template

SELECT extname FROM pg_extension WHERE extname = 'orioledb';

-- Oriole_table without PK
SELECT * FROM o_tb_no_pk;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT val FROM o_tb_no_pk WHERE k = 20;
SELECT val FROM o_tb_no_pk WHERE k = 20;
COMMIT;


-- PRIMARY KEY
SELECT id, val FROM o_tb ORDER BY id;

-- SECONDARY INDEX
SELECT * FROM o_tb_secondary_k;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_secondary_k WHERE k = 500;
SELECT id FROM o_tb_secondary_k WHERE k = 500;
COMMIT;

-- BRIDGE INDEX
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_bridge WHERE tag = 11;
SELECT id FROM o_tb_bridge WHERE tag = 11;
COMMIT;

-- TOAST INDEX
SELECT id, t_key, length(t_big) AS t_big_len
FROM o_tb_toast
ORDER BY id;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_toast WHERE t_key = 'k2';
SELECT id FROM o_tb_toast WHERE t_key = 'k2';
COMMIT;

-- HEAP TABLE
SELECT id, k FROM heap_table ORDER BY id;
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM heap_table WHERE k = 20;
SELECT id FROM heap_table WHERE k = 20;
COMMIT;

-- USER TABLESPACE
SELECT id, n FROM o_tb_tblspc ORDER BY id;

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
    SELECT id FROM o_tb_tblspc WHERE n = 222;
SELECT id FROM o_tb_tblspc WHERE n = 222;
COMMIT;

-- DML AFTER CLONE
INSERT INTO o_tb VALUES (3, 'three');
INSERT INTO o_tb_toast VALUES (3, 'k3', repeat('w', 3200));

SELECT * FROM o_tb;
SELECT id, t_key, length(t_big) AS t_big_len FROM o_tb_toast;



\c postgres
DROP DATABASE orioledb_from_template;

\c orioledb_template
SELECT * FROM o_tb;
SELECT id, t_key FROM o_tb_toast;

DROP TABLE o_tb_toast;
DROP TABLE o_tb_bridge;
DROP TABLE o_tb_secondary_k;
DROP TABLE o_tb;
DROP TABLE o_tb_no_pk;
DROP TABLE o_tb_tblspc;

DROP EXTENSION orioledb CASCADE;

\c postgres
DROP DATABASE orioledb_template;