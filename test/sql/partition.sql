CREATE SCHEMA partition;
SET SESSION search_path = 'partition';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_partition_on_conflict_range (
  val_1 int,
  val_2 int
) PARTITION BY RANGE(val_1);

CREATE TABLE o_test_partition_on_conflict_range_child (
  val_2 int,
  val_1 int NOT NULL
) USING orioledb;

CREATE UNIQUE INDEX ON o_test_partition_on_conflict_range (val_1);

ALTER TABLE o_test_partition_on_conflict_range
	ATTACH PARTITION o_test_partition_on_conflict_range_child
		FOR VALUES FROM (0) TO (20);

EXPLAIN (COSTS OFF)
	INSERT INTO o_test_partition_on_conflict_range
		VALUES (1, 2) ON CONFLICT (val_1)
			DO UPDATE SET val_1 = excluded.val_1 * 10;

INSERT INTO o_test_partition_on_conflict_range VALUES (0, 1);
INSERT INTO o_test_partition_on_conflict_range
	VALUES (1, 2) ON CONFLICT (val_1)
		DO UPDATE SET val_1 = excluded.val_1 * 10;

EXPLAIN (COSTS OFF) SELECT * FROM o_test_partition_on_conflict_range;
SELECT * FROM o_test_partition_on_conflict_range;
SELECT * FROM o_test_partition_on_conflict_range_child;

INSERT INTO o_test_partition_on_conflict_range
	VALUES (1, 2) ON CONFLICT (val_1)
		DO UPDATE SET val_1 = excluded.val_1 * 100;
INSERT INTO o_test_partition_on_conflict_range
	VALUES (1, 2) ON CONFLICT (val_1)
		DO UPDATE SET val_1 = excluded.val_1 * 10;
SELECT * FROM o_test_partition_on_conflict_range;
SELECT * FROM o_test_partition_on_conflict_range_child;

DELETE FROM o_test_partition_on_conflict_range WHERE val_1 = 10;
SELECT * FROM o_test_partition_on_conflict_range;
SELECT * FROM o_test_partition_on_conflict_range_child;

CREATE TABLE o_test_partition_on_conflict_list (
	a int UNIQUE,
	b char(2)
) PARTITION BY LIST (a);

CREATE TABLE o_test_partition_on_conflict_list_child (
	a int UNIQUE,
	b char(2)
) USING orioledb;

ALTER TABLE o_test_partition_on_conflict_list
	ATTACH PARTITION o_test_partition_on_conflict_list_child
		FOR VALUES IN (3);

INSERT INTO o_test_partition_on_conflict_list VALUES (3, 'a') ON CONFLICT (a)
	DO UPDATE SET b = excluded.b;
SELECT * FROM o_test_partition_on_conflict_list;
INSERT INTO o_test_partition_on_conflict_list VALUES (3, 'a') ON CONFLICT (a)
	DO UPDATE SET b = excluded.b || 'c';
SELECT * FROM o_test_partition_on_conflict_list;

DELETE FROM o_test_partition_on_conflict_list WHERE a = 3;
SELECT * FROM o_test_partition_on_conflict_list;

CREATE TABLE o_test_partition_on_conflict_update_same_trx (
	a int UNIQUE,
	b char(2)
) PARTITION BY LIST (a);

CREATE TABLE o_test_partition_on_conflict_update_same_trx_child (
	a int UNIQUE,
	b char(2)
) USING orioledb;

ALTER TABLE o_test_partition_on_conflict_update_same_trx
	ATTACH PARTITION o_test_partition_on_conflict_update_same_trx_child
		FOR VALUES IN (3);

BEGIN;
INSERT INTO o_test_partition_on_conflict_update_same_trx
	VALUES (3, 'a') ON CONFLICT (a) DO UPDATE SET b = excluded.b;
TABLE o_test_partition_on_conflict_update_same_trx;

INSERT INTO o_test_partition_on_conflict_update_same_trx
	VALUES (3, 'a') ON CONFLICT (a) DO UPDATE SET b = excluded.b || 'c';
TABLE o_test_partition_on_conflict_update_same_trx;
COMMIT;

CREATE TABLE o_test_cross_partition_update (
  val_1 int,
  val_2 int
) PARTITION BY RANGE(val_1);

CREATE TABLE o_test_cross_partition_update_child (
  val_2 int,
  val_1 int NOT NULL
) USING orioledb;

CREATE TABLE o_test_cross_partition_update_child2 (
  val_2 int,
  val_1 int NOT NULL
) USING orioledb;

CREATE UNIQUE INDEX ON o_test_cross_partition_update (val_1);

ALTER TABLE o_test_cross_partition_update
	ATTACH PARTITION o_test_cross_partition_update_child
		FOR VALUES FROM (0) TO (10);
ALTER TABLE o_test_cross_partition_update
	ATTACH PARTITION o_test_cross_partition_update_child2
		FOR VALUES FROM (10) TO (20);

INSERT INTO o_test_cross_partition_update
	SELECT v, v * 10 FROM generate_series(0, 19) v;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_cross_partition_update;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_cross_partition_update_child;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_cross_partition_update_child2;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_cross_partition_update
	WHERE val_1 BETWEEN 0 AND 5;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_cross_partition_update
	WHERE val_1 BETWEEN 10 AND 15;
SELECT * FROM o_test_cross_partition_update;
SELECT * FROM o_test_cross_partition_update_child;
SELECT * FROM o_test_cross_partition_update_child2;
SELECT * FROM o_test_cross_partition_update WHERE val_1 BETWEEN 0 AND 5;
SELECT * FROM o_test_cross_partition_update WHERE val_1 BETWEEN 10 AND 15;

DELETE FROM o_test_cross_partition_update WHERE val_1 BETWEEN 10 AND 15;
SELECT * FROM o_test_cross_partition_update;
SELECT * FROM o_test_cross_partition_update_child;
SELECT * FROM o_test_cross_partition_update_child2;

UPDATE o_test_cross_partition_update
	SET val_1 = val_1 + 10 WHERE val_1 BETWEEN 0 AND 5;
SELECT * FROM o_test_cross_partition_update;
SELECT * FROM o_test_cross_partition_update_child;
SELECT * FROM o_test_cross_partition_update_child2;

CREATE TABLE o_test_1 (
	val_1 int,
	val_2 int,
	PRIMARY KEY (val_1, val_2)
) USING orioledb;

CREATE TABLE o_test_2 (
	val_1 int,
	val_2 int
) PARTITION BY RANGE (val_1, val_2);

CREATE TABLE o_test_3 (
	val_1 int,
	val_2 int
) USING orioledb;

ALTER TABLE o_test_2
	ATTACH PARTITION o_test_3 FOR
		VALUES FROM (0, 0) TO (10, 10);

ALTER TABLE o_test_2
	ADD FOREIGN KEY (val_1, val_2)
		REFERENCES o_test_1;

INSERT INTO o_test_1
	VALUES (5, 6);

INSERT INTO o_test_2
	VALUES (5, 6);

UPDATE o_test_1 SET val_2 = 7;

CREATE TABLE o_test_partition_index (
	val_1 int,
	val_2 int
) PARTITION BY RANGE (val_1);

CREATE TABLE o_test_partition_index_child1 (
	val_1 int NOT NULL,
	val_2 int
) USING orioledb;

CREATE TABLE o_test_partition_index_child2 (
	val_1 int NOT NULL,
	val_2 int
) USING orioledb;

CREATE UNIQUE INDEX o_test_partition_index_child1_ix1
	ON o_test_partition_index_child1 (val_1);

ALTER TABLE o_test_partition_index ADD PRIMARY KEY (val_1);
ALTER TABLE o_test_partition_index
	ATTACH PARTITION o_test_partition_index_child1 FOR VALUES FROM (1) TO (6);
ALTER TABLE o_test_partition_index
	ATTACH PARTITION o_test_partition_index_child2 FOR VALUES FROM (6) TO (11);

INSERT INTO o_test_partition_index SELECT v, v FROM generate_series(1, 10) v;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_partition_index ORDER BY val_1;
SELECT * FROM o_test_partition_index ORDER BY val_1;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_partition_index
		WHERE val_1 BETWEEN 2 AND 3 ORDER BY val_1;
SELECT * FROM o_test_partition_index
	WHERE val_1 BETWEEN 2 AND 3 ORDER BY val_1;
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_partition_index
		WHERE val_1 BETWEEN 7 AND 9 ORDER BY val_1;
SELECT * FROM o_test_partition_index WHERE val_1 BETWEEN 7 AND 9 ORDER BY val_1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_partition_index_child1 ORDER BY val_1;
SELECT * FROM o_test_partition_index_child1 ORDER BY val_1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_partition_index_child2 ORDER BY val_1;
SELECT * FROM o_test_partition_index_child2 ORDER BY val_1;

BEGIN;
CREATE TABLE o_test_partition_pkey_update (
	val_1 int PRIMARY KEY
) PARTITION BY RANGE (val_1);

CREATE TABLE o_test_partition_pkey_update_child (
	LIKE o_test_partition_pkey_update
) USING orioledb;

ALTER TABLE o_test_partition_pkey_update
	ATTACH PARTITION o_test_partition_pkey_update_child
		FOR VALUES FROM (1) TO (5);

SELECT * FROM o_test_partition_pkey_update;
SELECT * FROM o_test_partition_pkey_update_child;
EXPLAIN (COSTS OFF)
	UPDATE o_test_partition_pkey_update SET val_1 = 3 WHERE val_1 = 1;
UPDATE o_test_partition_pkey_update SET val_1 = 3 WHERE val_1 = 1;
SELECT * FROM o_test_partition_pkey_update;
SELECT * FROM o_test_partition_pkey_update_child;
COMMIT;

CREATE TABLE o_test_partition_multiple_moves (
  val_1 int,
  val_2 int,
  val_3 int
) PARTITION BY LIST (val_1);

CREATE TABLE o_test_partition_multiple_moves_child1 (
  val_1 int,
  val_2 int,
  val_3 int
) USING orioledb;

ALTER TABLE o_test_partition_multiple_moves
	ATTACH PARTITION o_test_partition_multiple_moves_child1 FOR VALUES IN (1);

CREATE TABLE o_test_partition_multiple_moves_child2 (
  val_1 int,
  val_2 int,
  val_3 int
) USING orioledb;

ALTER TABLE o_test_partition_multiple_moves
	ATTACH PARTITION o_test_partition_multiple_moves_child2 FOR VALUES IN (2);

INSERT INTO o_test_partition_multiple_moves VALUES (1, 1, 60);

CREATE TABLE o_test_partition_multiple_moves_from (
  val int
) USING orioledb;

INSERT INTO o_test_partition_multiple_moves_from VALUES (1), (1);

SELECT tableoid::regclass, * FROM o_test_partition_multiple_moves;

UPDATE o_test_partition_multiple_moves t1 SET val_1 = 2
	FROM o_test_partition_multiple_moves_from t4
	WHERE t1.val_1 = t4.val AND val_1 = 1;

SELECT tableoid::regclass, * FROM o_test_partition_multiple_moves;

-- Should succeed in PG17 but fail in PG16
CREATE TABLE o_test_partition_pkey_update_move (
  val_1 INT PRIMARY KEY
) PARTITION BY LIST (val_1) USING orioledb;

-- Create it using default AM for PG16
CREATE TABLE IF NOT EXISTS o_test_partition_pkey_update_move (
  val_1 INT PRIMARY KEY
) PARTITION BY LIST (val_1);

CREATE TABLE o_test_partition_pkey_update_move_child1
	PARTITION OF o_test_partition_pkey_update_move FOR VALUES IN (1);

CREATE TABLE o_test_partition_pkey_update_move_child2 (
  val_1 int NOT NULL
) USING orioledb;

CREATE TABLE o_test_partition_pkey_update_move_child3
	PARTITION OF o_test_partition_pkey_update_move FOR VALUES IN (3, 4);

BEGIN;
ALTER TABLE o_test_partition_pkey_update_move
	ATTACH PARTITION o_test_partition_pkey_update_move_child2
		FOR VALUES IN (2);

INSERT INTO o_test_partition_pkey_update_move VALUES (1), (3);

SELECT tableoid::regclass, * FROM o_test_partition_pkey_update_move;
UPDATE o_test_partition_pkey_update_move SET val_1 = val_1 + 1;
SELECT tableoid::regclass, * FROM o_test_partition_pkey_update_move;
COMMIT;

CREATE TABLE o_test_cross_partition_update_trigger (
  a INT PRIMARY KEY
) PARTITION BY LIST (a);

CREATE TABLE o_test_cross_partition_update_trigger_fk (
	a INT,
	CONSTRAINT fkey FOREIGN KEY (a)
		REFERENCES o_test_cross_partition_update_trigger(a) ON UPDATE CASCADE
) USING orioledb;

CREATE TABLE o_test_cross_partition_update_trigger_child1 (
  a int NOT NULL
) USING orioledb;

CREATE TABLE o_test_cross_partition_update_trigger_child2 (
  a int NOT NULL
) USING orioledb;

ALTER TABLE o_test_cross_partition_update_trigger
	ATTACH PARTITION o_test_cross_partition_update_trigger_child1
		FOR VALUES IN (1);
ALTER TABLE o_test_cross_partition_update_trigger
	ATTACH PARTITION o_test_cross_partition_update_trigger_child2
		FOR VALUES IN (2);
INSERT INTO o_test_cross_partition_update_trigger VALUES (1);
INSERT INTO o_test_cross_partition_update_trigger_fk VALUES (1);

SELECT tableoid::regclass, * FROM o_test_cross_partition_update_trigger;
TABLE o_test_cross_partition_update_trigger_fk;
UPDATE o_test_cross_partition_update_trigger SET a = a + 1 RETURNING *;
SELECT tableoid::regclass, * FROM o_test_cross_partition_update_trigger;
TABLE o_test_cross_partition_update_trigger_fk;

BEGIN;

CREATE TABLE o_test_partition_truncate (
  a int UNIQUE,
  b char
) PARTITION BY LIST (a);

CREATE TABLE o_test_partition_truncate2 (
  b char,
  a int unique
) USING orioledb;

ALTER TABLE o_test_partition_truncate
	ATTACH PARTITION o_test_partition_truncate2 FOR VALUES IN (3);

INSERT INTO o_test_partition_truncate VALUES (3, 'b');

TRUNCATE o_test_partition_truncate;

SELECT * FROM o_test_partition_truncate;

COMMIT;


DROP EXTENSION orioledb CASCADE;
DROP SCHEMA partition CASCADE;
RESET search_path;
