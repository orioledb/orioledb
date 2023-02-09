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

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA partition CASCADE;
RESET search_path;
