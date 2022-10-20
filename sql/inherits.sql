CREATE EXTENSION orioledb;

CREATE TABLE p_stock (
	item_id int
) USING orioledb;

CREATE TABLE c_stock(
	ex int
) INHERITS (p_stock) USING orioledb;

SELECT orioledb_table_description('p_stock'::regclass);
SELECT orioledb_tbl_indices('p_stock'::regclass);
SELECT orioledb_table_description('c_stock'::regclass);
SELECT orioledb_tbl_indices('c_stock'::regclass);

ALTER TABLE p_stock ADD COLUMN balance int;

SELECT orioledb_table_description('p_stock'::regclass);
SELECT orioledb_tbl_indices('p_stock'::regclass);
SELECT orioledb_table_description('c_stock'::regclass);
SELECT orioledb_tbl_indices('c_stock'::regclass);

INSERT INTO p_stock (item_id, balance) VALUES (1, 2);
INSERT INTO p_stock (item_id, balance) VALUES (3, 4);

INSERT INTO c_stock (item_id, ex, balance) VALUES (5, 6, 7);

SELECT * FROM p_stock;
SELECT * FROM c_stock;

EXPLAIN (COSTS OFF) UPDATE p_stock SET balance = balance + 1;
UPDATE p_stock SET balance = balance + 1;

SELECT * FROM p_stock;
SELECT * FROM c_stock;

DELETE FROM p_stock WHERE item_id = 5;

SELECT * FROM p_stock;
SELECT * FROM c_stock;

DROP EXTENSION orioledb CASCADE;