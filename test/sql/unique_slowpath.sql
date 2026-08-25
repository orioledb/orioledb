-- The unique check has a fast path that decides the whole key range lives on
-- the page it descended to, and a slow path that walks right until the range
-- ends.  The slow path is the only caller of find_right_page() outside the
-- iterator, so it is the only place a modify context navigates a parent
-- locator, and until now nothing exercised it.
--
-- It is reached without any concurrency: the descent goes to the lower bound of
-- the unique key, so when a value's index entry begins a leaf page the descent
-- lands on the page that *ends* at it and has to step right.  Re-inserting
-- every existing value therefore takes the slow path once per leaf page.  A
-- 400-byte key keeps the entries per page low, so 1000 rows span enough pages
-- for the tree to have an inner level -- without one there is no parent to
-- navigate and the test proves nothing.
CREATE SCHEMA unique_slowpath;
SET SESSION search_path = 'unique_slowpath';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_unique_slowpath (
	id int NOT NULL PRIMARY KEY,
	u text NOT NULL
) USING orioledb;
INSERT INTO o_test_unique_slowpath
	SELECT g, lpad(g::text, 400, '0') FROM generate_series(1, 1000) g;
CREATE UNIQUE INDEX o_test_unique_slowpath_u ON o_test_unique_slowpath (u);

DO $$
DECLARE
	i int;
	conflicts int := 0;
BEGIN
	FOR i IN 1..1000 LOOP
		BEGIN
			INSERT INTO o_test_unique_slowpath
				VALUES (1000000 + i, lpad(i::text, 400, '0'));
		EXCEPTION WHEN unique_violation THEN
			conflicts := conflicts + 1;
		END;
	END LOOP;
	RAISE NOTICE 'conflicts: %', conflicts;
END $$;

-- Every duplicate must have been rejected, and the values reachable through the
-- unique index must still match the table.
SELECT count(*) FROM o_test_unique_slowpath;
SET enable_seqscan = OFF;
SELECT count(*), count(DISTINCT u) FROM o_test_unique_slowpath
	WHERE u > lpad('', 400, '0');
RESET enable_seqscan;

-- A value the table does not hold still has to pass the check and insert.
INSERT INTO o_test_unique_slowpath VALUES (2000001, lpad('2000001', 400, '0'));
SELECT count(*) FROM o_test_unique_slowpath;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA unique_slowpath CASCADE;
RESET search_path;
