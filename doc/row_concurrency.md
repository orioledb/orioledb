Row-level concurrency in OrioleDB
=================================

Row-level concurrency in OrioleDB is as close as possible to regular PostgreSQL tables.  However, there are still some differencies because tables in OrioleDB are index-organized.

Update of the primary key
-------------------------

Update of primary key is typically rare and unusual situation in index-organized table.  Unlike regular update, update of primary key is implemented as a sequence of delete and insert.  Concurrent update and delete can't follow this update and may result in error.  See the example below.  Session 2 gets an error due to concurrent primary key update in session 1.

```sql
CREATE TABLE tbl
(
	id int4 primary key,
	value numeric NOT NULL
) USING orioledb;
INSERT INTO tbl VALUES (1, 0.0);

(Session 1)
> BEGIN;
> UPDATE tbl SET id = 2 WHERE id = 1;
UPDATE 1
				(Session 2)
				> BEGIN;
				> UPDATE tbl SET value = value + 1 WHERE id = 1;
				(waiting)
> COMMIT;
COMMIT
				ERROR:  tuple to be locked has its primary key changed due to concurrent update
				> ROLLBACK;
				ROLLBACK
```

Following the update chain
--------------------------

If some row was deleted and then new row with same primary key value is immediately inserted, then concurrent update or delete may consider the new row as a new version of the old row.  See the example below.  Session 1 deletes row and then inserts row with same primary key value.  Session 2 were intended to update initial row, but finally updates the newly inserted row.

```sql
CREATE TABLE tbl
(
	id int4 primary key,
	value numeric NOT NULL
) USING orioledb;
INSERT INTO tbl VALUES (1, 0.0);

(Session 1)
> BEGIN;
> DELETE FROM tbl WHERE id = 1;
DELETE 1
> INSERT INTO tbl VALUES (1, 0.0);
INSERT 0 1
				(Session 2)
				> BEGIN;
				> UPDATE tbl SET value = value + 1 WHERE id = 1;
				(waiting)
> COMMIT;
COMMIT
				UPDATE 1
				> COMMIT;
				COMMIT
```
